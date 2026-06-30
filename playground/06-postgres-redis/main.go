package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	redisclient "github.com/redis/go-redis/v9"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	redisconnector "github.com/lemenendez/deltaflow/pkg/connectors/redis"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

const (
	syncID         = deltaflow.SyncID("playground-06-contacts-to-redis")
	contactType    = deltaflow.ProjectionType("ContactCache")
	contactID      = "con-001"
	ghostContactID = "con-ghost"
)

func main() {
	ctx := context.Background()
	db := openPostgres(ctx)
	defer db.Close()
	redisClient := openRedis(ctx)
	defer redisClient.Close()

	ttl := envDuration("DELTAFLOW_REDIS_TTL", 5*time.Minute)
	deltaStore := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := pgstore.NewJobStore(db, pgstore.JobStoreConfig{MaxAttempts: 3})
	dispatchStore := pgstore.NewDispatchStore(deltaStore, jobStore, pgstore.DispatchStoreConfig{})

	keyFunc := func(identity deltaflow.ProjectionIdentity) (string, error) {
		id, err := stringKey(identity.Key, "contact_id")
		if err != nil {
			return "", err
		}
		return "contacts:" + id, nil
	}
	redisApplier, err := redisconnector.NewApplier(redisconnector.ApplierConfig{
		Client:  redisClient,
		KeyFunc: keyFunc,
		TTL:     ttl,
	})
	if err != nil {
		log.Fatal(err)
	}
	applier := &failOnceApplier{next: redisApplier, contactID: contactID}
	projector := &contactProjector{db: db}
	worker := &deltaflow.SyncWorker{
		JobStore:   jobStore,
		Dispatcher: dispatchStore,
		Projector:  projector,
		Applier:    applier,
		SyncID:     syncID,
		WorkerID:   "playground-06-worker",
		LockFor:    5 * time.Second,
		PullSize:   8,
		BatchSize:  1,
	}

	if err := reset(ctx, db, redisClient); err != nil {
		log.Fatalf("reset: %v", err)
	}
	if err := writeContactAndEnqueue(ctx, db, deltaStore); err != nil {
		log.Fatalf("transactional contact write: %v", err)
	}
	if err := deleteGhostAndEnqueue(ctx, db, deltaStore, redisClient); err != nil {
		log.Fatalf("transactional ghost delete: %v", err)
	}
	if err := drain(ctx, db, worker); err != nil {
		log.Fatalf("worker drain: %v", err)
	}

	contactKey := "contacts:" + contactID
	payload, err := redisClient.Get(ctx, contactKey).Bytes()
	if err != nil {
		log.Fatalf("read projected contact: %v", err)
	}
	remaining, err := redisClient.TTL(ctx, contactKey).Result()
	if err != nil {
		log.Fatalf("read projected contact TTL: %v", err)
	}
	ghostErr := redisClient.Get(ctx, "contacts:"+ghostContactID).Err()
	if ghostErr != redisclient.Nil {
		log.Fatalf("ghost cache key still exists: %v", ghostErr)
	}

	counts, err := jobCounts(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("DeltaFlow playground 06-postgres-redis")
	fmt.Println("- Postgres source write and Delta enqueue committed in one transaction.")
	fmt.Println("- Redis SET failed once, then the worker re-projected latest state and retried.")
	fmt.Printf("- cache key=%s payload=%s ttl_remaining=%s\n", contactKey, payload, remaining)
	fmt.Printf("- ghost key contacts:%s deleted=true\n", ghostContactID)
	fmt.Printf("- jobs synced=%d dead=%d retry_attempts=%d\n", counts.synced, counts.dead, counts.attempts)
}

type contactProjector struct {
	db *sql.DB
}

func (p *contactProjector) Project(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
	id, err := stringKey(identity.Key, "contact_id")
	if err != nil {
		return deltaflow.Projection{}, err
	}
	var name, email string
	err = p.db.QueryRowContext(ctx, `SELECT name, email FROM public.playground_06_contacts WHERE id = $1`, id).Scan(&name, &email)
	if errors.Is(err, sql.ErrNoRows) {
		return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
	}
	if err != nil {
		return deltaflow.Projection{}, err
	}
	payload, err := json.Marshal(map[string]string{"id": id, "name": name, "email": email})
	if err != nil {
		return deltaflow.Projection{}, err
	}
	return deltaflow.Projection{Identity: identity, Payload: payload, MediaType: "application/json"}, nil
}

type failOnceApplier struct {
	next      deltaflow.ProjectionApplier
	contactID string
	failed    bool
}

func (a *failOnceApplier) Apply(ctx context.Context, op deltaflow.ProjectionOperation) error {
	id, err := stringKey(op.Identity.Key, "contact_id")
	if err != nil {
		return err
	}
	if op.Type == deltaflow.ProjectionOpUpsert && id == a.contactID && !a.failed {
		a.failed = true
		return errors.New("simulated temporary Redis timeout")
	}
	return a.next.Apply(ctx, op)
}

func openPostgres(ctx context.Context) *sql.DB {
	dsn := os.Getenv("DELTAFLOW_PG_DSN")
	if dsn == "" {
		dsn = "postgres://deltaflow:deltaflow@postgres:5432/deltaflow?sslmode=disable"
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		log.Fatal(err)
	}
	return db
}

func openRedis(ctx context.Context) *redisclient.Client {
	addr := os.Getenv("DELTAFLOW_REDIS_ADDR")
	if addr == "" {
		addr = "redis:6379"
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		log.Fatal(err)
	}
	return client
}

func reset(ctx context.Context, db *sql.DB, client *redisclient.Client) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS public.playground_06_contacts (id text PRIMARY KEY, name text NOT NULL, email text NOT NULL)`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = $1`, syncID); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM deltaflow.deltaflow_deltas WHERE sync_id = $1`, syncID); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM public.playground_06_contacts`); err != nil {
		return err
	}
	return client.Del(ctx, "contacts:"+contactID, "contacts:"+ghostContactID).Err()
}

func writeContactAndEnqueue(ctx context.Context, db *sql.DB, store *pgstore.DeltaStore) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO public.playground_06_contacts (id, name, email) VALUES ($1, $2, $3)`, contactID, "Ada Lovelace", "ada@example.test"); err != nil {
		return err
	}
	if _, err := store.EnqueueInTx(ctx, tx, newDelta(contactID, deltaflow.OriginOperationInserted)); err != nil {
		return err
	}
	return tx.Commit()
}

func deleteGhostAndEnqueue(ctx context.Context, db *sql.DB, store *pgstore.DeltaStore, client *redisclient.Client) error {
	if _, err := db.ExecContext(ctx, `INSERT INTO public.playground_06_contacts (id, name, email) VALUES ($1, $2, $3)`, ghostContactID, "Stale Contact", "stale@example.test"); err != nil {
		return err
	}
	if err := client.Set(ctx, "contacts:"+ghostContactID, `{"stale":true}`, 0).Err(); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM public.playground_06_contacts WHERE id = $1`, ghostContactID); err != nil {
		return err
	}
	if _, err := store.EnqueueInTx(ctx, tx, newDelta(ghostContactID, deltaflow.OriginOperationDeleted)); err != nil {
		return err
	}
	return tx.Commit()
}

func newDelta(id string, origin deltaflow.OriginOperationType) deltaflow.Delta {
	raw, _ := json.Marshal(id)
	return deltaflow.Delta{
		SyncID:         syncID,
		Origin:         origin,
		ProjectionType: contactType,
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": raw},
		OccurredAt:     time.Now().UTC(),
	}
}

func drain(ctx context.Context, db *sql.DB, worker *deltaflow.SyncWorker) error {
	for i := 0; i < 20; i++ {
		if err := worker.RunOnce(ctx); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `UPDATE deltaflow.deltaflow_sync_jobs SET available_at = NOW() WHERE sync_id = $1 AND state = 'retrying'`, syncID); err != nil {
			return err
		}
		counts, err := jobCounts(ctx, db)
		if err != nil {
			return err
		}
		if counts.synced+counts.dead == 2 && counts.pending == 0 && counts.processing == 0 && counts.retrying == 0 {
			return nil
		}
	}
	return errors.New("worker did not drain all jobs")
}

type counts struct {
	pending, processing, retrying, synced, dead, attempts int
}

func jobCounts(ctx context.Context, db *sql.DB) (counts, error) {
	var c counts
	err := db.QueryRowContext(ctx, `
SELECT
    COUNT(*) FILTER (WHERE state = 'pending'),
    COUNT(*) FILTER (WHERE state = 'processing'),
    COUNT(*) FILTER (WHERE state = 'retrying'),
    COUNT(*) FILTER (WHERE state = 'synced'),
    COUNT(*) FILTER (WHERE state = 'dead'),
    COALESCE(SUM(attempt_count), 0)
FROM deltaflow.deltaflow_sync_jobs
WHERE sync_id = $1`, syncID).Scan(&c.pending, &c.processing, &c.retrying, &c.synced, &c.dead, &c.attempts)
	return c, err
}

func stringKey(key deltaflow.ProjectionKey, field string) (string, error) {
	raw, ok := key[field]
	if !ok {
		return "", fmt.Errorf("projection key %q is required", field)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("projection key %q: %w", field, err)
	}
	if value == "" {
		return "", fmt.Errorf("projection key %q cannot be empty", field)
	}
	return value, nil
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		log.Fatalf("%s must be a non-negative duration: %q", name, value)
	}
	return duration
}
