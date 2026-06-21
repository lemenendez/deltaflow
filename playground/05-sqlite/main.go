package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	sqlitestore "github.com/lemenendez/deltaflow/pkg/connectors/sqlite"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	_ "modernc.org/sqlite"
)

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DELTAFLOW_SQLITE_DSN")
	if dsn == "" {
		dsn = "file:deltaflow-playground.sqlite"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping sqlite: %v", err)
	}
	if _, err := sqlitestore.ApplyMigrations(ctx, db); err != nil {
		log.Fatalf("apply sqlite migrations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		log.Fatalf("set WAL mode: %v", err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout=5000`); err != nil {
		log.Fatalf("set busy timeout: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS contacts (id TEXT PRIMARY KEY, full_name TEXT NOT NULL)`); err != nil {
		log.Fatalf("create contacts source table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM contacts`); err != nil {
		log.Fatalf("reset contacts: %v", err)
	}

	deltaStore := sqlitestore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := sqlitestore.NewJobStore(db, sqlitestore.JobStoreConfig{MaxAttempts: 3})
	dispatchStore := sqlitestore.NewDispatchStore(deltaStore, jobStore, sqlitestore.DispatchStoreConfig{})

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("begin tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO contacts (id, full_name) VALUES (?, ?)`, "c-1", "Alice SQLite"); err != nil {
		_ = tx.Rollback()
		log.Fatalf("insert contact: %v", err)
	}
	if _, err := deltaStore.EnqueueInTx(ctx, tx, deltaflow.Delta{
		SyncID:         "contacts-sqlite-demo",
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: "contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"c-1"`),
		},
	}); err != nil {
		_ = tx.Rollback()
		log.Fatalf("enqueue in tx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatalf("commit tx: %v", err)
	}

	applied := make([]string, 0, 1)
	worker := &deltaflow.SyncWorker{
		JobStore:   jobStore,
		Dispatcher: dispatchStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, id deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			var contactID string
			if err := json.Unmarshal(id.Key["contact_id"], &contactID); err != nil {
				return deltaflow.Projection{}, err
			}
			var fullName string
			if err := db.QueryRowContext(ctx, `SELECT full_name FROM contacts WHERE id = ?`, contactID).Scan(&fullName); err != nil {
				return deltaflow.Projection{}, err
			}
			payload, err := json.Marshal(map[string]string{"id": contactID, "full_name": fullName})
			if err != nil {
				return deltaflow.Projection{}, err
			}
			return deltaflow.Projection{Identity: id, Payload: payload, MediaType: "application/json"}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(_ context.Context, op deltaflow.ProjectionOperation) error {
			applied = append(applied, string(op.Projection.Payload))
			return nil
		}),
		SyncID:      "contacts-sqlite-demo",
		WorkerID:    "playground-05-sqlite",
		LockFor:     5 * time.Second,
		BatchSize:   1,
		Concurrency: 1,
	}

	if err := worker.RunOnce(ctx); err != nil {
		log.Fatalf("worker run once: %v", err)
	}

	var synced int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deltaflow_sync_jobs WHERE state = 'synced'`).Scan(&synced); err != nil {
		log.Fatalf("count synced jobs: %v", err)
	}

	fmt.Println("DeltaFlow playground 05-sqlite")
	fmt.Printf("applied=%d synced_jobs=%d\n", len(applied), synced)
	if len(applied) > 0 {
		fmt.Printf("payload=%s\n", applied[0])
	}
}
