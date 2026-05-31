//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lemenendez/deltaflow/pkg/connectors"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const testSyncID = deltaflow.SyncID("test-sync-integration")

var testKey = deltaflow.ProjectionKey{
	"contact_id": json.RawMessage(`"1"`),
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DELTAFLOW_PG_DSN")
	if dsn == "" {
		t.Skip("DELTAFLOW_PG_DSN not set; skipping integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		t.Fatalf("db.Ping: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// truncateAll resets both tables before each test so tests are independent.
// Jobs reference deltas via FK ON DELETE SET NULL, so truncate jobs first.
func truncateAll(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		"TRUNCATE deltaflow.deltaflow_sync_jobs, deltaflow.deltaflow_deltas CASCADE")
	if err != nil {
		t.Fatalf("truncateAll: %v", err)
	}
}

func ptrStr(s string) *string         { return &s }
func ptrTime(ts time.Time) *time.Time { return &ts }

// ---------------------------------------------------------------------------
// DeltaStore
// ---------------------------------------------------------------------------

func TestPGDeltaStoreEnqueueReturnsPending(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})

	d, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if d.ID == "" {
		t.Fatal("ID is empty")
	}
	if d.State != deltaflow.DeltaPending {
		t.Fatalf("state = %s, want pending", d.State)
	}
	if d.CreatedAt.IsZero() {
		t.Fatal("created_at is zero")
	}
	if d.OccurredAt.IsZero() {
		t.Fatal("occurred_at is zero")
	}
	if d.SyncID != testSyncID {
		t.Fatalf("sync_id = %s, want %s", d.SyncID, testSyncID)
	}
}

func TestPGDeltaStoreEnqueueComputesProjectionKeyHash(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})

	d, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if d.ProjectionKeyHash == "" {
		t.Fatal("projection_key_hash is empty")
	}
}

func TestPGDeltaStoreGetRoundTrips(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})

	inserted, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, ok, err := store.Get(ctx, inserted.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("Get: not found")
	}
	if got.ID != inserted.ID {
		t.Fatalf("ID = %s, want %s", got.ID, inserted.ID)
	}
	if got.SyncID != testSyncID {
		t.Fatalf("sync_id = %s, want %s", got.SyncID, testSyncID)
	}
	if got.ProjectionKeyHash != inserted.ProjectionKeyHash {
		t.Fatalf("projection_key_hash mismatch: %s vs %s", got.ProjectionKeyHash, inserted.ProjectionKeyHash)
	}
}

func TestPGDeltaStoreGetMissingReturnsFalse(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})

	_, ok, err := store.Get(ctx, "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("Get missing: %v", err)
	}
	if ok {
		t.Fatal("Get missing: expected false, got true")
	}
}

func TestPGDeltaStorePullFiltersToSyncID(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})

	own, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Enqueue own: %v", err)
	}
	// Foreign delta — must not appear in Pull for testSyncID.
	if _, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "other-sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"99"`),
		},
	}); err != nil {
		t.Fatalf("Enqueue foreign: %v", err)
	}

	pulled, err := store.Pull(ctx, testSyncID, 10)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(pulled) != 1 {
		t.Fatalf("len(pulled) = %d, want 1", len(pulled))
	}
	if pulled[0].ID != own.ID {
		t.Fatalf("pulled ID = %s, want %s", pulled[0].ID, own.ID)
	}
}

func TestPGDeltaStoreMarkDispatched(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})

	inserted, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := store.MarkDispatched(ctx, inserted.ID); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}

	got, ok, err := store.Get(ctx, inserted.ID)
	if err != nil || !ok {
		t.Fatalf("Get after dispatch: (ok=%v, %v)", ok, err)
	}
	if got.State != deltaflow.DeltaDispatched {
		t.Fatalf("state = %s, want dispatched", got.State)
	}
	if got.DispatchedAt == nil {
		t.Fatal("dispatched_at is nil")
	}
}

func TestPGDeltaStoreMarkDispatchedIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})

	inserted, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := store.MarkDispatched(ctx, inserted.ID); err != nil {
		t.Fatalf("first MarkDispatched: %v", err)
	}
	first, _, _ := store.Get(ctx, inserted.ID)
	firstAt := first.DispatchedAt

	if err := store.MarkDispatched(ctx, inserted.ID); err != nil {
		t.Fatalf("second MarkDispatched: %v", err)
	}
	second, _, _ := store.Get(ctx, inserted.ID)

	if second.DispatchedAt == nil || !second.DispatchedAt.Equal(*firstAt) {
		t.Fatalf("dispatched_at changed on idempotent call: %v → %v", firstAt, second.DispatchedAt)
	}
}

func TestPGDeltaStoreMarkDispatchedMissingReturnsError(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})

	err := store.MarkDispatched(ctx, "00000000-0000-0000-0000-000000000000")
	if !errors.Is(err, deltaflow.ErrDeltaNotFound) {
		t.Fatalf("error = %v, want ErrDeltaNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// JobStore
// ---------------------------------------------------------------------------

func newManualJob(syncID deltaflow.SyncID) deltaflow.SyncJob {
	return deltaflow.SyncJob{
		SyncID:         syncID,
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	}
}

func TestPGJobStoreCreateReturnsPending(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	job, err := store.Create(ctx, newManualJob(testSyncID))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if job.ID == "" {
		t.Fatal("ID is empty")
	}
	if job.State != deltaflow.StatePending {
		t.Fatalf("state = %s, want pending", job.State)
	}
	if job.CreatedAt.IsZero() {
		t.Fatal("created_at is zero")
	}
	if job.SyncID != testSyncID {
		t.Fatalf("sync_id = %s, want %s", job.SyncID, testSyncID)
	}
}

func TestPGJobStoreCreateRejectsOutboxWithoutDelta(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	_, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         testSyncID,
		Origin:         deltaflow.JobOriginOutbox,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if !errors.Is(err, deltaflow.ErrOutboxJobNeedsDelta) {
		t.Fatalf("error = %v, want ErrOutboxJobNeedsDelta", err)
	}
}

func TestPGJobStoreGetRoundTrips(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	created, err := store.Create(ctx, newManualJob(testSyncID))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, ok, err := store.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("Get: not found")
	}
	if got.ID != created.ID {
		t.Fatalf("ID = %s, want %s", got.ID, created.ID)
	}
	if got.MaxAttempts <= 0 {
		t.Fatalf("max_attempts = %d, want > 0", got.MaxAttempts)
	}
}

func TestPGJobStoreGetMissingReturnsFalse(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	_, ok, err := store.Get(ctx, "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("Get missing: %v", err)
	}
	if ok {
		t.Fatal("Get missing: expected false, got true")
	}
}

func TestPGJobStoreClaimNextPicksReadyJob(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	created, err := store.Create(ctx, newManualJob(testSyncID))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	claimed, err := store.ClaimNext(ctx, testSyncID, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimNext: returned nil")
	}
	if claimed.ID != created.ID {
		t.Fatalf("claimed ID = %s, want %s", claimed.ID, created.ID)
	}
	if claimed.State != deltaflow.StateProcessing {
		t.Fatalf("state = %s, want processing", claimed.State)
	}
	if claimed.LockedBy == nil || *claimed.LockedBy != "worker-1" {
		t.Fatalf("locked_by = %v, want worker-1", claimed.LockedBy)
	}
	if claimed.LockedUntil == nil || claimed.LockedUntil.Before(time.Now()) {
		t.Fatalf("locked_until = %v, want future timestamp", claimed.LockedUntil)
	}
}

func TestPGJobStoreClaimNextIgnoresForeignSync(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	if _, err := store.Create(ctx, newManualJob("other-sync")); err != nil {
		t.Fatalf("Create foreign: %v", err)
	}

	claimed, err := store.ClaimNext(ctx, testSyncID, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if claimed != nil {
		t.Fatalf("ClaimNext returned job %s for wrong syncID", claimed.ID)
	}
}

func TestPGJobStoreClaimNextIgnoresFutureAvailableAt(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	_, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         testSyncID,
		Origin:         deltaflow.JobOriginManual,
		State:          deltaflow.StateRetrying,
		AvailableAt:    time.Now().UTC().Add(time.Hour),
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	claimed, err := store.ClaimNext(ctx, testSyncID, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if claimed != nil {
		t.Fatal("ClaimNext claimed a not-yet-available job")
	}
}

func TestPGJobStoreClaimNextSkipsActiveLease(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	if _, err := store.Create(ctx, newManualJob(testSyncID)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Worker-1 holds an active lease for an hour.
	if _, err := store.ClaimNext(ctx, testSyncID, "worker-1", time.Hour); err != nil {
		t.Fatalf("first ClaimNext: %v", err)
	}

	// Worker-2 must not steal the leased job.
	claimed, err := store.ClaimNext(ctx, testSyncID, "worker-2", time.Minute)
	if err != nil {
		t.Fatalf("second ClaimNext: %v", err)
	}
	if claimed != nil {
		t.Fatalf("ClaimNext returned job %s while active lease held", claimed.ID)
	}
}

func TestPGJobStoreClaimNextReclaimsExpiredLease(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	// Insert a job already in processing state with an expired lock.
	expired := time.Now().UTC().Add(-time.Minute)
	created, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         testSyncID,
		Origin:         deltaflow.JobOriginManual,
		State:          deltaflow.StateProcessing,
		LockedBy:       ptrStr("old-worker"),
		LockedUntil:    ptrTime(expired),
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Create processing job: %v", err)
	}

	claimed, err := store.ClaimNext(ctx, testSyncID, "worker-new", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimNext: returned nil for expired-lease job")
	}
	if claimed.ID != created.ID {
		t.Fatalf("claimed ID = %s, want %s", claimed.ID, created.ID)
	}
	if claimed.LockedBy == nil || *claimed.LockedBy != "worker-new" {
		t.Fatalf("locked_by = %v, want worker-new", claimed.LockedBy)
	}
}

func TestPGJobStoreClaimNextRejectsZeroLockFor(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	_, err := store.ClaimNext(ctx, testSyncID, "worker-1", 0)
	if !errors.Is(err, deltaflow.ErrInvalidLockFor) {
		t.Fatalf("error = %v, want ErrInvalidLockFor", err)
	}
}

func TestPGJobStoreMarkSynced(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	if _, err := store.Create(ctx, newManualJob(testSyncID)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	claimed, err := store.ClaimNext(ctx, testSyncID, "worker-1", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNext: (%v, %v)", claimed, err)
	}

	if err := store.MarkSynced(ctx, claimed.ID, false); err != nil {
		t.Fatalf("MarkSynced: %v", err)
	}

	got, ok, err := store.Get(ctx, claimed.ID)
	if err != nil || !ok {
		t.Fatalf("Get after MarkSynced: (ok=%v, %v)", ok, err)
	}
	if got.State != deltaflow.StateSynced {
		t.Fatalf("state = %s, want synced", got.State)
	}
	if got.GhostDetected {
		t.Fatal("ghost_detected = true, want false")
	}
	if got.SyncedAt == nil {
		t.Fatal("synced_at is nil")
	}
	if got.LockedBy != nil || got.LockedUntil != nil {
		t.Fatal("lock not cleared after MarkSynced")
	}
}

func TestPGJobStoreMarkSyncedGhostDetected(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	if _, err := store.Create(ctx, newManualJob(testSyncID)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	claimed, err := store.ClaimNext(ctx, testSyncID, "worker-1", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNext: (%v, %v)", claimed, err)
	}

	if err := store.MarkSynced(ctx, claimed.ID, true); err != nil {
		t.Fatalf("MarkSynced ghost: %v", err)
	}

	got, _, _ := store.Get(ctx, claimed.ID)
	if !got.GhostDetected {
		t.Fatal("ghost_detected = false, want true")
	}
	if got.State != deltaflow.StateSynced {
		t.Fatalf("state = %s, want synced", got.State)
	}
}

func TestPGJobStoreMarkRetrying(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	if _, err := store.Create(ctx, newManualJob(testSyncID)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	claimed, err := store.ClaimNext(ctx, testSyncID, "worker-1", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNext: (%v, %v)", claimed, err)
	}

	nextRun := time.Now().UTC().Add(5 * time.Second)
	if err := store.MarkRetrying(ctx, claimed.ID, errors.New("transient failure"), nextRun); err != nil {
		t.Fatalf("MarkRetrying: %v", err)
	}

	got, ok, err := store.Get(ctx, claimed.ID)
	if err != nil || !ok {
		t.Fatalf("Get after MarkRetrying: (ok=%v, %v)", ok, err)
	}
	if got.State != deltaflow.StateRetrying {
		t.Fatalf("state = %s, want retrying", got.State)
	}
	if got.AttemptCount != claimed.AttemptCount+1 {
		t.Fatalf("attempt_count = %d, want %d", got.AttemptCount, claimed.AttemptCount+1)
	}
	if got.LastError == nil || *got.LastError != "transient failure" {
		t.Fatalf("last_error = %v, want %q", got.LastError, "transient failure")
	}
	if got.LockedBy != nil || got.LockedUntil != nil {
		t.Fatal("lock not cleared after MarkRetrying")
	}
}

func TestPGJobStoreMarkDead(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	store := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})

	if _, err := store.Create(ctx, newManualJob(testSyncID)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	claimed, err := store.ClaimNext(ctx, testSyncID, "worker-1", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimNext: (%v, %v)", claimed, err)
	}

	if err := store.MarkDead(ctx, claimed.ID, errors.New("unrecoverable")); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	got, ok, err := store.Get(ctx, claimed.ID)
	if err != nil || !ok {
		t.Fatalf("Get after MarkDead: (ok=%v, %v)", ok, err)
	}
	if got.State != deltaflow.StateDead {
		t.Fatalf("state = %s, want dead", got.State)
	}
	if got.AttemptCount != claimed.AttemptCount+1 {
		t.Fatalf("attempt_count = %d, want %d", got.AttemptCount, claimed.AttemptCount+1)
	}
	if got.LastError == nil || *got.LastError != "unrecoverable" {
		t.Fatalf("last_error = %v, want %q", got.LastError, "unrecoverable")
	}
	if got.DeadAt == nil {
		t.Fatal("dead_at is nil")
	}
	if got.LockedBy != nil || got.LockedUntil != nil {
		t.Fatal("lock not cleared after MarkDead")
	}
}

// ---------------------------------------------------------------------------
// DispatchStore
// ---------------------------------------------------------------------------

func TestPGDispatchStoreDispatchPendingCreatesJobsAndMarkDeltas(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	ds := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	js := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})
	dispatcher := pgstore.NewDispatchStore(ds, js, pgstore.DispatchStoreConfig{})

	d1, err := ds.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Enqueue d1: %v", err)
	}
	d2, err := ds.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
	})
	if err != nil {
		t.Fatalf("Enqueue d2: %v", err)
	}

	jobs, err := dispatcher.DispatchPending(ctx, testSyncID, 10)
	if err != nil {
		t.Fatalf("DispatchPending: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("len(jobs) = %d, want 2", len(jobs))
	}
	for _, job := range jobs {
		if job.SyncID != testSyncID {
			t.Fatalf("job sync_id = %s, want %s", job.SyncID, testSyncID)
		}
		if job.State != deltaflow.StatePending {
			t.Fatalf("job state = %s, want pending", job.State)
		}
	}

	// Both deltas must be marked dispatched.
	for _, d := range []*deltaflow.Delta{d1, d2} {
		got, ok, err := ds.Get(ctx, d.ID)
		if err != nil || !ok {
			t.Fatalf("Get delta %s: (ok=%v, %v)", d.ID, ok, err)
		}
		if got.State != deltaflow.DeltaDispatched {
			t.Fatalf("delta %s state = %s, want dispatched", d.ID, got.State)
		}
		if got.DispatchedAt == nil {
			t.Fatalf("delta %s dispatched_at is nil", d.ID)
		}
	}
}

func TestPGDispatchStoreDispatchScopedToSyncID(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	ds := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	js := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})
	dispatcher := pgstore.NewDispatchStore(ds, js, pgstore.DispatchStoreConfig{})

	own, err := ds.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Enqueue own: %v", err)
	}
	foreign, err := ds.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "other-sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"99"`),
		},
	})
	if err != nil {
		t.Fatalf("Enqueue foreign: %v", err)
	}

	jobs, err := dispatcher.DispatchPending(ctx, testSyncID, 10)
	if err != nil {
		t.Fatalf("DispatchPending: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}

	ownGot, _, _ := ds.Get(ctx, own.ID)
	if ownGot.State != deltaflow.DeltaDispatched {
		t.Fatalf("own delta state = %s, want dispatched", ownGot.State)
	}

	foreignGot, _, _ := ds.Get(ctx, foreign.ID)
	if foreignGot.State != deltaflow.DeltaPending {
		t.Fatalf("foreign delta state = %s, want pending (untouched)", foreignGot.State)
	}
}

func TestPGDispatchStoreSecondCallProducesNoNewJobs(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	ds := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	js := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})
	dispatcher := pgstore.NewDispatchStore(ds, js, pgstore.DispatchStoreConfig{})

	if _, err := ds.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	first, err := dispatcher.DispatchPending(ctx, testSyncID, 10)
	if err != nil || len(first) != 1 {
		t.Fatalf("first DispatchPending: (len=%d, %v)", len(first), err)
	}

	// Delta is now 'dispatched'; a second call must find nothing pending.
	second, err := dispatcher.DispatchPending(ctx, testSyncID, 10)
	if err != nil {
		t.Fatalf("second DispatchPending: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second DispatchPending returned %d jobs, want 0", len(second))
	}
}

func TestPGDispatchStoreConflictOnOutboxDeltaProducesNoError(t *testing.T) {
	db := openTestDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	ds := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	js := pgstore.NewJobStore(db, pgstore.JobStoreConfig{})
	dispatcher := pgstore.NewDispatchStore(ds, js, pgstore.DispatchStoreConfig{})

	delta, err := ds.Enqueue(ctx, deltaflow.Delta{
		SyncID:         testSyncID,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  testKey,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// First dispatch creates the outbox job and marks delta dispatched.
	first, err := dispatcher.DispatchPending(ctx, testSyncID, 10)
	if err != nil || len(first) != 1 {
		t.Fatalf("first DispatchPending: (len=%d, %v)", len(first), err)
	}

	// Revert delta to pending manually so the dispatcher sees it again;
	// the ON CONFLICT DO NOTHING clause must silently skip re-creating the job.
	if _, err := db.ExecContext(ctx,
		"UPDATE deltaflow.deltaflow_deltas SET state = 'pending', dispatched_at = NULL WHERE id = $1::uuid",
		string(delta.ID)); err != nil {
		t.Fatalf("reset delta state: %v", err)
	}

	second, err := dispatcher.DispatchPending(ctx, testSyncID, 10)
	if err != nil {
		t.Fatalf("second DispatchPending after conflict: %v", err)
	}
	// The existing outbox job already exists; ON CONFLICT DO NOTHING fires,
	// so no new job is returned but the call must not error.
	if len(second) > 1 {
		t.Fatalf("expected at most 1 job (conflict skipped), got %d", len(second))
	}
}
