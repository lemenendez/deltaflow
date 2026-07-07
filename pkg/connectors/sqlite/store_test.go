package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"

	_ "modernc.org/sqlite"
)

func TestDeltaStoreEnqueuePullAndMarkDispatched(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewDeltaStore(db, connectors.DeltaStoreConfig{})

	inserted, err := store.Enqueue(context.Background(), deltaflow.Delta{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-1"`)},
	})
	if err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}
	if inserted == nil || inserted.ID == "" {
		t.Fatalf("inserted = %#v, want non-nil with id", inserted)
	}

	pulled, err := store.Pull(context.Background(), "contacts-sync", 10)
	if err != nil {
		t.Fatalf("Pull error: %v", err)
	}
	if len(pulled) != 1 {
		t.Fatalf("len(pulled) = %d, want 1", len(pulled))
	}

	if err := store.MarkDispatched(context.Background(), inserted.ID); err != nil {
		t.Fatalf("MarkDispatched error: %v", err)
	}
	if err := store.MarkDispatched(context.Background(), inserted.ID); err != nil {
		t.Fatalf("MarkDispatched idempotent error: %v", err)
	}

	updated, ok, err := store.Get(context.Background(), inserted.ID)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !ok {
		t.Fatal("Get ok = false, want true")
	}
	if updated.State != deltaflow.DeltaDispatched {
		t.Fatalf("state = %q, want %q", updated.State, deltaflow.DeltaDispatched)
	}
	if updated.DispatchedAt == nil {
		t.Fatal("DispatchedAt = nil, want timestamp")
	}
}

func TestDeltaStoreEnqueueBatchIsWindowIdempotent(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewDeltaStore(db, connectors.DeltaStoreConfig{})
	deltas := []deltaflow.Delta{
		{SyncID: "sync", ProjectionType: "Customer", ProjectionKey: deltaflow.ProjectionKey{"id": json.RawMessage(`"1"`)}, DedupWindow: "customers-2026"},
		{SyncID: "sync", ProjectionType: "Customer", ProjectionKey: deltaflow.ProjectionKey{"id": json.RawMessage(`"2"`)}, DedupWindow: "customers-2026"},
	}
	first, err := store.EnqueueBatch(context.Background(), deltas)
	if err != nil {
		t.Fatalf("first EnqueueBatch: %v", err)
	}
	if first.InsertedCount != 2 || first.DuplicateCount != 0 {
		t.Fatalf("first result = %#v", first)
	}
	second, err := store.EnqueueBatch(context.Background(), deltas)
	if err != nil {
		t.Fatalf("second EnqueueBatch: %v", err)
	}
	if second.InsertedCount != 0 || second.DuplicateCount != 2 {
		t.Fatalf("second result = %#v", second)
	}
}

func TestDeltaStoreEnqueueReturnsExistingWindowDuplicate(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewDeltaStore(db, connectors.DeltaStoreConfig{})
	delta := deltaflow.Delta{SyncID: "sync", ProjectionType: "Customer", ProjectionKey: deltaflow.ProjectionKey{"id": json.RawMessage(`"1"`)}, DedupWindow: "window"}
	first, err := store.Enqueue(context.Background(), delta)
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	second, err := store.Enqueue(context.Background(), delta)
	if err != nil {
		t.Fatalf("second Enqueue: %v", err)
	}
	if second.ID != first.ID || second.DedupKey == "" {
		t.Fatalf("duplicate = %#v, first = %#v", second, first)
	}
}

func TestDeltaStoreEnqueueBatchTxCommitAndRollback(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewDeltaStore(db, connectors.DeltaStoreConfig{})
	ctx := context.Background()
	batch := func(window string) []deltaflow.Delta {
		return []deltaflow.Delta{{SyncID: "sync", ProjectionType: "Customer", ProjectionKey: deltaflow.ProjectionKey{"id": json.RawMessage(`"1"`)}, DedupWindow: deltaflow.DedupWindow(window)}}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin commit tx: %v", err)
	}
	if _, err = store.EnqueueBatchTx(ctx, tx, batch("commit")); err != nil {
		t.Fatalf("EnqueueBatchTx commit: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin rollback tx: %v", err)
	}
	if _, err = store.EnqueueBatchTx(ctx, tx, batch("rollback")); err != nil {
		t.Fatalf("EnqueueBatchTx rollback: %v", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	var count int
	if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deltaflow_deltas`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("durable delta count = %d, want 1", count)
	}
}

func TestDeltaStoreConfiguredBatchLimit(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewDeltaStore(db, connectors.DeltaStoreConfig{MaxEnqueueBatchSize: 1})
	deltas := []deltaflow.Delta{{DedupWindow: "window"}, {DedupWindow: "window"}}
	_, err := store.EnqueueBatch(context.Background(), deltas)
	if !errors.Is(err, deltaflow.ErrEnqueueBatchTooLarge) {
		t.Fatalf("error = %v", err)
	}
}

func TestDeltaStoreEnqueueReturnsErrorWhenReadBackMissing(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewDeltaStore(db, connectors.DeltaStoreConfig{})

	if _, err := db.ExecContext(context.Background(), `
CREATE TRIGGER deltaflow_test_delete_delta_after_insert
AFTER INSERT ON deltaflow_deltas
BEGIN
	DELETE FROM deltaflow_deltas WHERE id = NEW.id;
END;`); err != nil {
		t.Fatalf("create delete-after-insert trigger error: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS deltaflow_test_delete_delta_after_insert`)
	})

	inserted, err := store.Enqueue(context.Background(), deltaflow.Delta{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-missing"`)},
	})
	if err == nil {
		t.Fatal("Enqueue error = nil, want non-nil when read-back row is missing")
	}
	if inserted != nil {
		t.Fatalf("Enqueue returned delta = %#v, want nil", inserted)
	}
	if !strings.Contains(err.Error(), "read-back missing") {
		t.Fatalf("Enqueue error = %v, want read-back missing message", err)
	}
}

func TestDispatchStoreIsIdempotentForOutbox(t *testing.T) {
	db := openSQLiteTestDB(t)
	deltaStore := NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := NewJobStore(db, JobStoreConfig{})
	dispatch := NewDispatchStore(deltaStore, jobStore, DispatchStoreConfig{})

	inserted, err := deltaStore.Enqueue(context.Background(), deltaflow.Delta{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-1"`)},
	})
	if err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}

	jobs, err := dispatch.DispatchPending(context.Background(), "contacts-sync", 10)
	if err != nil {
		t.Fatalf("DispatchPending error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}

	again, err := dispatch.DispatchPending(context.Background(), "contacts-sync", 10)
	if err != nil {
		t.Fatalf("DispatchPending second error: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("len(again) = %d, want 0", len(again))
	}

	job, err := jobStore.ClaimNext(context.Background(), "contacts-sync", "w1", 5*time.Second)
	if err != nil {
		t.Fatalf("ClaimNext error: %v", err)
	}
	if job == nil {
		t.Fatal("ClaimNext = nil, want claimed job")
	}
	if job.DeltaID == nil || *job.DeltaID != inserted.ID {
		t.Fatalf("job.DeltaID = %v, want %q", job.DeltaID, inserted.ID)
	}
}

func TestDispatchStoreDoesNotMaskUnexpectedOutboxInsertConstraintFailure(t *testing.T) {
	db := openSQLiteTestDB(t)
	deltaStore := NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := NewJobStore(db, JobStoreConfig{})
	dispatch := NewDispatchStore(deltaStore, jobStore, DispatchStoreConfig{})

	inserted, err := deltaStore.Enqueue(context.Background(), deltaflow.Delta{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-collision"`)},
	})
	if err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}

	if _, err := db.ExecContext(context.Background(), `
CREATE TRIGGER deltaflow_test_force_outbox_job_id_collision
BEFORE INSERT ON deltaflow_sync_jobs
WHEN NEW.origin = 'outbox'
BEGIN
	INSERT INTO deltaflow_sync_jobs (
		id,
		sync_id,
		delta_id,
		origin,
		projection_type,
		projection_key,
		projection_key_hash,
		state,
		attempt_count,
		max_attempts,
		available_at_micros,
		created_at_micros,
		updated_at_micros,
		ghost_detected
	)
	VALUES (
		NEW.id,
		'other-sync',
		NULL,
		'manual',
		NEW.projection_type,
		NEW.projection_key,
		NEW.projection_key_hash,
		'pending',
		0,
		NEW.max_attempts,
		NEW.available_at_micros,
		NEW.created_at_micros,
		NEW.updated_at_micros,
		0
	);
END;`); err != nil {
		t.Fatalf("create collision trigger error: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS deltaflow_test_force_outbox_job_id_collision`)
	})

	jobs, err := dispatch.DispatchPending(context.Background(), "contacts-sync", 10)
	if err == nil {
		t.Fatal("DispatchPending error = nil, want constraint failure")
	}
	if jobs != nil {
		t.Fatalf("DispatchPending jobs = %v, want nil on constraint failure", jobs)
	}

	updated, ok, err := deltaStore.Get(context.Background(), inserted.ID)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !ok {
		t.Fatal("Get ok = false, want true")
	}
	if updated.State != deltaflow.DeltaPending {
		t.Fatalf("delta state = %q, want %q", updated.State, deltaflow.DeltaPending)
	}
	if updated.DispatchedAt != nil {
		t.Fatalf("DispatchedAt = %v, want nil after failed dispatch", updated.DispatchedAt)
	}
}

func TestJobStoreLeaseOwnershipFlow(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewJobStore(db, JobStoreConfig{})

	created, err := store.Create(context.Background(), deltaflow.SyncJob{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-1"`)},
	})
	if err != nil {
		t.Fatalf("Create error: %v", err)
	}

	claimed, err := store.ClaimNext(context.Background(), "contacts-sync", "w1", 3*time.Second)
	if err != nil {
		t.Fatalf("ClaimNext error: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimNext = nil, want job")
	}
	if claimed.ID != created.ID {
		t.Fatalf("claimed.ID = %q, want %q", claimed.ID, created.ID)
	}

	if err := store.RenewLease(context.Background(), claimed.ID, "w2", 3*time.Second); !errors.Is(err, deltaflow.ErrJobLeaseNotOwned) {
		t.Fatalf("RenewLease wrong owner err = %v, want ErrJobLeaseNotOwned", err)
	}
	if err := store.MarkSynced(context.Background(), claimed.ID, "w1", false); err != nil {
		t.Fatalf("MarkSynced error: %v", err)
	}

	updated, ok, err := store.Get(context.Background(), claimed.ID)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !ok {
		t.Fatal("Get ok = false, want true")
	}
	if updated.State != deltaflow.StateSynced {
		t.Fatalf("state = %q, want %q", updated.State, deltaflow.StateSynced)
	}
}

func TestJobStoreCreateReturnsErrorWhenReadBackMissing(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewJobStore(db, JobStoreConfig{})

	if _, err := db.ExecContext(context.Background(), `
CREATE TRIGGER deltaflow_test_delete_job_after_insert
AFTER INSERT ON deltaflow_sync_jobs
BEGIN
	DELETE FROM deltaflow_sync_jobs WHERE id = NEW.id;
END;`); err != nil {
		t.Fatalf("create delete-after-insert trigger error: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS deltaflow_test_delete_job_after_insert`)
	})

	created, err := store.Create(context.Background(), deltaflow.SyncJob{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-missing"`)},
	})
	if err == nil {
		t.Fatal("Create error = nil, want non-nil when read-back row is missing")
	}
	if created != nil {
		t.Fatalf("Create returned job = %#v, want nil", created)
	}
	if !strings.Contains(err.Error(), "read-back missing") {
		t.Fatalf("Create error = %v, want read-back missing message", err)
	}
}

func TestJobStoreClaimNextBatchValidatesBeforeLimitZero(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewJobStore(db, JobStoreConfig{})

	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		jobs, err := store.ClaimNextBatch(ctx, "contacts-sync", "w1", 0, 5*time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ClaimNextBatch error = %v, want context.Canceled", err)
		}
		if jobs != nil {
			t.Fatalf("ClaimNextBatch jobs = %v, want nil", jobs)
		}
	})

	t.Run("invalid lockFor", func(t *testing.T) {
		jobs, err := store.ClaimNextBatch(context.Background(), "contacts-sync", "w1", 0, 0)
		if !errors.Is(err, deltaflow.ErrInvalidLockFor) {
			t.Fatalf("ClaimNextBatch error = %v, want ErrInvalidLockFor", err)
		}
		if jobs != nil {
			t.Fatalf("ClaimNextBatch jobs = %v, want nil", jobs)
		}
	})

	t.Run("zero limit still returns nil slice on valid input", func(t *testing.T) {
		jobs, err := store.ClaimNextBatch(context.Background(), "contacts-sync", "w1", 0, 5*time.Second)
		if err != nil {
			t.Fatalf("ClaimNextBatch error = %v, want nil", err)
		}
		if jobs != nil {
			t.Fatalf("ClaimNextBatch jobs = %v, want nil", jobs)
		}
	})
}

func TestJobStoreClaimNextBatchRequeuesOnMidBatchError(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewJobStore(db, JobStoreConfig{})

	claimedFirst, err := store.Create(context.Background(), deltaflow.SyncJob{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-batch-good"`)},
	})
	if err != nil {
		t.Fatalf("Create good job error: %v", err)
	}

	now := time.Now().UTC()
	nowMicros := microsFromTime(now)
	badID := "job-bad-json"
	if _, err := db.ExecContext(context.Background(), `
INSERT INTO deltaflow_sync_jobs (
	id,
	sync_id,
	delta_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	attempt_count,
	max_attempts,
	last_error,
	last_error_code,
	available_at_micros,
	locked_by,
	locked_until_micros,
	ghost_detected,
	synced_at_micros,
	dead_at_micros,
	created_at_micros,
	updated_at_micros
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		badID,
		"contacts-sync",
		nil,
		deltaflow.JobOriginManual,
		"contact",
		`{"contact_id":`,
		"bad-hash",
		deltaflow.StatePending,
		0,
		5,
		nil,
		nil,
		nowMicros+1,
		nil,
		nil,
		0,
		nil,
		nil,
		nowMicros+1,
		nowMicros+1,
	); err != nil {
		t.Fatalf("insert malformed job error: %v", err)
	}

	jobs, err := store.ClaimNextBatch(context.Background(), "contacts-sync", "w1", 2, 5*time.Second)
	if err == nil {
		t.Fatal("ClaimNextBatch error = nil, want malformed row error")
	}
	if jobs != nil {
		t.Fatalf("ClaimNextBatch jobs = %d, want nil on error", len(jobs))
	}

	requeued, ok, err := store.Get(context.Background(), claimedFirst.ID)
	if err != nil {
		t.Fatalf("Get first claimed job error: %v", err)
	}
	if !ok {
		t.Fatal("first claimed job missing after batch error")
	}
	if requeued.State != deltaflow.StateRetrying {
		t.Fatalf("first claimed state = %q, want %q", requeued.State, deltaflow.StateRetrying)
	}

	var processingCount int
	if err := db.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM deltaflow_sync_jobs
WHERE state = 'processing'`).Scan(&processingCount); err != nil {
		t.Fatalf("processing count query error: %v", err)
	}
	if processingCount != 0 {
		t.Fatalf("processing jobs = %d, want 0", processingCount)
	}
}

func TestClaimCandidateTxRejectsStaleClaimPredicate(t *testing.T) {
	db := openSQLiteTestDB(t)
	fixedNow := time.Unix(1_700_000_000, 0).UTC()
	store := NewJobStore(db, JobStoreConfig{Now: func() time.Time { return fixedNow }})

	created, err := store.Create(context.Background(), deltaflow.SyncJob{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-stale"`)},
	})
	if err != nil {
		t.Fatalf("Create error: %v", err)
	}

	nowMicros := microsFromTime(fixedNow)
	futureLock := microsFromTime(fixedNow.Add(30 * time.Second))
	if _, err := db.ExecContext(context.Background(), `
UPDATE deltaflow_sync_jobs
SET
	state = 'processing',
	locked_by = ?,
	locked_until_micros = ?,
	updated_at_micros = ?
WHERE id = ?`, "other-worker", futureLock, nowMicros, created.ID); err != nil {
		t.Fatalf("prepare stale row error: %v", err)
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx error: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	claimed, err := store.claimCandidateTx(context.Background(), tx, "contacts-sync", string(created.ID), "w1", nowMicros, microsFromTime(fixedNow.Add(5*time.Second)))
	if err != nil {
		t.Fatalf("claimCandidateTx error: %v", err)
	}
	if claimed {
		t.Fatal("claimCandidateTx = true, want false for stale candidate")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	got, ok, err := store.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !ok {
		t.Fatal("Get ok = false, want true")
	}
	if got.LockedBy == nil || *got.LockedBy != "other-worker" {
		t.Fatalf("LockedBy = %v, want other-worker", got.LockedBy)
	}
	if got.State != deltaflow.StateProcessing {
		t.Fatalf("state = %q, want %q", got.State, deltaflow.StateProcessing)
	}
}

func TestAcquireWorkerLockSingleton(t *testing.T) {
	db := openSQLiteTestDB(t)
	ctx := context.Background()

	release, err := AcquireWorkerLock(ctx, db, "w1", 5*time.Second)
	if err != nil {
		t.Fatalf("AcquireWorkerLock error: %v", err)
	}
	defer func() { _ = release(context.Background()) }()

	_, err = AcquireWorkerLock(ctx, db, "w2", 5*time.Second)
	if !errors.Is(err, ErrWorkerAlreadyRunning) {
		t.Fatalf("second AcquireWorkerLock err = %v, want ErrWorkerAlreadyRunning", err)
	}
}

func TestDeltaStoreEnqueueInTxCommitAndRollback(t *testing.T) {
	db := openSQLiteTestDB(t)
	store := NewDeltaStore(db, connectors.DeltaStoreConfig{})
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx commit path: %v", err)
	}
	inserted, err := store.EnqueueInTx(ctx, tx, deltaflow.Delta{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-commit"`)},
	})
	if err != nil {
		t.Fatalf("EnqueueInTx commit path error: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit error: %v", err)
	}
	if _, ok, err := store.Get(ctx, inserted.ID); err != nil || !ok {
		t.Fatalf("Get after commit = (%v, %v), want ok", err, ok)
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx rollback path: %v", err)
	}
	rolledBack, err := store.EnqueueInTx(ctx, tx, deltaflow.Delta{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: "contact",
		ProjectionKey:  deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-rollback"`)},
	})
	if err != nil {
		t.Fatalf("EnqueueInTx rollback path error: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback error: %v", err)
	}
	if _, ok, err := store.Get(ctx, rolledBack.ID); err != nil {
		t.Fatalf("Get after rollback error: %v", err)
	} else if ok {
		t.Fatal("rolled back delta still visible")
	}
}

func openSQLiteTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.ExecContext(context.Background(), `PRAGMA foreign_keys=ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}

	if _, err := ApplyMigrations(context.Background(), db); err != nil {
		t.Fatalf("ApplyMigrations error: %v", err)
	}

	return db
}
