package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestSyncWorkerMarksUpsertSynced(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDeltaStore()
	inserted := insertTestDelta(t, ctx, store, deltaflow.Delta{ID: "delta-upsert"})

	var applied []deltaflow.ProjectionOperation
	worker := SyncWorker{
		Store: store,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{
				Identity:  identity,
				Payload:   []byte(`{"name":"Ada"}`),
				MediaType: "application/json",
				Checksum:  "checksum",
			}, nil
		}),
		Applier:  recordApplier(&applied, nil),
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetDelta(t, ctx, store, inserted.ID)
	if got.State != deltaflow.StateSynced {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateSynced)
	}
	if got.GhostDetected {
		t.Fatal("ghost_detected = true, want false")
	}
	if got.AttemptCount != 0 {
		t.Fatalf("attempt_count = %d, want 0", got.AttemptCount)
	}
	if len(applied) != 1 || applied[0].Type != deltaflow.ProjectionOpUpsert {
		t.Fatalf("applied operations = %#v, want one upsert", applied)
	}
}

func TestSyncWorkerMarksGhostDeleteSynced(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDeltaStore()
	inserted := insertTestDelta(t, ctx, store, deltaflow.Delta{ID: "delta-ghost"})

	var applied []deltaflow.ProjectionOperation
	worker := SyncWorker{
		Store: store,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
		}),
		Applier:  recordApplier(&applied, nil),
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetDelta(t, ctx, store, inserted.ID)
	if got.State != deltaflow.StateSynced {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateSynced)
	}
	if !got.GhostDetected {
		t.Fatal("ghost_detected = false, want true")
	}
	if len(applied) != 1 || applied[0].Type != deltaflow.ProjectionOpDelete {
		t.Fatalf("applied operations = %#v, want one delete", applied)
	}
}

func TestSyncWorkerRetriesFailedApply(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDeltaStore()
	inserted := insertTestDelta(t, ctx, store, deltaflow.Delta{
		ID:          "delta-retry",
		MaxAttempts: 3,
	})

	errApply := errors.New("apply failed")
	worker := SyncWorker{
		Store: store,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:  recordApplier(nil, errApply),
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetDelta(t, ctx, store, inserted.ID)
	if got.State != deltaflow.StateRetrying {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateRetrying)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", got.AttemptCount)
	}
	if got.LastError == nil || *got.LastError != errApply.Error() {
		t.Fatalf("last_error = %v, want %q", got.LastError, errApply.Error())
	}
	if got.LockedBy != nil || got.LockedUntil != nil {
		t.Fatal("lock was not cleared after retry")
	}
}

func TestSyncWorkerMarksDeadAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDeltaStore()
	inserted := insertTestDelta(t, ctx, store, deltaflow.Delta{
		ID:           "delta-dead",
		AttemptCount: 4,
		MaxAttempts:  5,
	})

	errApply := errors.New("still failing")
	worker := SyncWorker{
		Store: store,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:  recordApplier(nil, errApply),
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetDelta(t, ctx, store, inserted.ID)
	if got.State != deltaflow.StateDead {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateDead)
	}
	if got.AttemptCount != 5 {
		t.Fatalf("attempt_count = %d, want 5", got.AttemptCount)
	}
	if got.DeadAt == nil {
		t.Fatal("dead_at is nil")
	}
	if got.LockedBy != nil || got.LockedUntil != nil {
		t.Fatal("lock was not cleared after dead")
	}
}

func insertTestDelta(t *testing.T, ctx context.Context, store *MemoryDeltaStore, delta deltaflow.Delta) *deltaflow.Delta {
	t.Helper()

	delta.SyncID = "sync"
	delta.ProjectionType = "Contact"
	delta.ProjectionKey = deltaflow.ProjectionKey{
		"contact_id": json.RawMessage(`"1"`),
	}

	inserted, err := store.Insert(ctx, delta)
	if err != nil {
		t.Fatalf("Insert returned error: %v", err)
	}
	return inserted
}

func mustGetDelta(t *testing.T, ctx context.Context, store *MemoryDeltaStore, deltaID string) *deltaflow.Delta {
	t.Helper()

	delta, ok, err := store.Get(ctx, deltaID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !ok {
		t.Fatalf("delta %q not found", deltaID)
	}
	return delta
}

func recordApplier(applied *[]deltaflow.ProjectionOperation, err error) deltaflow.ProjectionApplier {
	return deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
		if applied != nil {
			*applied = append(*applied, op)
		}
		return err
	})
}
