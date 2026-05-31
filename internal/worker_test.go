package internal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestSyncWorkerRunOnceRejectsMissingSyncID(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{ID: "delta-misconfigured"})

	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:  recordApplier(nil, nil),
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	err := worker.RunOnce(ctx)
	if err == nil {
		t.Fatal("RunOnce returned nil error, want validation failure")
	}
	if !strings.Contains(err.Error(), "sync_id is required") {
		t.Fatalf("RunOnce error = %v, want sync_id validation failure", err)
	}

	if len(jobStore.jobs) != 0 {
		t.Fatalf("job store mutated = %d jobs, want 0", len(jobStore.jobs))
	}
	if got := mustGetDelta(t, ctx, deltaStore, "delta-misconfigured"); got.State != deltaflow.DeltaPending {
		t.Fatalf("delta state = %s, want %s", got.State, deltaflow.DeltaPending)
	}
}

func TestSyncWorkerMarksUpsertSynced(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{ID: "delta-upsert"})

	var applied []deltaflow.ProjectionOperation
	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{
				Identity:  identity,
				Payload:   []byte(`{"name":"Ada"}`),
				MediaType: "application/json",
				Checksum:  "checksum",
			}, nil
		}),
		Applier:  recordApplier(&applied, nil),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJobByDelta(t, ctx, jobStore, inserted.ID)
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
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{ID: "delta-ghost"})

	var applied []deltaflow.ProjectionOperation
	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
		}),
		Applier:  recordApplier(&applied, nil),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJobByDelta(t, ctx, jobStore, inserted.ID)
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
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{
		ID: "delta-retry",
	})

	errApply := errors.New("apply failed")
	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:  recordApplier(nil, errApply),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJobByDelta(t, ctx, jobStore, inserted.ID)
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
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{ID: "delta-dead"})
	job := seedJobForDelta(t, ctx, jobStore, inserted.ID, 4, 5)
	if err := deltaStore.MarkDispatched(ctx, inserted.ID); err != nil {
		t.Fatalf("MarkDispatched returned error: %v", err)
	}

	errApply := errors.New("still failing")
	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:  recordApplier(nil, errApply),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJob(t, ctx, jobStore, job.ID)
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

func TestSyncWorkerDispatchesDeltaToJobAtomically(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{ID: "delta-dispatch"})

	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:  recordApplier(nil, nil),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.dispatchDeltas(ctx); err != nil {
		t.Fatalf("dispatchDeltas returned error: %v", err)
	}

	gotDelta := mustGetDelta(t, ctx, deltaStore, inserted.ID)
	if gotDelta.State != deltaflow.DeltaDispatched {
		t.Fatalf("delta state = %s, want %s", gotDelta.State, deltaflow.DeltaDispatched)
	}
	if gotDelta.DispatchedAt == nil {
		t.Fatal("dispatched_at is nil")
	}

	job := mustGetJobByDelta(t, ctx, jobStore, inserted.ID)
	if job.DeltaID == nil || *job.DeltaID != inserted.ID {
		t.Fatalf("job delta_id = %v, want %s", job.DeltaID, inserted.ID)
	}
}

func TestSyncWorkerDispatchesOnlyOwnSyncDeltas(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	own := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{ID: "delta-own"})
	foreign, err := deltaStore.Enqueue(ctx, deltaflow.Delta{
		ID:             "delta-foreign",
		SyncID:         "other-sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
	})
	if err != nil {
		t.Fatalf("Enqueue foreign returned error: %v", err)
	}

	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:  recordApplier(nil, nil),
		SyncID:   "sync",
		WorkerID: "worker-1",
		PullSize: 10,
		LockFor:  time.Minute,
	}

	if err := worker.dispatchDeltas(ctx); err != nil {
		t.Fatalf("dispatchDeltas returned error: %v", err)
	}

	ownGot := mustGetDelta(t, ctx, deltaStore, own.ID)
	if ownGot.State != deltaflow.DeltaDispatched {
		t.Fatalf("own state = %s, want %s", ownGot.State, deltaflow.DeltaDispatched)
	}
	foreignGot := mustGetDelta(t, ctx, deltaStore, foreign.ID)
	if foreignGot.State != deltaflow.DeltaPending {
		t.Fatalf("foreign state = %s, want %s", foreignGot.State, deltaflow.DeltaPending)
	}
	if _, ok := jobStore.jobByDelta[foreign.ID]; ok {
		t.Fatal("dispatch created a job for foreign syncID")
	}
}

func TestSyncWorkerProcessesManualJobWithoutDispatcher(t *testing.T) {
	ctx := context.Background()
	jobStore := NewJobMemoryStore()

	job, err := jobStore.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"manual"`),
		},
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	var applied []deltaflow.ProjectionOperation
	worker := SyncWorker{
		JobStore: jobStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:  recordApplier(&applied, nil),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJob(t, ctx, jobStore, job.ID)
	if got.State != deltaflow.StateSynced {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateSynced)
	}
	if len(applied) != 1 || applied[0].Type != deltaflow.ProjectionOpUpsert {
		t.Fatalf("applied operations = %#v, want one upsert", applied)
	}
}

func enqueueTestDelta(t *testing.T, ctx context.Context, store *DeltaMemoryStore, delta deltaflow.Delta) *deltaflow.Delta {
	t.Helper()

	delta.SyncID = "sync"
	delta.Origin = deltaflow.OriginOperationInserted
	delta.ProjectionType = "Contact"
	delta.ProjectionKey = deltaflow.ProjectionKey{
		"contact_id": json.RawMessage(`"1"`),
	}

	inserted, err := store.Enqueue(ctx, delta)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	return inserted
}

func mustGetDelta(t *testing.T, ctx context.Context, store *DeltaMemoryStore, deltaID deltaflow.DeltaID) *deltaflow.Delta {
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

func seedJobForDelta(t *testing.T, ctx context.Context, store *JobMemoryStore, deltaID deltaflow.DeltaID, attemptCount int, maxAttempts int) *deltaflow.SyncJob {
	t.Helper()

	job, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		DeltaID:        cloneDeltaIDPtr(&deltaID),
		Origin:         deltaflow.JobOriginOutbox,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
		AttemptCount: attemptCount,
		MaxAttempts:  maxAttempts,
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	return job
}

func mustGetJob(t *testing.T, ctx context.Context, store *JobMemoryStore, jobID deltaflow.SyncJobID) *deltaflow.SyncJob {
	t.Helper()

	job, ok, err := store.Get(ctx, jobID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !ok {
		t.Fatalf("job %q not found", jobID)
	}
	return job
}

func mustGetJobByDelta(t *testing.T, ctx context.Context, store *JobMemoryStore, deltaID deltaflow.DeltaID) *deltaflow.SyncJob {
	t.Helper()

	store.mu.Lock()
	jobID, ok := store.jobByDelta[deltaID]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("job for delta %q not found", deltaID)
	}
	return mustGetJob(t, ctx, store, jobID)
}

func recordApplier(applied *[]deltaflow.ProjectionOperation, err error) deltaflow.ProjectionApplier {
	return deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
		if applied != nil {
			*applied = append(*applied, op)
		}
		return err
	})
}
