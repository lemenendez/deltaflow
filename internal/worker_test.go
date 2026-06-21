package internal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestSyncWorkerRunOnceRejectsMissingSyncID(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

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
	if got := mustGetDelta(t, ctx, deltaStore, inserted.ID); got.State != deltaflow.DeltaPending {
		t.Fatalf("delta state = %s, want %s", got.State, deltaflow.DeltaPending)
	}
}

func TestSyncWorkerRunOnceRejectsMissingWorkerID(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: recordApplier(nil, nil),
		SyncID:  "sync",
		LockFor: time.Minute,
	}

	err := worker.RunOnce(ctx)
	if err == nil {
		t.Fatal("RunOnce returned nil error, want validation failure")
	}
	if !strings.Contains(err.Error(), "worker_id is required") {
		t.Fatalf("RunOnce error = %v, want worker_id validation failure", err)
	}

	if len(jobStore.jobs) != 0 {
		t.Fatalf("job store mutated = %d jobs, want 0", len(jobStore.jobs))
	}
	if got := mustGetDelta(t, ctx, deltaStore, inserted.ID); got.State != deltaflow.DeltaPending {
		t.Fatalf("delta state = %s, want %s", got.State, deltaflow.DeltaPending)
	}
}

func TestSyncWorkerRunOnceRejectsMissingRequiredCollaborators(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		worker  SyncWorker
		wantErr string
	}{
		{
			name: "missing job store",
			worker: SyncWorker{
				Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
					return deltaflow.Projection{Identity: identity}, nil
				}),
				Applier:  recordApplier(nil, nil),
				SyncID:   "sync",
				WorkerID: "worker-1",
				LockFor:  time.Minute,
			},
			wantErr: "job_store is required",
		},
		{
			name: "missing projector",
			worker: SyncWorker{
				JobStore: NewJobMemoryStore(),
				Applier:  recordApplier(nil, nil),
				SyncID:   "sync",
				WorkerID: "worker-1",
				LockFor:  time.Minute,
			},
			wantErr: "projector is required",
		},
		{
			name: "missing applier",
			worker: SyncWorker{
				JobStore: NewJobMemoryStore(),
				Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
					return deltaflow.Projection{Identity: identity}, nil
				}),
				SyncID:   "sync",
				WorkerID: "worker-1",
				LockFor:  time.Minute,
			},
			wantErr: "applier is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.worker.RunOnce(ctx)
			if err == nil {
				t.Fatal("RunOnce returned nil error, want validation failure")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("RunOnce error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestSyncWorkerMarksUpsertSynced(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

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
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

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
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

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
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})
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
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

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

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
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
	own := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})
	foreign, err := deltaStore.Enqueue(ctx, deltaflow.Delta{
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

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
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

func TestSyncWorkerRenewsLeaseDuringLongApply(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	baseJobStore := NewJobMemoryStore()
	spyJobStore := newRenewLeaseSpyJobStore(baseJobStore)
	enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

	worker := SyncWorker{
		JobStore:   spyJobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, baseJobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			select {
			case <-spyJobStore.firstRenewed():
				time.Sleep(10 * time.Millisecond)
				return nil
			case <-time.After(500 * time.Millisecond):
				return errors.New("timed out waiting for lease renewal")
			}
		}),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  20 * time.Millisecond,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	if spyJobStore.renewCount() < 1 {
		t.Fatal("expected at least one lease renewal during apply")
	}

	jobs, err := baseJobStore.ClaimNext(ctx, "sync", "worker-2", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext after sync returned error: %v", err)
	}
	if jobs != nil {
		t.Fatalf("ClaimNext returned %v, want nil because job should be synced", jobs.ID)
	}
}

func TestSyncWorkerRetriesWhenLeaseHeartbeatFails(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	baseJobStore := NewJobMemoryStore()
	spyJobStore := newRenewLeaseSpyJobStoreWithError(baseJobStore, errors.New("renew failed"))
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

	worker := SyncWorker{
		JobStore:   spyJobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, baseJobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			select {
			case <-spyJobStore.firstRenewed():
				time.Sleep(10 * time.Millisecond)
				return nil
			case <-time.After(time.Second):
				return errors.New("timed out waiting for heartbeat failure")
			}
		}),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  300 * time.Millisecond,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJobByDelta(t, ctx, baseJobStore, inserted.ID)
	if got.State != deltaflow.StateRetrying {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateRetrying)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", got.AttemptCount)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "lease renewal failed") {
		t.Fatalf("last_error = %v, want lease renewal failure message", got.LastError)
	}
}

func TestSyncWorkerCancelsJobContextWhenLeaseHeartbeatFails(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	baseJobStore := NewJobMemoryStore()
	spyJobStore := newRenewLeaseSpyJobStoreWithError(baseJobStore, errors.New("renew failed"))
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

	worker := SyncWorker{
		JobStore:   spyJobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, baseJobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			select {
			case <-spyJobStore.firstRenewed():
			case <-time.After(time.Second):
				return errors.New("timed out waiting for heartbeat attempt")
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return errors.New("job context was not canceled after heartbeat failure")
			}
		}),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  300 * time.Millisecond,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJobByDelta(t, ctx, baseJobStore, inserted.ID)
	if got.State != deltaflow.StateRetrying {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateRetrying)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", got.AttemptCount)
	}
	if got.LastError == nil {
		t.Fatal("last_error is nil, want cancellation-related failure")
	}
}

func TestSyncWorkerPrefersHeartbeatErrorWhenApplyReturnsContextCanceled(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	baseJobStore := NewJobMemoryStore()
	spyJobStore := newRenewLeaseSpyJobStoreWithError(baseJobStore, errors.New("renew failed"))
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

	worker := SyncWorker{
		JobStore:   spyJobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, baseJobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			select {
			case <-spyJobStore.firstRenewed():
			case <-time.After(time.Second):
				return errors.New("timed out waiting for heartbeat attempt")
			}

			<-ctx.Done()
			return ctx.Err()
		}),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  300 * time.Millisecond,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJobByDelta(t, ctx, baseJobStore, inserted.ID)
	if got.State != deltaflow.StateRetrying {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateRetrying)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", got.AttemptCount)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "lease renewal failed") {
		t.Fatalf("last_error = %v, want lease renewal failure message", got.LastError)
	}
}

func TestSyncWorkerPrefersHeartbeatErrorWhenDeleteApplyReturnsContextCanceled(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	baseJobStore := NewJobMemoryStore()
	spyJobStore := newRenewLeaseSpyJobStoreWithError(baseJobStore, errors.New("renew failed"))
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

	worker := SyncWorker{
		JobStore:   spyJobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, baseJobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			if op.Type != deltaflow.ProjectionOpDelete {
				return errors.New("expected delete operation")
			}

			select {
			case <-spyJobStore.firstRenewed():
			case <-time.After(time.Second):
				return errors.New("timed out waiting for heartbeat attempt")
			}

			<-ctx.Done()
			return ctx.Err()
		}),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  300 * time.Millisecond,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJobByDelta(t, ctx, baseJobStore, inserted.ID)
	if got.State != deltaflow.StateRetrying {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateRetrying)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", got.AttemptCount)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "lease renewal failed") {
		t.Fatalf("last_error = %v, want lease renewal failure message", got.LastError)
	}
}

func TestSyncWorkerPrefersHeartbeatErrorWhenProjectReturnsContextCanceled(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	baseJobStore := NewJobMemoryStore()
	spyJobStore := newRenewLeaseSpyJobStoreWithError(baseJobStore, errors.New("renew failed"))
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

	worker := SyncWorker{
		JobStore:   spyJobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, baseJobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			select {
			case <-spyJobStore.firstRenewed():
			case <-time.After(time.Second):
				return deltaflow.Projection{}, errors.New("timed out waiting for heartbeat attempt")
			}

			<-ctx.Done()
			return deltaflow.Projection{}, ctx.Err()
		}),
		Applier:  recordApplier(nil, nil),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  300 * time.Millisecond,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJobByDelta(t, ctx, baseJobStore, inserted.ID)
	if got.State != deltaflow.StateRetrying {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateRetrying)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", got.AttemptCount)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "lease renewal failed") {
		t.Fatalf("last_error = %v, want lease renewal failure message", got.LastError)
	}
}

func TestSyncWorkerMarksDeadWhenLeaseHeartbeatFailsAtLastAttempt(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	baseJobStore := NewJobMemoryStore()
	spyJobStore := newRenewLeaseSpyJobStoreWithError(baseJobStore, errors.New("renew failed"))
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})
	job := seedJobForDelta(t, ctx, baseJobStore, inserted.ID, 4, 5)
	if err := deltaStore.MarkDispatched(ctx, inserted.ID); err != nil {
		t.Fatalf("MarkDispatched returned error: %v", err)
	}

	worker := SyncWorker{
		JobStore:   spyJobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, baseJobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			select {
			case <-spyJobStore.firstRenewed():
				time.Sleep(10 * time.Millisecond)
				return nil
			case <-time.After(time.Second):
				return errors.New("timed out waiting for heartbeat failure")
			}
		}),
		SyncID:   "sync",
		WorkerID: "worker-1",
		LockFor:  300 * time.Millisecond,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	got := mustGetJob(t, ctx, baseJobStore, job.ID)
	if got.State != deltaflow.StateDead {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateDead)
	}
	if got.AttemptCount != 5 {
		t.Fatalf("attempt_count = %d, want 5", got.AttemptCount)
	}
	if got.DeadAt == nil {
		t.Fatal("dead_at is nil")
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "lease renewal failed") {
		t.Fatalf("last_error = %v, want lease renewal failure message", got.LastError)
	}
}

func TestSyncWorkerProcessesBatchedJobsConcurrently(t *testing.T) {
	ctx := context.Background()
	jobStore := NewJobMemoryStore()

	firstJob, err := jobStore.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Create first job returned error: %v", err)
	}
	secondJob, err := jobStore.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
	})
	if err != nil {
		t.Fatalf("Create second job returned error: %v", err)
	}

	var mu sync.Mutex
	running := 0
	maxRunning := 0
	started := make(chan struct{})
	var startedOnce sync.Once
	release := make(chan struct{})

	worker := SyncWorker{
		JobStore: jobStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			mu.Lock()
			running++
			if running > maxRunning {
				maxRunning = running
			}
			if running == 2 {
				startedOnce.Do(func() { close(started) })
			}
			mu.Unlock()

			select {
			case <-release:
			case <-time.After(time.Second):
				return errors.New("timed out waiting for concurrent processing release")
			}

			mu.Lock()
			running--
			mu.Unlock()
			return nil
		}),
		SyncID:      "sync",
		WorkerID:    "worker-1",
		LockFor:     time.Hour,
		BatchSize:   1,
		Concurrency: 2,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.RunOnce(ctx)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for both workers to enter applier")
	}

	close(release)

	if err := <-errCh; err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	if maxRunning != 2 {
		t.Fatalf("max concurrent applies = %d, want 2", maxRunning)
	}

	for _, job := range []*deltaflow.SyncJob{firstJob, secondJob} {
		got := mustGetJob(t, ctx, jobStore, job.ID)
		if got.State != deltaflow.StateSynced {
			t.Fatalf("job %s state = %s, want %s", job.ID, got.State, deltaflow.StateSynced)
		}
	}
}

func TestSyncWorkerPreservesRetryAndDeadOutcomesWithinBatch(t *testing.T) {
	ctx := context.Background()
	jobStore := NewJobMemoryStore()

	retryJob, err := jobStore.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"retry"`),
		},
	})
	if err != nil {
		t.Fatalf("Create retry job returned error: %v", err)
	}
	deadJob, err := jobStore.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"dead"`),
		},
		AttemptCount: 1,
		MaxAttempts:  2,
	})
	if err != nil {
		t.Fatalf("Create dead job returned error: %v", err)
	}
	_ = deadJob

	worker := SyncWorker{
		JobStore: jobStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			switch string(op.Identity.Key["contact_id"]) {
			case `"retry"`:
				return errors.New("retryable apply failure")
			case `"dead"`:
				return errors.New("terminal apply failure")
			default:
				return nil
			}
		}),
		SyncID:    "sync",
		WorkerID:  "worker-1",
		LockFor:   time.Hour,
		BatchSize: 2,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	retryGot := mustGetJob(t, ctx, jobStore, retryJob.ID)
	if retryGot.State != deltaflow.StateRetrying {
		t.Fatalf("retry job state = %s, want %s", retryGot.State, deltaflow.StateRetrying)
	}
	if retryGot.AttemptCount != 1 {
		t.Fatalf("retry job attempt_count = %d, want 1", retryGot.AttemptCount)
	}
	if retryGot.LastError == nil || *retryGot.LastError != "retryable apply failure" {
		t.Fatalf("retry job last_error = %v, want retryable apply failure", retryGot.LastError)
	}

	deadGot := mustGetJob(t, ctx, jobStore, deadJob.ID)
	if deadGot.State != deltaflow.StateDead {
		t.Fatalf("dead job state = %s, want %s", deadGot.State, deltaflow.StateDead)
	}
	if deadGot.AttemptCount != 2 {
		t.Fatalf("dead job attempt_count = %d, want 2", deadGot.AttemptCount)
	}
	if deadGot.DeadAt == nil {
		t.Fatal("dead job dead_at is nil")
	}
	if deadGot.LastError == nil || *deadGot.LastError != "terminal apply failure" {
		t.Fatalf("dead job last_error = %v, want terminal apply failure", deadGot.LastError)
	}
}

func TestSyncWorkerFallsBackToClaimNextWhenBatchClaimsUnavailable(t *testing.T) {
	ctx := context.Background()
	jobStore := newClaimNextOnlyJobStore(NewJobMemoryStore())

	firstJob, err := jobStore.inner.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Create first job returned error: %v", err)
	}
	secondJob, err := jobStore.inner.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
	})
	if err != nil {
		t.Fatalf("Create second job returned error: %v", err)
	}

	worker := SyncWorker{
		JobStore: jobStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:     recordApplier(nil, nil),
		SyncID:      "sync",
		WorkerID:    "worker-1",
		LockFor:     time.Hour,
		BatchSize:   3,
		Concurrency: 1,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	if got := jobStore.claimCount(); got != 3 {
		t.Fatalf("ClaimNext call count = %d, want 3", got)
	}

	for _, job := range []*deltaflow.SyncJob{firstJob, secondJob} {
		got := mustGetJob(t, ctx, jobStore.inner, job.ID)
		if got.State != deltaflow.StateSynced {
			t.Fatalf("job %s state = %s, want %s", job.ID, got.State, deltaflow.StateSynced)
		}
	}
}

func TestSyncWorkerRequeuesPartiallyClaimedJobsWhenClaimNextFails(t *testing.T) {
	ctx := context.Background()
	claimErr := errors.New("claim failed")
	jobStore := newClaimNextOnlyJobStoreWithClaimErrorAfter(NewJobMemoryStore(), 2, claimErr)

	job, err := jobStore.inner.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Create job returned error: %v", err)
	}

	worker := SyncWorker{
		JobStore: jobStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:   recordApplier(nil, nil),
		SyncID:    "sync",
		WorkerID:  "worker-1",
		LockFor:   time.Hour,
		BatchSize: 3,
	}

	err = worker.RunOnce(ctx)
	if !errors.Is(err, claimErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, claimErr)
	}

	if got := jobStore.claimCount(); got != 2 {
		t.Fatalf("ClaimNext call count = %d, want 2", got)
	}

	got := mustGetJob(t, ctx, jobStore.inner, job.ID)
	if got.State != deltaflow.StateRetrying {
		t.Fatalf("job state = %s, want %s", got.State, deltaflow.StateRetrying)
	}
	if got.AttemptCount != 0 {
		t.Fatalf("attempt_count = %d, want 0", got.AttemptCount)
	}
	if got.LockedBy != nil || got.LockedUntil != nil {
		t.Fatal("lock was not cleared after claim failure requeue")
	}
	if got.LastError == nil || *got.LastError != claimErr.Error() {
		t.Fatalf("last_error = %v, want %q", got.LastError, claimErr.Error())
	}
}

func TestSyncWorkerRunOnceReturnsFirstWorkerFailure(t *testing.T) {
	ctx := context.Background()
	rootErr := errors.New("root failure")
	jobStore := newWorkerErrorOrderJobStore(NewJobMemoryStore(), "worker-1", rootErr)

	worker := SyncWorker{
		JobStore: jobStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:     recordApplier(nil, nil),
		SyncID:      "sync",
		WorkerID:    "worker-1",
		LockFor:     time.Hour,
		BatchSize:   1,
		Concurrency: 2,
	}

	err := worker.RunOnce(ctx)
	if !errors.Is(err, rootErr) {
		t.Fatalf("RunOnce error = %v, want %v", err, rootErr)
	}
}

func TestSyncWorkerDerivesDefaultPullSizeFromConcurrencyAndBatchSize(t *testing.T) {
	ctx := context.Background()
	jobStore := NewJobMemoryStore()
	dispatcher := &dispatchSpyStore{}

	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: dispatcher,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:     recordApplier(nil, nil),
		SyncID:      "sync",
		WorkerID:    "worker-1",
		LockFor:     time.Hour,
		BatchSize:   4,
		Concurrency: 3,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	if got := dispatcher.lastLimit(); got != 12 {
		t.Fatalf("DispatchPending limit = %d, want 12", got)
	}
}

func TestSyncWorkerUsesExplicitPullSizeOverDerivedDefault(t *testing.T) {
	ctx := context.Background()
	jobStore := NewJobMemoryStore()
	dispatcher := &dispatchSpyStore{}

	worker := SyncWorker{
		JobStore:   jobStore,
		Dispatcher: dispatcher,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier:     recordApplier(nil, nil),
		SyncID:      "sync",
		WorkerID:    "worker-1",
		LockFor:     time.Hour,
		PullSize:    7,
		BatchSize:   10,
		Concurrency: 10,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	if got := dispatcher.lastLimit(); got != 7 {
		t.Fatalf("DispatchPending limit = %d, want 7", got)
	}
}

func TestSyncWorkerCancellationRequeuesUnprocessedClaimedJobs(t *testing.T) {
	baseCtx := context.Background()
	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()

	jobStore := NewJobMemoryStore()

	firstJob, err := jobStore.Create(baseCtx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Create first job returned error: %v", err)
	}
	secondJob, err := jobStore.Create(baseCtx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
	})
	if err != nil {
		t.Fatalf("Create second job returned error: %v", err)
	}

	var applyCalls int
	worker := SyncWorker{
		JobStore: jobStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			applyCalls++
			if applyCalls == 1 {
				cancel()
			}
			return nil
		}),
		SyncID:    "sync",
		WorkerID:  "worker-1",
		LockFor:   time.Hour,
		BatchSize: 2,
	}

	err = worker.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce error = %v, want context canceled", err)
	}

	if applyCalls != 1 {
		t.Fatalf("apply call count = %d, want 1", applyCalls)
	}

	states := map[deltaflow.SyncJobState]int{}
	for _, id := range []deltaflow.SyncJobID{firstJob.ID, secondJob.ID} {
		got := mustGetJob(t, baseCtx, jobStore, id)
		states[got.State]++
		if got.State == deltaflow.StateProcessing {
			t.Fatalf("job %s remained in processing after cancellation", id)
		}
		if got.State == deltaflow.StateRetrying && got.AttemptCount != 0 {
			t.Fatalf("requeued attempt_count = %d, want 0", got.AttemptCount)
		}
	}

	if states[deltaflow.StateSynced] != 1 {
		t.Fatalf("synced jobs = %d, want 1", states[deltaflow.StateSynced])
	}
	if states[deltaflow.StateRetrying] != 1 {
		t.Fatalf("retrying jobs = %d, want 1", states[deltaflow.StateRetrying])
	}
}

func TestSyncWorkerRequeuesAllRemainingJobsWithSlowRequeueCalls(t *testing.T) {
	baseCtx := context.Background()
	ctx, cancel := context.WithCancel(baseCtx)
	defer cancel()
	baseStore := NewJobMemoryStore()
	jobStore := newSlowRequeueJobStore(baseStore, []time.Duration{1900 * time.Millisecond, 300 * time.Millisecond})

	firstJob, err := baseStore.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Create first job returned error: %v", err)
	}
	secondJob, err := baseStore.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
	})
	if err != nil {
		t.Fatalf("Create second job returned error: %v", err)
	}
	thirdJob, err := baseStore.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"3"`),
		},
	})
	if err != nil {
		t.Fatalf("Create third job returned error: %v", err)
	}

	applyCalls := 0
	worker := SyncWorker{
		JobStore: jobStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: identity}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(ctx context.Context, op deltaflow.ProjectionOperation) error {
			applyCalls++
			if applyCalls == 1 {
				cancel()
			}
			return nil
		}),
		SyncID:    "sync",
		WorkerID:  "worker-1",
		LockFor:   time.Hour,
		BatchSize: 3,
	}

	err = worker.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce error = %v, want context canceled", err)
	}

	if applyCalls != 1 {
		t.Fatalf("apply call count = %d, want 1", applyCalls)
	}
	if got := jobStore.requeueCount(); got != 2 {
		t.Fatalf("RequeueClaimed call count = %d, want 2", got)
	}

	firstGot := mustGetJob(t, baseCtx, baseStore, firstJob.ID)
	if firstGot.State != deltaflow.StateSynced {
		t.Fatalf("first job state = %s, want %s", firstGot.State, deltaflow.StateSynced)
	}
	if firstGot.AttemptCount != 0 {
		t.Fatalf("first job attempt_count = %d, want 0", firstGot.AttemptCount)
	}

	for _, id := range []deltaflow.SyncJobID{secondJob.ID, thirdJob.ID} {
		got := mustGetJob(t, baseCtx, baseStore, id)
		if got.State != deltaflow.StateRetrying {
			t.Fatalf("job %s state = %s, want %s", id, got.State, deltaflow.StateRetrying)
		}
		if got.AttemptCount != 0 {
			t.Fatalf("job %s attempt_count = %d, want 0", id, got.AttemptCount)
		}
		if got.LockedBy != nil || got.LockedUntil != nil {
			t.Fatalf("job %s lock was not cleared after requeue", id)
		}
		if got.LastError == nil || *got.LastError != context.Canceled.Error() {
			t.Fatalf("job %s last_error = %v, want %q", id, got.LastError, context.Canceled.Error())
		}
	}
}

type renewLeaseSpyJobStore struct {
	inner        deltaflow.JobStore
	mu           sync.Mutex
	renewedCount int
	renewErr     error
	renewedCh    chan struct{}
	renewedOnce  sync.Once
}

type slowRequeueJobStore struct {
	inner        *JobMemoryStore
	delays       []time.Duration
	mu           sync.Mutex
	requeueCalls int
}

type workerErrorOrderJobStore struct {
	inner       *JobMemoryStore
	failingID   string
	failingErr  error
	errorRaised chan struct{}
}

func newRenewLeaseSpyJobStore(inner deltaflow.JobStore) *renewLeaseSpyJobStore {
	return &renewLeaseSpyJobStore{
		inner:     inner,
		renewedCh: make(chan struct{}),
	}
}

func newRenewLeaseSpyJobStoreWithError(inner deltaflow.JobStore, renewErr error) *renewLeaseSpyJobStore {
	store := newRenewLeaseSpyJobStore(inner)
	store.renewErr = renewErr
	return store
}

func newSlowRequeueJobStore(inner *JobMemoryStore, delays []time.Duration) *slowRequeueJobStore {
	copiedDelays := append([]time.Duration(nil), delays...)
	return &slowRequeueJobStore{inner: inner, delays: copiedDelays}
}

func newWorkerErrorOrderJobStore(inner *JobMemoryStore, failingID string, failingErr error) *workerErrorOrderJobStore {
	return &workerErrorOrderJobStore{
		inner:       inner,
		failingID:   failingID,
		failingErr:  failingErr,
		errorRaised: make(chan struct{}),
	}
}

func (s *workerErrorOrderJobStore) Create(ctx context.Context, job deltaflow.SyncJob) (*deltaflow.SyncJob, error) {
	return s.inner.Create(ctx, job)
}

func (s *workerErrorOrderJobStore) Get(ctx context.Context, jobID deltaflow.SyncJobID) (*deltaflow.SyncJob, bool, error) {
	return s.inner.Get(ctx, jobID)
}

func (s *workerErrorOrderJobStore) ClaimNext(ctx context.Context, syncID deltaflow.SyncID, workerID string, lockFor time.Duration) (*deltaflow.SyncJob, error) {
	if workerID == s.failingID {
		select {
		case <-s.errorRaised:
		default:
			close(s.errorRaised)
		}
		return nil, s.failingErr
	}

	select {
	case <-s.errorRaised:
		<-ctx.Done()
		return nil, ctx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *workerErrorOrderJobStore) RenewLease(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, lockFor time.Duration) error {
	return s.inner.RenewLease(ctx, jobID, workerID, lockFor)
}

func (s *workerErrorOrderJobStore) MarkSynced(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, ghostDetected bool) error {
	return s.inner.MarkSynced(ctx, jobID, workerID, ghostDetected)
}

func (s *workerErrorOrderJobStore) MarkRetrying(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error, nextRunAt time.Time) error {
	return s.inner.MarkRetrying(ctx, jobID, workerID, err, nextRunAt)
}

func (s *workerErrorOrderJobStore) RequeueClaimed(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, reason error, nextRunAt time.Time) error {
	return s.inner.RequeueClaimed(ctx, jobID, workerID, reason, nextRunAt)
}

func (s *workerErrorOrderJobStore) MarkDead(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error) error {
	return s.inner.MarkDead(ctx, jobID, workerID, err)
}

func (s *slowRequeueJobStore) Create(ctx context.Context, job deltaflow.SyncJob) (*deltaflow.SyncJob, error) {
	return s.inner.Create(ctx, job)
}

func (s *slowRequeueJobStore) Get(ctx context.Context, jobID deltaflow.SyncJobID) (*deltaflow.SyncJob, bool, error) {
	return s.inner.Get(ctx, jobID)
}

func (s *slowRequeueJobStore) ClaimNext(ctx context.Context, syncID deltaflow.SyncID, workerID string, lockFor time.Duration) (*deltaflow.SyncJob, error) {
	return s.inner.ClaimNext(ctx, syncID, workerID, lockFor)
}

func (s *slowRequeueJobStore) ClaimNextBatch(ctx context.Context, syncID deltaflow.SyncID, workerID string, limit int, lockFor time.Duration) ([]*deltaflow.SyncJob, error) {
	return s.inner.ClaimNextBatch(ctx, syncID, workerID, limit, lockFor)
}

func (s *slowRequeueJobStore) RenewLease(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, lockFor time.Duration) error {
	return s.inner.RenewLease(ctx, jobID, workerID, lockFor)
}

func (s *slowRequeueJobStore) MarkSynced(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, ghostDetected bool) error {
	return s.inner.MarkSynced(ctx, jobID, workerID, ghostDetected)
}

func (s *slowRequeueJobStore) MarkRetrying(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error, nextRunAt time.Time) error {
	return s.inner.MarkRetrying(ctx, jobID, workerID, err, nextRunAt)
}

func (s *slowRequeueJobStore) RequeueClaimed(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, reason error, nextRunAt time.Time) error {
	s.mu.Lock()
	s.requeueCalls++
	call := s.requeueCalls
	delay := time.Duration(0)
	if n := len(s.delays); n > 0 {
		idx := call - 1
		if idx >= n {
			idx = n - 1
		}
		delay = s.delays[idx]
	}
	s.mu.Unlock()

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	return s.inner.RequeueClaimed(ctx, jobID, workerID, reason, nextRunAt)
}

func (s *slowRequeueJobStore) MarkDead(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error) error {
	return s.inner.MarkDead(ctx, jobID, workerID, err)
}

func (s *slowRequeueJobStore) requeueCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requeueCalls
}

func (s *renewLeaseSpyJobStore) Create(ctx context.Context, job deltaflow.SyncJob) (*deltaflow.SyncJob, error) {
	return s.inner.Create(ctx, job)
}

func (s *renewLeaseSpyJobStore) Get(ctx context.Context, jobID deltaflow.SyncJobID) (*deltaflow.SyncJob, bool, error) {
	return s.inner.Get(ctx, jobID)
}

func (s *renewLeaseSpyJobStore) ClaimNext(ctx context.Context, syncID deltaflow.SyncID, workerID string, lockFor time.Duration) (*deltaflow.SyncJob, error) {
	return s.inner.ClaimNext(ctx, syncID, workerID, lockFor)
}

func (s *renewLeaseSpyJobStore) RenewLease(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, lockFor time.Duration) error {
	s.mu.Lock()
	s.renewedCount++
	s.mu.Unlock()
	s.renewedOnce.Do(func() { close(s.renewedCh) })
	if s.renewErr != nil {
		return s.renewErr
	}
	return s.inner.RenewLease(ctx, jobID, workerID, lockFor)
}

func (s *renewLeaseSpyJobStore) MarkSynced(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, ghostDetected bool) error {
	return s.inner.MarkSynced(ctx, jobID, workerID, ghostDetected)
}

func (s *renewLeaseSpyJobStore) MarkRetrying(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error, nextRunAt time.Time) error {
	return s.inner.MarkRetrying(ctx, jobID, workerID, err, nextRunAt)
}

func (s *renewLeaseSpyJobStore) RequeueClaimed(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, reason error, nextRunAt time.Time) error {
	return s.inner.RequeueClaimed(ctx, jobID, workerID, reason, nextRunAt)
}

func (s *renewLeaseSpyJobStore) MarkDead(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error) error {
	return s.inner.MarkDead(ctx, jobID, workerID, err)
}

func (s *renewLeaseSpyJobStore) firstRenewed() <-chan struct{} {
	return s.renewedCh
}

func (s *renewLeaseSpyJobStore) renewCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.renewedCount
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

type claimNextOnlyJobStore struct {
	inner           *JobMemoryStore
	mu              sync.Mutex
	claimCalls      int
	claimErrorAfter int
	claimError      error
}

type dispatchSpyStore struct {
	mu    sync.Mutex
	limit int
}

func (s *dispatchSpyStore) DispatchPending(ctx context.Context, syncID deltaflow.SyncID, limit int) ([]*deltaflow.SyncJob, error) {
	s.mu.Lock()
	s.limit = limit
	s.mu.Unlock()
	return nil, nil
}

func (s *dispatchSpyStore) lastLimit() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limit
}

func newClaimNextOnlyJobStore(inner *JobMemoryStore) *claimNextOnlyJobStore {
	return &claimNextOnlyJobStore{inner: inner}
}

func newClaimNextOnlyJobStoreWithClaimErrorAfter(inner *JobMemoryStore, claimErrorAfter int, claimError error) *claimNextOnlyJobStore {
	return &claimNextOnlyJobStore{inner: inner, claimErrorAfter: claimErrorAfter, claimError: claimError}
}

func (s *claimNextOnlyJobStore) Create(ctx context.Context, job deltaflow.SyncJob) (*deltaflow.SyncJob, error) {
	return s.inner.Create(ctx, job)
}

func (s *claimNextOnlyJobStore) Get(ctx context.Context, jobID deltaflow.SyncJobID) (*deltaflow.SyncJob, bool, error) {
	return s.inner.Get(ctx, jobID)
}

func (s *claimNextOnlyJobStore) ClaimNext(ctx context.Context, syncID deltaflow.SyncID, workerID string, lockFor time.Duration) (*deltaflow.SyncJob, error) {
	s.mu.Lock()
	s.claimCalls++
	claimCalls := s.claimCalls
	claimErrorAfter := s.claimErrorAfter
	claimError := s.claimError
	s.mu.Unlock()
	if claimError != nil && claimErrorAfter > 0 && claimCalls >= claimErrorAfter {
		return nil, claimError
	}
	return s.inner.ClaimNext(ctx, syncID, workerID, lockFor)
}

func (s *claimNextOnlyJobStore) RenewLease(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, lockFor time.Duration) error {
	return s.inner.RenewLease(ctx, jobID, workerID, lockFor)
}

func (s *claimNextOnlyJobStore) MarkSynced(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, ghostDetected bool) error {
	return s.inner.MarkSynced(ctx, jobID, workerID, ghostDetected)
}

func (s *claimNextOnlyJobStore) MarkRetrying(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error, nextRunAt time.Time) error {
	return s.inner.MarkRetrying(ctx, jobID, workerID, err, nextRunAt)
}

func (s *claimNextOnlyJobStore) RequeueClaimed(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, reason error, nextRunAt time.Time) error {
	return s.inner.RequeueClaimed(ctx, jobID, workerID, reason, nextRunAt)
}

func (s *claimNextOnlyJobStore) MarkDead(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error) error {
	return s.inner.MarkDead(ctx, jobID, workerID, err)
}

func (s *claimNextOnlyJobStore) claimCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claimCalls
}
