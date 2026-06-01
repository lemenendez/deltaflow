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

type renewLeaseSpyJobStore struct {
	inner        deltaflow.JobStore
	mu           sync.Mutex
	renewedCount int
	renewErr     error
	renewedCh    chan struct{}
	renewedOnce  sync.Once
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
