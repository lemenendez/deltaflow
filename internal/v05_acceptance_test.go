package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestV05AcceptanceWorkerUpsertPath(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

	var applied []deltaflow.ProjectionOperation
	worker := deltaflow.SyncWorker{
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
		WorkerID: "v05-worker",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	job := mustGetJobByDelta(t, ctx, jobStore, inserted.ID)
	if job.State != deltaflow.StateSynced {
		t.Fatalf("job state = %s, want %s", job.State, deltaflow.StateSynced)
	}
	if job.GhostDetected {
		t.Fatal("ghost_detected = true, want false")
	}
	if len(applied) != 1 {
		t.Fatalf("applied operations = %d, want 1", len(applied))
	}
	if applied[0].Type != deltaflow.ProjectionOpUpsert {
		t.Fatalf("operation type = %s, want %s", applied[0].Type, deltaflow.ProjectionOpUpsert)
	}
	if applied[0].Projection == nil || string(applied[0].Projection.Payload) != `{"name":"Ada"}` {
		t.Fatalf("applied projection = %#v, want payload", applied[0].Projection)
	}
}

func TestV05AcceptanceWorkerGhostDeletePath(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

	var applied []deltaflow.ProjectionOperation
	worker := deltaflow.SyncWorker{
		JobStore:   jobStore,
		Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
		}),
		Applier:  recordApplier(&applied, nil),
		SyncID:   "sync",
		WorkerID: "v05-worker",
		LockFor:  time.Minute,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	job := mustGetJobByDelta(t, ctx, jobStore, inserted.ID)
	if job.State != deltaflow.StateSynced {
		t.Fatalf("job state = %s, want %s", job.State, deltaflow.StateSynced)
	}
	if !job.GhostDetected {
		t.Fatal("ghost_detected = false, want true")
	}
	if len(applied) != 1 {
		t.Fatalf("applied operations = %d, want 1", len(applied))
	}
	if applied[0].Type != deltaflow.ProjectionOpDelete {
		t.Fatalf("operation type = %s, want %s", applied[0].Type, deltaflow.ProjectionOpDelete)
	}
	if applied[0].Projection != nil {
		t.Fatalf("delete projection = %#v, want nil", applied[0].Projection)
	}
}

func TestV05AcceptanceWorkerFailedApplyRetryAndDeadBehavior(t *testing.T) {
	ctx := context.Background()
	applyErr := errors.New("apply failed")

	t.Run("retry before max attempts", func(t *testing.T) {
		deltaStore := NewDeltaMemoryStore()
		jobStore := NewJobMemoryStore()
		inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})

		worker := deltaflow.SyncWorker{
			JobStore:   jobStore,
			Dispatcher: NewMemoryDispatchStore(deltaStore, jobStore, nil),
			Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
				return deltaflow.Projection{Identity: identity}, nil
			}),
			Applier:  recordApplier(nil, applyErr),
			SyncID:   "sync",
			WorkerID: "v05-worker",
			LockFor:  time.Minute,
		}

		if err := worker.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce returned error: %v", err)
		}

		job := mustGetJobByDelta(t, ctx, jobStore, inserted.ID)
		if job.State != deltaflow.StateRetrying {
			t.Fatalf("job state = %s, want %s", job.State, deltaflow.StateRetrying)
		}
		if job.AttemptCount != 1 {
			t.Fatalf("attempt_count = %d, want 1", job.AttemptCount)
		}
		if job.LastError == nil || *job.LastError != applyErr.Error() {
			t.Fatalf("last_error = %v, want %q", job.LastError, applyErr.Error())
		}
	})

	t.Run("dead at max attempts", func(t *testing.T) {
		deltaStore := NewDeltaMemoryStore()
		jobStore := NewJobMemoryStore()
		inserted := enqueueTestDelta(t, ctx, deltaStore, deltaflow.Delta{})
		seedJobForDelta(t, ctx, jobStore, inserted.ID, 2, 3)
		if err := deltaStore.MarkDispatched(ctx, inserted.ID); err != nil {
			t.Fatalf("MarkDispatched returned error: %v", err)
		}

		worker := deltaflow.SyncWorker{
			JobStore: jobStore,
			Projector: deltaflow.ProjectorFunc(func(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
				return deltaflow.Projection{Identity: identity}, nil
			}),
			Applier:  recordApplier(nil, applyErr),
			SyncID:   "sync",
			WorkerID: "v05-worker",
			LockFor:  time.Minute,
		}

		if err := worker.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce returned error: %v", err)
		}

		job := mustGetJobByDelta(t, ctx, jobStore, inserted.ID)
		if job.State != deltaflow.StateDead {
			t.Fatalf("job state = %s, want %s", job.State, deltaflow.StateDead)
		}
		if job.AttemptCount != 3 {
			t.Fatalf("attempt_count = %d, want 3", job.AttemptCount)
		}
		if job.DeadAt == nil {
			t.Fatal("dead_at is nil")
		}
	})
}
