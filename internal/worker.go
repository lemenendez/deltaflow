package internal

import (
	"context"
	"errors"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type SyncWorker struct {
	JobStore   deltaflow.JobStore
	Dispatcher deltaflow.DispatchStore
	Projector  deltaflow.Projector
	Applier    deltaflow.ProjectionApplier

	WorkerID string
	LockFor  time.Duration
	PullSize int
}

func (w *SyncWorker) RunOnce(ctx context.Context) error {
	if err := w.dispatchDeltas(ctx); err != nil {
		return err
	}

	job, err := w.JobStore.ClaimNext(ctx, w.WorkerID, w.LockFor)
	if err != nil {
		return err
	}

	if job == nil {
		return nil
	}

	identity := deltaflow.ProjectionIdentity{
		Type: job.ProjectionType,
		Key:  job.ProjectionKey,
	}

	projection, err := w.Projector.Project(ctx, identity)

	if errors.Is(err, deltaflow.ErrProjectionNotFound) {
		op := deltaflow.ProjectionOperation{
			Type:     deltaflow.ProjectionOpDelete,
			Identity: identity,
		}

		if applyErr := w.Applier.Apply(ctx, op); applyErr != nil {
			return w.failOrRetry(ctx, job, applyErr)
		}

		return w.JobStore.MarkSynced(ctx, job.ID, true)
	}

	if err != nil {
		return w.failOrRetry(ctx, job, err)
	}

	op := deltaflow.ProjectionOperation{
		Type:       deltaflow.ProjectionOpUpsert,
		Identity:   identity,
		Projection: &projection,
	}

	if err := w.Applier.Apply(ctx, op); err != nil {
		return w.failOrRetry(ctx, job, err)
	}

	return w.JobStore.MarkSynced(ctx, job.ID, false)
}

func (w *SyncWorker) dispatchDeltas(ctx context.Context) error {
	pullSize := w.PullSize
	if pullSize <= 0 {
		pullSize = 1
	}
	if w.Dispatcher == nil {
		return nil
	}
	_, err := w.Dispatcher.DispatchPending(ctx, pullSize)
	return err
}

func (w *SyncWorker) failOrRetry(ctx context.Context, job *deltaflow.SyncJob, err error) error {
	nextAttempt := job.AttemptCount + 1

	if nextAttempt >= job.MaxAttempts {
		return w.JobStore.MarkDead(ctx, job.ID, err)
	}

	nextRunAt := time.Now().UTC().Add(backoff(nextAttempt))
	return w.JobStore.MarkRetrying(ctx, job.ID, err, nextRunAt)
}

func backoff(attempt int) time.Duration {
	switch {
	case attempt <= 1:
		return 5 * time.Second
	case attempt == 2:
		return 30 * time.Second
	case attempt == 3:
		return 2 * time.Minute
	default:
		return 5 * time.Minute
	}
}
