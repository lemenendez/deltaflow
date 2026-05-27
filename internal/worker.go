package internal

import (
	"context"
	"errors"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type SyncWorker struct {
	Store     deltaflow.DeltaStore
	Projector deltaflow.Projector
	Applier   deltaflow.ProjectionApplier

	WorkerID string
	LockFor  time.Duration
}

func (w *SyncWorker) RunOnce(ctx context.Context) error {
	delta, err := w.Store.ClaimNext(ctx, w.WorkerID, w.LockFor)
	if err != nil {
		return err
	}

	if delta == nil {
		return nil
	}

	identity := deltaflow.ProjectionIdentity{
		Type: delta.ProjectionType,
		Key:  delta.ProjectionKey,
	}

	projection, err := w.Projector.Project(ctx, identity)

	if errors.Is(err, deltaflow.ErrProjectionNotFound) {
		op := deltaflow.ProjectionOperation{
			Type:     deltaflow.ProjectionOpDelete,
			Identity: identity,
		}

		if applyErr := w.Applier.Apply(ctx, op); applyErr != nil {
			return w.failOrRetry(ctx, delta, applyErr)
		}

		return w.Store.MarkSynced(ctx, delta.ID, true)
	}

	if err != nil {
		return w.failOrRetry(ctx, delta, err)
	}

	op := deltaflow.ProjectionOperation{
		Type:       deltaflow.ProjectionOpUpsert,
		Identity:   identity,
		Projection: &projection,
	}

	if err := w.Applier.Apply(ctx, op); err != nil {
		return w.failOrRetry(ctx, delta, err)
	}

	return w.Store.MarkSynced(ctx, delta.ID, false)
}

func (w *SyncWorker) failOrRetry(ctx context.Context, delta *deltaflow.Delta, err error) error {
	nextAttempt := delta.AttemptCount + 1

	if nextAttempt >= delta.MaxAttempts {
		return w.Store.MarkDead(ctx, delta.ID, err)
	}

	nextRunAt := time.Now().UTC().Add(backoff(nextAttempt))
	return w.Store.MarkRetrying(ctx, delta.ID, err, nextRunAt)
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
