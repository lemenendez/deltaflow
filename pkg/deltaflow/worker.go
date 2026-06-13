package deltaflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// SyncWorkerConfig contains the required runtime identity for a SyncWorker.
type SyncWorkerConfig struct {
	SyncID   SyncID
	WorkerID string
	LockFor  time.Duration
	PullSize int
}

func (c SyncWorkerConfig) Validate() error {
	if c.SyncID == "" {
		return errors.New("sync worker config: sync_id is required")
	}
	if c.WorkerID == "" {
		return errors.New("sync worker config: worker_id is required")
	}
	if c.LockFor <= 0 {
		return errors.New("sync worker config: lock_for must be positive")
	}

	return nil
}

// SyncWorker dispatches pending deltas, claims one sync job, projects the
// latest state, applies the resulting operation, and records the outcome.
type SyncWorker struct {
	JobStore   JobStore
	Dispatcher DispatchStore
	Projector  Projector
	Applier    ProjectionApplier
	Logger     *slog.Logger

	SyncID   SyncID
	WorkerID string
	LockFor  time.Duration
	PullSize int
}

// RunOnce processes at most one claimed job.
func (w *SyncWorker) RunOnce(ctx context.Context) error {
	if err := w.validateRunOnceDependencies(); err != nil {
		return err
	}

	if err := w.dispatchDeltas(ctx); err != nil {
		return err
	}

	job, err := w.JobStore.ClaimNext(ctx, w.SyncID, w.WorkerID, w.LockFor)
	if err != nil {
		return err
	}

	if job == nil {
		w.logLease("worker_claim_empty",
			"sync_id", w.SyncID,
			"worker_id", w.WorkerID,
		)
		return nil
	}
	w.logLease("worker_claimed",
		"sync_id", w.SyncID,
		"job_id", job.ID,
		"worker_id", w.WorkerID,
		"state", job.State,
		"attempt_count", job.AttemptCount,
		"locked_until", job.LockedUntil,
		"lease_ms_remaining", workerLeaseMSRemaining(job.LockedUntil, time.Now().UTC()),
	)

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatErrCh := make(chan error, 1)
	go w.heartbeatLease(jobCtx, cancel, job.ID, heartbeatErrCh)

	identity := ProjectionIdentity{
		Type: job.ProjectionType,
		Key:  job.ProjectionKey,
	}

	projection, err := w.Projector.Project(jobCtx, identity)

	if errors.Is(err, ErrProjectionNotFound) {
		op := ProjectionOperation{
			Type:     ProjectionOpDelete,
			Identity: identity,
		}

		if applyErr := w.Applier.Apply(jobCtx, op); applyErr != nil {
			if heartbeatErr := w.tryHeartbeatError(heartbeatErrCh); heartbeatErr != nil {
				return w.failOrRetry(ctx, job, heartbeatErr)
			}
			return w.failOrRetry(ctx, job, applyErr)
		}
		if heartbeatErr := w.tryHeartbeatError(heartbeatErrCh); heartbeatErr != nil {
			return w.failOrRetry(ctx, job, heartbeatErr)
		}

		return w.JobStore.MarkSynced(ctx, job.ID, w.WorkerID, true)
	}

	if err != nil {
		if heartbeatErr := w.tryHeartbeatError(heartbeatErrCh); heartbeatErr != nil {
			return w.failOrRetry(ctx, job, heartbeatErr)
		}
		return w.failOrRetry(ctx, job, err)
	}

	op := ProjectionOperation{
		Type:       ProjectionOpUpsert,
		Identity:   identity,
		Projection: &projection,
	}

	if err := w.Applier.Apply(jobCtx, op); err != nil {
		if heartbeatErr := w.tryHeartbeatError(heartbeatErrCh); heartbeatErr != nil {
			return w.failOrRetry(ctx, job, heartbeatErr)
		}
		return w.failOrRetry(ctx, job, err)
	}
	if heartbeatErr := w.tryHeartbeatError(heartbeatErrCh); heartbeatErr != nil {
		return w.failOrRetry(ctx, job, heartbeatErr)
	}

	return w.JobStore.MarkSynced(ctx, job.ID, w.WorkerID, false)
}

func (w *SyncWorker) validateRunOnceDependencies() error {
	if err := w.config().Validate(); err != nil {
		return err
	}
	if w.JobStore == nil {
		return errors.New("sync worker config: job_store is required")
	}
	if w.Projector == nil {
		return errors.New("sync worker config: projector is required")
	}
	if w.Applier == nil {
		return errors.New("sync worker config: applier is required")
	}

	return nil
}

func (w *SyncWorker) dispatchDeltas(ctx context.Context) error {
	if err := w.config().Validate(); err != nil {
		return err
	}

	pullSize := w.PullSize
	if pullSize <= 0 {
		pullSize = 1
	}
	if w.Dispatcher == nil {
		return nil
	}
	_, err := w.Dispatcher.DispatchPending(ctx, w.SyncID, pullSize)
	return err
}

func (w *SyncWorker) config() SyncWorkerConfig {
	return SyncWorkerConfig{
		SyncID:   w.SyncID,
		WorkerID: w.WorkerID,
		LockFor:  w.LockFor,
		PullSize: w.PullSize,
	}
}

func (w *SyncWorker) failOrRetry(ctx context.Context, job *SyncJob, err error) error {
	nextAttempt := job.AttemptCount + 1

	if nextAttempt >= job.MaxAttempts {
		return w.JobStore.MarkDead(ctx, job.ID, w.WorkerID, err)
	}

	nextRunAt := time.Now().UTC().Add(workerBackoff(nextAttempt))
	return w.JobStore.MarkRetrying(ctx, job.ID, w.WorkerID, err, nextRunAt)
}

func (w *SyncWorker) heartbeatLease(ctx context.Context, cancel context.CancelFunc, jobID SyncJobID, errCh chan<- error) {
	interval := w.LockFor / 2
	if interval <= 0 {
		interval = w.LockFor
	}
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logLease("worker_heartbeat_stopped",
				"sync_id", w.SyncID,
				"job_id", jobID,
				"worker_id", w.WorkerID,
			)
			return
		case <-ticker.C:
			if err := w.JobStore.RenewLease(ctx, jobID, w.WorkerID, w.LockFor); err != nil {
				w.logLease("worker_heartbeat_renew_failed",
					"sync_id", w.SyncID,
					"job_id", jobID,
					"worker_id", w.WorkerID,
					"reason", workerLeaseResult(err),
					"error", err.Error(),
				)
				select {
				case errCh <- fmt.Errorf("lease renewal failed: %w", err):
				default:
				}
				cancel()
				return
			}
		}
	}
}

func (w *SyncWorker) tryHeartbeatError(errCh <-chan error) error {
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func workerBackoff(attempt int) time.Duration {
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

func (w *SyncWorker) logLease(event string, attrs ...any) {
	if w.Logger == nil {
		return
	}
	eventAttrs := make([]any, 0, len(attrs)+2)
	eventAttrs = append(eventAttrs, "event", event)
	eventAttrs = append(eventAttrs, attrs...)
	w.Logger.Info("lease event", eventAttrs...)
}

func workerLeaseMSRemaining(lockedUntil *time.Time, now time.Time) int64 {
	if lockedUntil == nil {
		return 0
	}
	remaining := lockedUntil.Sub(now)
	if remaining < 0 {
		return 0
	}
	return int64(remaining / time.Millisecond)
}

func workerLeaseResult(err error) string {
	if err == nil {
		return LeaseTelemetryResultSuccess
	}
	if errors.Is(err, ErrJobNotFound) {
		return LeaseTelemetryResultJobNotFound
	}
	if errors.Is(err, ErrJobLeaseNotOwned) {
		return LeaseTelemetryResultLeaseNotOwned
	}
	if errors.Is(err, ErrInvalidLockFor) {
		return LeaseTelemetryResultInvalidLockFor
	}
	return LeaseTelemetryResultError
}
