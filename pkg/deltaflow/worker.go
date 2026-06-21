package deltaflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const workerFinalizeTimeout = 2 * time.Second

// SyncWorkerConfig contains the required runtime identity for a SyncWorker.
type SyncWorkerConfig struct {
	SyncID      SyncID
	WorkerID    string
	LockFor     time.Duration
	PullSize    int
	BatchSize   int
	Concurrency int
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

// SyncWorker dispatches pending deltas, claims jobs for one worker cycle,
// projects latest state, applies resulting operations, and records outcomes.
//
// Concurrency contract:
// When Concurrency > 1, RunOnce processes jobs from multiple goroutines.
// Projector.Project and ProjectionApplier.Apply can therefore be invoked
// concurrently and must be safe for concurrent use (or wrapped externally).
type SyncWorker struct {
	JobStore   JobStore
	Dispatcher DispatchStore
	Projector  Projector
	Applier    ProjectionApplier
	Logger     *slog.Logger

	SyncID      SyncID
	WorkerID    string
	LockFor     time.Duration
	PullSize    int
	BatchSize   int
	Concurrency int
}

// RunOnce dispatches pending deltas and processes one worker cycle.
// Each cycle can run with configurable concurrency and per-goroutine batch claims.
// If Concurrency > 1, multiple goroutines may call Projector.Project and
// ProjectionApplier.Apply at the same time.
func (w *SyncWorker) RunOnce(ctx context.Context) error {
	if err := w.validateRunOnceDependencies(); err != nil {
		return err
	}

	if err := w.dispatchDeltas(ctx); err != nil {
		return err
	}

	concurrency := w.effectiveConcurrency()
	batchSize := w.effectiveBatchSize()

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var firstErrOnce sync.Once
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		workerID := w.WorkerIDForRoutine(i)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := w.processBatch(workerCtx, id, batchSize); err != nil {
				firstErrOnce.Do(func() {
					firstErr = err
					cancel()
				})
			}
		}(workerID)
	}

	wg.Wait()
	return firstErr
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

	pullSize := w.effectivePullSize()
	if w.Dispatcher == nil {
		return nil
	}
	_, err := w.Dispatcher.DispatchPending(ctx, w.SyncID, pullSize)
	return err
}

func (w *SyncWorker) config() SyncWorkerConfig {
	return SyncWorkerConfig{
		SyncID:      w.SyncID,
		WorkerID:    w.WorkerID,
		LockFor:     w.LockFor,
		PullSize:    w.PullSize,
		BatchSize:   w.BatchSize,
		Concurrency: w.Concurrency,
	}
}

func (w *SyncWorker) processBatch(ctx context.Context, workerID string, batchSize int) error {
	jobs, err := w.claimBatch(ctx, workerID, batchSize)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}

	for i, job := range jobs {
		if err := ctx.Err(); err != nil {
			w.requeueClaimedJobs(ctx, workerID, jobs[i:], err)
			return err
		}
		if err := w.processJob(ctx, workerID, job); err != nil {
			w.requeueClaimedJobs(ctx, workerID, jobs[i+1:], err)
			return err
		}
	}

	return nil
}

func (w *SyncWorker) processJob(ctx context.Context, workerID string, job *SyncJob) error {
	w.logLease("worker_claimed",
		"sync_id", w.SyncID,
		"job_id", job.ID,
		"worker_id", workerID,
		"state", job.State,
		"attempt_count", job.AttemptCount,
		"locked_until", job.LockedUntil,
		"lease_ms_remaining", workerLeaseMSRemaining(job.LockedUntil, time.Now().UTC()),
	)

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatErrCh := make(chan error, 1)
	go w.heartbeatLeaseWithWorker(jobCtx, cancel, job.ID, workerID, heartbeatErrCh)

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
				return w.failOrRetryWithWorker(ctx, job, workerID, heartbeatErr)
			}
			return w.failOrRetryWithWorker(ctx, job, workerID, applyErr)
		}
		if heartbeatErr := w.tryHeartbeatError(heartbeatErrCh); heartbeatErr != nil {
			return w.failOrRetryWithWorker(ctx, job, workerID, heartbeatErr)
		}

		return w.markSynced(ctx, job.ID, workerID, true)
	}

	if err != nil {
		if heartbeatErr := w.tryHeartbeatError(heartbeatErrCh); heartbeatErr != nil {
			return w.failOrRetryWithWorker(ctx, job, workerID, heartbeatErr)
		}
		return w.failOrRetryWithWorker(ctx, job, workerID, err)
	}

	op := ProjectionOperation{
		Type:       ProjectionOpUpsert,
		Identity:   identity,
		Projection: &projection,
	}

	if err := w.Applier.Apply(jobCtx, op); err != nil {
		if heartbeatErr := w.tryHeartbeatError(heartbeatErrCh); heartbeatErr != nil {
			return w.failOrRetryWithWorker(ctx, job, workerID, heartbeatErr)
		}
		return w.failOrRetryWithWorker(ctx, job, workerID, err)
	}
	if heartbeatErr := w.tryHeartbeatError(heartbeatErrCh); heartbeatErr != nil {
		return w.failOrRetryWithWorker(ctx, job, workerID, heartbeatErr)
	}

	return w.markSynced(ctx, job.ID, workerID, false)
}

func (w *SyncWorker) claimBatch(ctx context.Context, workerID string, batchSize int) ([]*SyncJob, error) {
	if claimant, ok := w.JobStore.(JobStoreBatchClaims); ok {
		return claimant.ClaimNextBatch(ctx, w.SyncID, workerID, batchSize, w.LockFor)
	}

	jobs := make([]*SyncJob, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		job, err := w.JobStore.ClaimNext(ctx, w.SyncID, workerID, w.LockFor)
		if err != nil {
			w.requeueClaimedJobs(ctx, workerID, jobs, err)
			return nil, err
		}
		if job == nil {
			break
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (w *SyncWorker) failOrRetry(ctx context.Context, job *SyncJob, err error) error {
	return w.failOrRetryWithWorker(ctx, job, w.WorkerID, err)
}

func (w *SyncWorker) failOrRetryWithWorker(ctx context.Context, job *SyncJob, workerID string, err error) error {
	nextAttempt := job.AttemptCount + 1
	finalizeCtx, cancel := w.finalizeContext(ctx)
	defer cancel()

	if nextAttempt >= job.MaxAttempts {
		return w.JobStore.MarkDead(finalizeCtx, job.ID, workerID, err)
	}

	nextRunAt := time.Now().UTC().Add(workerBackoff(nextAttempt))
	return w.JobStore.MarkRetrying(finalizeCtx, job.ID, workerID, err, nextRunAt)
}

func (w *SyncWorker) markSynced(ctx context.Context, jobID SyncJobID, workerID string, ghostDetected bool) error {
	finalizeCtx, cancel := w.finalizeContext(ctx)
	defer cancel()
	return w.JobStore.MarkSynced(finalizeCtx, jobID, workerID, ghostDetected)
}

func (w *SyncWorker) requeueClaimedJobs(ctx context.Context, workerID string, jobs []*SyncJob, reason error) {
	if len(jobs) == 0 {
		return
	}
	nextRunAt := time.Now().UTC()
	for _, job := range jobs {
		finalizeCtx, cancel := w.finalizeContext(ctx)
		if err := w.JobStore.RequeueClaimed(finalizeCtx, job.ID, workerID, reason, nextRunAt); err != nil {
			w.logLease("worker_requeue_claimed_failed",
				"sync_id", w.SyncID,
				"job_id", job.ID,
				"worker_id", workerID,
				"error", err.Error(),
			)
		}
		cancel()
	}
}

func (w *SyncWorker) finalizeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), workerFinalizeTimeout)
}

func (w *SyncWorker) heartbeatLease(ctx context.Context, cancel context.CancelFunc, jobID SyncJobID, errCh chan<- error) {
	w.heartbeatLeaseWithWorker(ctx, cancel, jobID, w.WorkerID, errCh)
}

func (w *SyncWorker) heartbeatLeaseWithWorker(ctx context.Context, cancel context.CancelFunc, jobID SyncJobID, workerID string, errCh chan<- error) {
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
				"worker_id", workerID,
			)
			return
		case <-ticker.C:
			if err := w.JobStore.RenewLease(ctx, jobID, workerID, w.LockFor); err != nil {
				w.logLease("worker_heartbeat_renew_failed",
					"sync_id", w.SyncID,
					"job_id", jobID,
					"worker_id", workerID,
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

func (w *SyncWorker) effectiveBatchSize() int {
	if w.BatchSize <= 0 {
		return 1
	}
	return w.BatchSize
}

func (w *SyncWorker) effectiveConcurrency() int {
	if w.Concurrency <= 0 {
		return 1
	}
	return w.Concurrency
}

func (w *SyncWorker) effectivePullSize() int {
	if w.PullSize > 0 {
		return w.PullSize
	}
	return w.effectiveConcurrency() * w.effectiveBatchSize()
}

func (w *SyncWorker) WorkerIDForRoutine(routine int) string {
	if routine <= 0 {
		return w.WorkerID
	}
	return fmt.Sprintf("%s-r%d", w.WorkerID, routine)
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
