package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type JobStoreConfig = connectors.JobStoreBaseConfig

type JobStore struct {
	connectors.JobStoreBase
}

const syncJobReturningColumns = `
RETURNING
	id::text,
	sync_id,
	delta_id::text,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	attempt_count,
	max_attempts,
	last_error,
	last_error_code,
	available_at,
	locked_by,
	locked_until,
	ghost_detected,
	synced_at,
	dead_at,
	created_at,
	updated_at`

func NewJobStore(db *sql.DB, cfg JobStoreConfig) *JobStore {
	return &JobStore{JobStoreBase: connectors.NewJobStoreBase(db, cfg)}
}

func (s *JobStore) Create(ctx context.Context, job deltaflow.SyncJob) (*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if job.ID != "" {
		return nil, deltaflow.ErrJobIDProvided
	}

	job, err := s.PrepareJobForCreate(job)
	if err != nil {
		return nil, err
	}

	const returning = `
RETURNING
	id::text,
	sync_id,
	delta_id::text,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	attempt_count,
	max_attempts,
	last_error,
	last_error_code,
	available_at,
	locked_by,
	locked_until,
	ghost_detected,
	synced_at,
	dead_at,
	created_at,
	updated_at`

	var deltaID any
	if job.DeltaID != nil {
		deltaID = *job.DeltaID
	}

	row := s.DB.QueryRowContext(ctx, `
INSERT INTO deltaflow.deltaflow_sync_jobs (
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
	available_at,
	locked_by,
	locked_until,
	ghost_detected,
	synced_at,
	dead_at,
	created_at,
	updated_at
)
VALUES (
	$1,
	$2::uuid,
	$3,
	$4,
	$5::jsonb,
	$6,
	$7,
	$8,
	$9,
	$10,
	$11,
	$12,
	$13,
	$14,
	$15,
	$16,
	$17,
	$18,
	$19
)`+returning,
		job.SyncID,
		deltaID,
		job.Origin,
		job.ProjectionType,
		job.ProjectionKey,
		job.ProjectionKeyHash,
		job.State,
		job.AttemptCount,
		job.MaxAttempts,
		job.LastError,
		job.LastErrorCode,
		job.AvailableAt,
		job.LockedBy,
		job.LockedUntil,
		job.GhostDetected,
		job.SyncedAt,
		job.DeadAt,
		job.CreatedAt,
		job.UpdatedAt,
	)

	created, ok, err := s.ScanSyncJob(row)
	if err != nil {
		if isOutboxDeltaMappedViolation(err) {
			return nil, deltaflow.ErrDeltaAlreadyMapped
		}
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return created, nil
}

func (s *JobStore) createOutboxFromDeltaTx(ctx context.Context, tx *sql.Tx, delta dispatchPendingDelta, now time.Time) (*deltaflow.SyncJob, bool, error) {
	row := tx.QueryRowContext(ctx, `
INSERT INTO deltaflow.deltaflow_sync_jobs (
	sync_id,
	delta_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	attempt_count,
	max_attempts,
	available_at,
	created_at,
	updated_at
)
VALUES ($1, $2::uuid, 'outbox', $3, $4::jsonb, $5, 'pending', 0, $6, $7, $7, $7)
ON CONFLICT (delta_id) WHERE (origin = 'outbox') DO NOTHING
RETURNING
	id::text,
	sync_id,
	delta_id::text,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	attempt_count,
	max_attempts,
	last_error,
	last_error_code,
	available_at,
	locked_by,
	locked_until,
	ghost_detected,
	synced_at,
	dead_at,
	created_at,
	updated_at`,
		delta.SyncID,
		delta.ID,
		delta.ProjectionType,
		delta.ProjectionKeyJSON,
		delta.ProjectionKeyHash,
		s.DefaultMaxAttempts(),
		now.UTC(),
	)

	return s.ScanSyncJob(row)
}

func (s *JobStore) Get(ctx context.Context, jobID deltaflow.SyncJobID) (*deltaflow.SyncJob, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	row := s.DB.QueryRowContext(ctx, `
SELECT
	id::text,
	sync_id,
	delta_id::text,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	attempt_count,
	max_attempts,
	last_error,
	last_error_code,
	available_at,
	locked_by,
	locked_until,
	ghost_detected,
	synced_at,
	dead_at,
	created_at,
	updated_at
FROM deltaflow.deltaflow_sync_jobs
WHERE id = $1::uuid`, jobID)

	job, ok, err := s.ScanSyncJob(row)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return job, true, nil
}

func (s *JobStore) ClaimNext(ctx context.Context, syncID deltaflow.SyncID, workerID string, lockFor time.Duration) (*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lockFor <= 0 {
		s.LeaseTelemetry().ObserveLeaseClaim(deltaflow.LeaseTelemetryResultInvalidLockFor)
		s.logLease("lease_claim_rejected",
			"sync_id", syncID,
			"worker_id", workerID,
			"reason", "invalid_lock_for",
		)
		return nil, deltaflow.ErrInvalidLockFor
	}

	now := s.Now()
	lockedUntil := now.Add(lockFor)

	row := s.DB.QueryRowContext(ctx, `
WITH candidate AS (
		SELECT
			id,
			(
				state = 'processing'
				AND (locked_until IS NULL OR locked_until <= $1)
			) AS reclaimed
	FROM deltaflow.deltaflow_sync_jobs
	WHERE
		sync_id = $2
		AND (
			(
				state IN ('pending', 'retrying')
				AND available_at <= $1
			)
			OR (
				state = 'processing'
				AND (locked_until IS NULL OR locked_until <= $1)
			)
		)
	ORDER BY available_at ASC, created_at ASC, id ASC
	LIMIT 1
	FOR UPDATE SKIP LOCKED
)
UPDATE deltaflow.deltaflow_sync_jobs j
SET
	state = 'processing',
	locked_by = $3,
	locked_until = $4,
	updated_at = $1
FROM candidate
WHERE j.id = candidate.id
RETURNING
	j.id::text,
	j.sync_id,
	j.delta_id::text,
	j.origin,
	j.projection_type,
	j.projection_key,
	j.projection_key_hash,
	j.state,
	j.attempt_count,
	j.max_attempts,
	j.last_error,
	j.last_error_code,
	j.available_at,
	j.locked_by,
	j.locked_until,
	j.ghost_detected,
	j.synced_at,
	j.dead_at,
	j.created_at,
	j.updated_at,
	candidate.reclaimed`, now, syncID, workerID, lockedUntil)

	var reclaimed bool
	job, ok, err := s.ScanSyncJobWithExtras(row, &reclaimed)
	if err != nil {
		s.LeaseTelemetry().ObserveLeaseClaim(deltaflow.LeaseTelemetryResultError)
		s.logLease("lease_claim_failed",
			"sync_id", syncID,
			"worker_id", workerID,
			"reason", leaseResult(err),
			"error", err.Error(),
		)
		return nil, err
	}
	if !ok {
		s.LeaseTelemetry().ObserveLeaseClaim(deltaflow.LeaseTelemetryResultEmpty)
		s.logLease("lease_claim_empty",
			"sync_id", syncID,
			"worker_id", workerID,
		)
		return nil, nil
	}
	s.LeaseTelemetry().ObserveLeaseClaim(deltaflow.LeaseTelemetryResultSuccess)
	if reclaimed {
		s.LeaseTelemetry().ObserveLeaseReclaim()
	}
	reason := "ready"
	if reclaimed {
		reason = "expired_reclaimed"
	}
	s.logLease("lease_claimed",
		"sync_id", syncID,
		"job_id", job.ID,
		"worker_id", workerID,
		"state", job.State,
		"attempt_count", job.AttemptCount,
		"locked_until", job.LockedUntil,
		"lease_ms_remaining", int64(lockFor/time.Millisecond),
		"reason", reason,
	)
	return job, nil
}

func (s *JobStore) RenewLease(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, lockFor time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lockFor <= 0 {
		s.LeaseTelemetry().ObserveLeaseRenew(deltaflow.LeaseTelemetryResultInvalidLockFor, 0)
		s.logLease("lease_renew_rejected",
			"job_id", jobID,
			"worker_id", workerID,
			"reason", "invalid_lock_for",
		)
		return deltaflow.ErrInvalidLockFor
	}

	now := s.Now()
	lockedUntil := now.Add(lockFor)
	start := time.Now()
	err := s.update(ctx, jobID, `
UPDATE deltaflow.deltaflow_sync_jobs
SET
	locked_until = $2,
	updated_at = $3
WHERE id = $1::uuid
	AND state = 'processing'
	AND locked_by = $4
	AND locked_until > $3`, lockedUntil, now, workerID)
	result := leaseResult(err)
	s.LeaseTelemetry().ObserveLeaseRenew(result, time.Since(start))
	if errors.Is(err, deltaflow.ErrJobLeaseNotOwned) {
		s.LeaseTelemetry().ObserveLeaseOwnershipCheck(deltaflow.LeaseTelemetryTransitionRenewLease, deltaflow.LeaseTelemetryOwnershipRejected)
	}
	if err != nil {
		s.logLease("lease_renew_failed",
			"job_id", jobID,
			"worker_id", workerID,
			"reason", result,
		)
		return err
	}
	s.LeaseTelemetry().ObserveLeaseOwnershipCheck(deltaflow.LeaseTelemetryTransitionRenewLease, deltaflow.LeaseTelemetryOwnershipOwned)
	s.logLease("lease_renewed",
		"job_id", jobID,
		"worker_id", workerID,
		"locked_until", lockedUntil,
		"lease_ms_remaining", int64(lockFor/time.Millisecond),
	)
	return nil
}

func (s *JobStore) MarkSynced(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, ghostDetected bool) error {
	now := s.Now()
	updated, err := s.updateAndReturn(ctx, jobID, `
UPDATE deltaflow.deltaflow_sync_jobs
SET
	state = 'synced',
	ghost_detected = $2,
	synced_at = $3,
	locked_by = NULL,
	locked_until = NULL,
	updated_at = $3
WHERE id = $1::uuid
	AND state = 'processing'
	AND locked_by = $4
	AND locked_until > $3`+syncJobReturningColumns, ghostDetected, now, workerID)
	s.observeOwnershipResult(deltaflow.LeaseTelemetryTransitionMarkSynced, err)
	if err != nil {
		s.logLeaseTransitionRejected(ctx, deltaflow.LeaseTelemetryTransitionMarkSynced, jobID, workerID, err, now)
		return err
	}
	s.logLeaseTransitionApplied(deltaflow.LeaseTelemetryTransitionMarkSynced, updated, workerID, now)
	return nil
}

func (s *JobStore) MarkRetrying(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error, nextRunAt time.Time) error {
	now := s.Now()
	msg := connectors.ErrorMessage(err)
	updated, updateErr := s.updateAndReturn(ctx, jobID, `
UPDATE deltaflow.deltaflow_sync_jobs
SET
	state = 'retrying',
	attempt_count = attempt_count + 1,
	last_error = $2,
	available_at = $3,
	locked_by = NULL,
	locked_until = NULL,
	updated_at = $4
WHERE id = $1::uuid
	AND state = 'processing'
	AND locked_by = $5
	AND locked_until > $4`+syncJobReturningColumns, msg, nextRunAt.UTC(), now, workerID)
	s.observeOwnershipResult(deltaflow.LeaseTelemetryTransitionMarkRetrying, updateErr)
	if updateErr != nil {
		s.logLeaseTransitionRejected(ctx, deltaflow.LeaseTelemetryTransitionMarkRetrying, jobID, workerID, updateErr, now)
		return updateErr
	}
	s.logLeaseTransitionApplied(deltaflow.LeaseTelemetryTransitionMarkRetrying, updated, workerID, now,
		"reason", msg,
	)
	return nil
}

func (s *JobStore) MarkDead(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error) error {
	now := s.Now()
	msg := connectors.ErrorMessage(err)
	updated, updateErr := s.updateAndReturn(ctx, jobID, `
UPDATE deltaflow.deltaflow_sync_jobs
SET
	state = 'dead',
	attempt_count = attempt_count + 1,
	last_error = $2,
	dead_at = $3,
	locked_by = NULL,
	locked_until = NULL,
	updated_at = $3
WHERE id = $1::uuid
	AND state = 'processing'
	AND locked_by = $4
	AND locked_until > $3`+syncJobReturningColumns, msg, now, workerID)
	s.observeOwnershipResult(deltaflow.LeaseTelemetryTransitionMarkDead, updateErr)
	if updateErr != nil {
		s.logLeaseTransitionRejected(ctx, deltaflow.LeaseTelemetryTransitionMarkDead, jobID, workerID, updateErr, now)
		return updateErr
	}
	s.logLeaseTransitionApplied(deltaflow.LeaseTelemetryTransitionMarkDead, updated, workerID, now,
		"reason", msg,
	)
	return nil
}

func (s *JobStore) updateAndReturn(ctx context.Context, jobID deltaflow.SyncJobID, query string, args ...any) (*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, jobID)
	allArgs = append(allArgs, args...)

	row := s.DB.QueryRowContext(ctx, query, allArgs...)
	job, ok, err := s.ScanSyncJob(row)
	if err != nil {
		return nil, err
	}
	if ok {
		return job, nil
	}

	exists, err := s.jobExists(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, deltaflow.ErrJobNotFound
	}
	return nil, deltaflow.ErrJobLeaseNotOwned
}

func (s *JobStore) update(ctx context.Context, jobID deltaflow.SyncJobID, query string, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	allArgs := make([]any, 0, len(args)+1)
	allArgs = append(allArgs, jobID)
	allArgs = append(allArgs, args...)

	res, err := s.DB.ExecContext(ctx, query, allArgs...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		exists, err := s.jobExists(ctx, jobID)
		if err != nil {
			return err
		}
		if !exists {
			return deltaflow.ErrJobNotFound
		}
		return deltaflow.ErrJobLeaseNotOwned
	}
	return nil
}

func (s *JobStore) jobExists(ctx context.Context, jobID deltaflow.SyncJobID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	row := s.DB.QueryRowContext(ctx, `SELECT 1 FROM deltaflow.deltaflow_sync_jobs WHERE id = $1::uuid`, jobID)
	var exists int
	if err := row.Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isOutboxDeltaMappedViolation(err error) bool {
	if err == nil {
		return false
	}
	if !connectors.IsUniqueViolation(err) {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "deltaflow_sync_jobs_outbox_delta_unique")
}

func (s *JobStore) observeOwnershipResult(transition string, err error) {
	if err == nil {
		s.LeaseTelemetry().ObserveLeaseOwnershipCheck(transition, deltaflow.LeaseTelemetryOwnershipOwned)
		return
	}
	if errors.Is(err, deltaflow.ErrJobLeaseNotOwned) {
		s.LeaseTelemetry().ObserveLeaseOwnershipCheck(transition, deltaflow.LeaseTelemetryOwnershipRejected)
	}
}

func (s *JobStore) logLeaseTransitionApplied(transition string, job *deltaflow.SyncJob, workerID string, now time.Time, extra ...any) {
	if job == nil {
		return
	}
	attrs := []any{
		"transition", transition,
		"sync_id", job.SyncID,
		"job_id", job.ID,
		"worker_id", workerID,
		"state", job.State,
		"attempt_count", job.AttemptCount,
		"locked_until", job.LockedUntil,
		"lease_ms_remaining", leaseMSRemaining(job.LockedUntil, now),
	}
	attrs = append(attrs, extra...)
	s.logLease("lease_transition_applied", attrs...)
}

func (s *JobStore) logLeaseTransitionRejected(ctx context.Context, transition string, jobID deltaflow.SyncJobID, workerID string, updateErr error, now time.Time) {
	attrs := []any{
		"transition", transition,
		"job_id", jobID,
		"worker_id", workerID,
		"reason", leaseResult(updateErr),
	}

	current, ok, getErr := s.Get(ctx, jobID)
	if getErr != nil {
		attrs = append(attrs, "lookup_error", getErr.Error())
	} else if ok {
		attrs = append(attrs,
			"sync_id", current.SyncID,
			"state", current.State,
			"attempt_count", current.AttemptCount,
			"locked_until", current.LockedUntil,
			"lease_ms_remaining", leaseMSRemaining(current.LockedUntil, now),
		)
	}

	s.logLease("lease_transition_rejected", attrs...)
}

func (s *JobStore) logLease(event string, attrs ...any) {
	logger := s.LeaseLogger()
	if logger == nil {
		return
	}
	eventAttrs := make([]any, 0, len(attrs)+2)
	eventAttrs = append(eventAttrs, "event", event)
	eventAttrs = append(eventAttrs, attrs...)
	logger.Info("lease event", eventAttrs...)
}

func leaseResult(err error) string {
	if err == nil {
		return deltaflow.LeaseTelemetryResultSuccess
	}
	if errors.Is(err, deltaflow.ErrJobNotFound) {
		return deltaflow.LeaseTelemetryResultJobNotFound
	}
	if errors.Is(err, deltaflow.ErrJobLeaseNotOwned) {
		return deltaflow.LeaseTelemetryResultLeaseNotOwned
	}
	if errors.Is(err, deltaflow.ErrInvalidLockFor) {
		return deltaflow.LeaseTelemetryResultInvalidLockFor
	}
	return deltaflow.LeaseTelemetryResultError
}

func leaseMSRemaining(lockedUntil *time.Time, now time.Time) int64 {
	if lockedUntil == nil {
		return 0
	}
	remaining := lockedUntil.Sub(now)
	if remaining < 0 {
		return 0
	}
	return int64(remaining / time.Millisecond)
}
