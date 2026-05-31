package postgres

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type JobStoreConfig = connectors.JobStoreBaseConfig

type JobStore struct {
	connectors.JobStoreBase
}

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
		return nil, deltaflow.ErrInvalidLockFor
	}

	now := s.Now()
	lockedUntil := now.Add(lockFor)

	row := s.DB.QueryRowContext(ctx, `
WITH candidate AS (
	SELECT id
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
	j.updated_at`, now, syncID, workerID, lockedUntil)

	job, ok, err := s.ScanSyncJob(row)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return job, nil
}

func (s *JobStore) MarkSynced(ctx context.Context, jobID deltaflow.SyncJobID, ghostDetected bool) error {
	return s.update(ctx, jobID, `
UPDATE deltaflow.deltaflow_sync_jobs
SET
	state = 'synced',
	ghost_detected = $2,
	synced_at = $3,
	locked_by = NULL,
	locked_until = NULL,
	updated_at = $3
WHERE id = $1::uuid`, ghostDetected, s.Now())
}

func (s *JobStore) MarkRetrying(ctx context.Context, jobID deltaflow.SyncJobID, err error, nextRunAt time.Time) error {
	now := s.Now()
	msg := connectors.ErrorMessage(err)
	return s.update(ctx, jobID, `
UPDATE deltaflow.deltaflow_sync_jobs
SET
	state = 'retrying',
	attempt_count = attempt_count + 1,
	last_error = $2,
	available_at = $3,
	locked_by = NULL,
	locked_until = NULL,
	updated_at = $4
WHERE id = $1::uuid`, msg, nextRunAt.UTC(), now)
}

func (s *JobStore) MarkDead(ctx context.Context, jobID deltaflow.SyncJobID, err error) error {
	now := s.Now()
	msg := connectors.ErrorMessage(err)
	return s.update(ctx, jobID, `
UPDATE deltaflow.deltaflow_sync_jobs
SET
	state = 'dead',
	attempt_count = attempt_count + 1,
	last_error = $2,
	dead_at = $3,
	locked_by = NULL,
	locked_until = NULL,
	updated_at = $3
WHERE id = $1::uuid`, msg, now)
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
		return deltaflow.ErrJobNotFound
	}
	return nil
}

func isOutboxDeltaMappedViolation(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "deltaflow_sync_jobs_outbox_delta_unique")
}
