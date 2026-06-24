package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
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

	normalized, err := s.PrepareJobForCreate(job)
	if err != nil {
		return nil, err
	}
	id, err := newID("job", normalized.CreatedAt)
	if err != nil {
		return nil, err
	}
	projectionKeyJSON, err := json.Marshal(normalized.ProjectionKey)
	if err != nil {
		return nil, err
	}

	var deltaID any
	if normalized.DeltaID != nil {
		deltaID = string(*normalized.DeltaID)
	}

	_, err = s.DB.ExecContext(ctx, `
INSERT INTO deltaflow_sync_jobs (
	id,
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
	available_at_micros,
	locked_by,
	locked_until_micros,
	ghost_detected,
	synced_at_micros,
	dead_at_micros,
	created_at_micros,
	updated_at_micros
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		normalized.SyncID,
		deltaID,
		normalized.Origin,
		normalized.ProjectionType,
		string(projectionKeyJSON),
		normalized.ProjectionKeyHash,
		normalized.State,
		normalized.AttemptCount,
		normalized.MaxAttempts,
		normalized.LastError,
		normalized.LastErrorCode,
		microsFromTime(normalized.AvailableAt),
		normalized.LockedBy,
		timePtrMicros(normalized.LockedUntil),
		boolToInt(normalized.GhostDetected),
		timePtrMicros(normalized.SyncedAt),
		timePtrMicros(normalized.DeadAt),
		microsFromTime(normalized.CreatedAt),
		microsFromTime(normalized.UpdatedAt),
	)
	if err != nil {
		if isOutboxDeltaMappedViolation(err) {
			return nil, deltaflow.ErrDeltaAlreadyMapped
		}
		return nil, err
	}

	created, ok, err := s.Get(ctx, deltaflow.SyncJobID(id))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return created, nil
}

func (s *JobStore) createOutboxFromDeltaTx(ctx context.Context, tx *sql.Tx, delta dispatchPendingDelta, now time.Time) (*deltaflow.SyncJob, bool, error) {
	id, err := newID("job", now)
	if err != nil {
		return nil, false, err
	}
	nowMicros := microsFromTime(now)

	res, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO deltaflow_sync_jobs (
	id,
	sync_id,
	delta_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	attempt_count,
	max_attempts,
	available_at_micros,
	created_at_micros,
	updated_at_micros,
	ghost_detected
)
VALUES (?, ?, ?, 'outbox', ?, ?, ?, 'pending', 0, ?, ?, ?, ?, 0)`,
		id,
		delta.SyncID,
		delta.ID,
		delta.ProjectionType,
		string(delta.ProjectionKeyJSON),
		delta.ProjectionKeyHash,
		s.DefaultMaxAttempts(),
		nowMicros,
		nowMicros,
		nowMicros,
	)
	if err != nil {
		return nil, false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if affected == 0 {
		return nil, false, nil
	}

	row := tx.QueryRowContext(ctx, syncJobSelectByID, id)
	job, ok, err := scanSyncJob(row)
	if err != nil {
		return nil, false, err
	}
	return job, ok, nil
}

func (s *JobStore) Get(ctx context.Context, jobID deltaflow.SyncJobID) (*deltaflow.SyncJob, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	row := s.DB.QueryRowContext(ctx, syncJobSelectByID, jobID)
	return scanSyncJob(row)
}

func (s *JobStore) ClaimNext(ctx context.Context, syncID deltaflow.SyncID, workerID string, lockFor time.Duration) (*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lockFor <= 0 {
		return nil, deltaflow.ErrInvalidLockFor
	}

	now := s.Now()
	nowMicros := microsFromTime(now)
	lockedUntilMicros := microsFromTime(now.Add(lockFor))

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var candidateID string
	err = tx.QueryRowContext(ctx, `
SELECT id
FROM deltaflow_sync_jobs
WHERE sync_id = ?
	AND (
		(state IN ('pending', 'retrying') AND available_at_micros <= ?)
		OR (state = 'processing' AND (locked_until_micros IS NULL OR locked_until_micros <= ?))
	)
ORDER BY available_at_micros ASC, created_at_micros ASC, id ASC
LIMIT 1`, syncID, nowMicros, nowMicros).Scan(&candidateID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if commitErr := tx.Commit(); commitErr != nil {
				return nil, commitErr
			}
			return nil, nil
		}
		return nil, err
	}

	claimed, err := s.claimCandidateTx(ctx, tx, syncID, candidateID, workerID, nowMicros, lockedUntilMicros)
	if err != nil {
		return nil, err
	}
	if !claimed {
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, commitErr
		}
		return nil, nil
	}

	row := tx.QueryRowContext(ctx, syncJobSelectByID, candidateID)
	job, ok, err := scanSyncJob(row)
	if err != nil {
		return nil, err
	}
	if !ok {
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, commitErr
		}
		return nil, nil
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *JobStore) ClaimNextBatch(ctx context.Context, syncID deltaflow.SyncID, workerID string, limit int, lockFor time.Duration) ([]*deltaflow.SyncJob, error) {
	if limit <= 0 {
		return nil, nil
	}
	jobs := make([]*deltaflow.SyncJob, 0, limit)
	for i := 0; i < limit; i++ {
		job, err := s.ClaimNext(ctx, syncID, workerID, lockFor)
		if err != nil {
			s.requeueClaimedBatch(context.WithoutCancel(ctx), workerID, jobs, err)
			return nil, err
		}
		if job == nil {
			break
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (s *JobStore) requeueClaimedBatch(ctx context.Context, workerID string, jobs []*deltaflow.SyncJob, reason error) {
	if len(jobs) == 0 {
		return
	}
	nextRunAt := s.Now()
	for _, job := range jobs {
		_ = s.RequeueClaimed(ctx, job.ID, workerID, reason, nextRunAt)
	}
}

func (s *JobStore) claimCandidateTx(ctx context.Context, tx *sql.Tx, syncID deltaflow.SyncID, candidateID, workerID string, nowMicros, lockedUntilMicros int64) (bool, error) {
	res, err := tx.ExecContext(ctx, `
UPDATE deltaflow_sync_jobs
SET
	state = 'processing',
	locked_by = ?,
	locked_until_micros = ?,
	updated_at_micros = ?
WHERE id = ?
	AND sync_id = ?
	AND (
		(state IN ('pending', 'retrying') AND available_at_micros <= ?)
		OR (state = 'processing' AND (locked_until_micros IS NULL OR locked_until_micros <= ?))
	)`, workerID, lockedUntilMicros, nowMicros, candidateID, syncID, nowMicros, nowMicros)
	return rowsAffected(res, err)
}

func rowsAffected(res sql.Result, execErr error) (bool, error) {
	if execErr != nil {
		return false, execErr
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *JobStore) RenewLease(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, lockFor time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lockFor <= 0 {
		return deltaflow.ErrInvalidLockFor
	}

	now := s.Now()
	err := s.updateOwned(ctx, jobID, `
UPDATE deltaflow_sync_jobs
SET
	locked_until_micros = ?,
	updated_at_micros = ?
WHERE state = 'processing'
	AND locked_by = ?
	AND locked_until_micros > ?
	AND id = ?`, microsFromTime(now.Add(lockFor)), microsFromTime(now), workerID, microsFromTime(now), jobID)
	return err
}

func (s *JobStore) MarkSynced(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, ghostDetected bool) error {
	now := s.Now()
	return s.updateOwned(ctx, jobID, `
UPDATE deltaflow_sync_jobs
SET
	state = 'synced',
	ghost_detected = ?,
	synced_at_micros = ?,
	locked_by = NULL,
	locked_until_micros = NULL,
	updated_at_micros = ?
WHERE state = 'processing'
	AND locked_by = ?
	AND locked_until_micros > ?
	AND id = ?`, boolToInt(ghostDetected), microsFromTime(now), microsFromTime(now), workerID, microsFromTime(now), jobID)
}

func (s *JobStore) MarkRetrying(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error, nextRunAt time.Time) error {
	now := s.Now()
	return s.updateOwned(ctx, jobID, `
UPDATE deltaflow_sync_jobs
SET
	state = 'retrying',
	attempt_count = attempt_count + 1,
	last_error = ?,
	available_at_micros = ?,
	locked_by = NULL,
	locked_until_micros = NULL,
	updated_at_micros = ?
WHERE state = 'processing'
	AND locked_by = ?
	AND locked_until_micros > ?
	AND id = ?`, connectors.ErrorMessage(err), microsFromTime(nextRunAt), microsFromTime(now), workerID, microsFromTime(now), jobID)
}

func (s *JobStore) RequeueClaimed(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, reason error, nextRunAt time.Time) error {
	now := s.Now()
	return s.updateOwned(ctx, jobID, `
UPDATE deltaflow_sync_jobs
SET
	state = 'retrying',
	last_error = ?,
	available_at_micros = ?,
	locked_by = NULL,
	locked_until_micros = NULL,
	updated_at_micros = ?
WHERE state = 'processing'
	AND locked_by = ?
	AND locked_until_micros > ?
	AND id = ?`, connectors.ErrorMessage(reason), microsFromTime(nextRunAt), microsFromTime(now), workerID, microsFromTime(now), jobID)
}

func (s *JobStore) MarkDead(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error) error {
	now := s.Now()
	return s.updateOwned(ctx, jobID, `
UPDATE deltaflow_sync_jobs
SET
	state = 'dead',
	attempt_count = attempt_count + 1,
	last_error = ?,
	dead_at_micros = ?,
	locked_by = NULL,
	locked_until_micros = NULL,
	updated_at_micros = ?
WHERE state = 'processing'
	AND locked_by = ?
	AND locked_until_micros > ?
	AND id = ?`, connectors.ErrorMessage(err), microsFromTime(now), microsFromTime(now), workerID, microsFromTime(now), jobID)
}

func (s *JobStore) updateOwned(ctx context.Context, jobID deltaflow.SyncJobID, query string, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	res, err := s.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}

	exists, err := s.jobExists(ctx, jobID)
	if err != nil {
		return err
	}
	if !exists {
		return deltaflow.ErrJobNotFound
	}
	return deltaflow.ErrJobLeaseNotOwned
}

func (s *JobStore) ListActiveLeases(ctx context.Context, syncID deltaflow.SyncID, limit int) ([]*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}

	now := microsFromTime(s.Now())
	return s.listLeases(ctx, syncID, now, "locked_until_micros > ?", limit)
}

func (s *JobStore) ListExpiredProcessingLeases(ctx context.Context, syncID deltaflow.SyncID, limit int) ([]*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}

	now := microsFromTime(s.Now())
	return s.listLeases(ctx, syncID, now, "(locked_until_micros IS NULL OR locked_until_micros <= ?)", limit)
}

func (s *JobStore) ListNearExpiryLeases(ctx context.Context, syncID deltaflow.SyncID, within time.Duration, limit int) ([]*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if within < 0 {
		return nil, deltaflow.ErrInvalidLeaseWindow
	}
	if limit <= 0 {
		return nil, nil
	}

	now := s.Now()
	nowMicros := microsFromTime(now)
	threshold := microsFromTime(now.Add(within))

	query := `
SELECT
	id,
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
	available_at_micros,
	locked_by,
	locked_until_micros,
	ghost_detected,
	synced_at_micros,
	dead_at_micros,
	created_at_micros,
	updated_at_micros
FROM deltaflow_sync_jobs
WHERE
	state = 'processing'
	AND locked_until_micros > ?
	AND locked_until_micros <= ?`

	args := []any{nowMicros, threshold}
	if syncID != "" {
		query += `
	AND sync_id = ?`
		args = append(args, syncID)
	}

	query += `
ORDER BY locked_until_micros ASC, updated_at_micros ASC, id ASC
LIMIT ?`
	args = append(args, limit)

	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return s.collectJobs(rows)
}

func (s *JobStore) ForceReleaseExpiredLease(ctx context.Context, jobID deltaflow.SyncJobID, auditReason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reason := strings.TrimSpace(auditReason)
	if reason == "" {
		return deltaflow.ErrAuditReasonRequired
	}

	now := microsFromTime(s.Now())
	msg := "operator_force_release: " + reason
	return s.updateLeaseOperator(ctx, jobID, `
UPDATE deltaflow_sync_jobs
SET
	last_error = ?,
	locked_by = NULL,
	locked_until_micros = NULL,
	updated_at_micros = ?
WHERE
	id = ?
	AND state = 'processing'
	AND (locked_until_micros IS NULL OR locked_until_micros <= ?)`, msg, now, jobID, now)
}

func (s *JobStore) RequeueExpiredLease(ctx context.Context, jobID deltaflow.SyncJobID, nextRunAt time.Time, auditReason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reason := strings.TrimSpace(auditReason)
	if reason == "" {
		return deltaflow.ErrAuditReasonRequired
	}

	now := microsFromTime(s.Now())
	msg := "operator_requeue: " + reason
	return s.updateLeaseOperator(ctx, jobID, `
UPDATE deltaflow_sync_jobs
SET
	state = 'retrying',
	available_at_micros = ?,
	last_error = ?,
	locked_by = NULL,
	locked_until_micros = NULL,
	updated_at_micros = ?
WHERE
	id = ?
	AND state = 'processing'
	AND (locked_until_micros IS NULL OR locked_until_micros <= ?)`, microsFromTime(nextRunAt.UTC()), msg, now, jobID, now)
}

func (s *JobStore) listLeases(ctx context.Context, syncID deltaflow.SyncID, ts int64, predicate string, limit int) ([]*deltaflow.SyncJob, error) {
	query := `
SELECT
	id,
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
	available_at_micros,
	locked_by,
	locked_until_micros,
	ghost_detected,
	synced_at_micros,
	dead_at_micros,
	created_at_micros,
	updated_at_micros
FROM deltaflow_sync_jobs
WHERE
	state = 'processing'
	AND ` + predicate

	args := []any{ts}
	if syncID != "" {
		query += `
	AND sync_id = ?`
		args = append(args, syncID)
	}

	query += `
ORDER BY locked_until_micros ASC, updated_at_micros ASC, id ASC
LIMIT ?`
	args = append(args, limit)

	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return s.collectJobs(rows)
}

func (s *JobStore) collectJobs(rows *sql.Rows) ([]*deltaflow.SyncJob, error) {
	jobs := make([]*deltaflow.SyncJob, 0)
	for rows.Next() {
		job, ok, err := scanSyncJob(rows)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *JobStore) updateLeaseOperator(ctx context.Context, jobID deltaflow.SyncJobID, query string, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	res, err := s.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}

	exists, err := s.jobExists(ctx, jobID)
	if err != nil {
		return err
	}
	if !exists {
		return deltaflow.ErrJobNotFound
	}
	return deltaflow.ErrJobLeaseNotExpired
}

func (s *JobStore) jobExists(ctx context.Context, jobID deltaflow.SyncJobID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var exists int
	err := s.DB.QueryRowContext(ctx, `SELECT 1 FROM deltaflow_sync_jobs WHERE id = ?`, jobID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

const syncJobSelectByID = `
SELECT
	id,
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
	available_at_micros,
	locked_by,
	locked_until_micros,
	ghost_detected,
	synced_at_micros,
	dead_at_micros,
	created_at_micros,
	updated_at_micros
FROM deltaflow_sync_jobs
WHERE id = ?`

func scanSyncJob(scanner rowScanner) (*deltaflow.SyncJob, bool, error) {
	var (
		id                string
		syncID            string
		deltaID           sql.NullString
		origin            string
		projectionType    string
		projectionKeyText string
		projectionKeyHash string
		state             string
		attemptCount      int
		maxAttempts       int
		lastError         sql.NullString
		lastErrorCode     sql.NullString
		availableAt       int64
		lockedBy          sql.NullString
		lockedUntil       sql.NullInt64
		ghostDetected     int
		syncedAt          sql.NullInt64
		deadAt            sql.NullInt64
		createdAt         int64
		updatedAt         int64
	)

	err := scanner.Scan(
		&id,
		&syncID,
		&deltaID,
		&origin,
		&projectionType,
		&projectionKeyText,
		&projectionKeyHash,
		&state,
		&attemptCount,
		&maxAttempts,
		&lastError,
		&lastErrorCode,
		&availableAt,
		&lockedBy,
		&lockedUntil,
		&ghostDetected,
		&syncedAt,
		&deadAt,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}

	var projectionKey deltaflow.ProjectionKey
	if err := json.Unmarshal([]byte(projectionKeyText), &projectionKey); err != nil {
		return nil, false, err
	}

	job := &deltaflow.SyncJob{
		ID:                deltaflow.SyncJobID(id),
		SyncID:            deltaflow.SyncID(syncID),
		Origin:            deltaflow.SyncJobOriginType(origin),
		ProjectionType:    deltaflow.ProjectionType(projectionType),
		ProjectionKey:     projectionKey,
		ProjectionKeyHash: deltaflow.ProjectionKeyHash(projectionKeyHash),
		State:             deltaflow.SyncJobState(state),
		AttemptCount:      attemptCount,
		MaxAttempts:       maxAttempts,
		AvailableAt:       timeFromMicros(availableAt),
		GhostDetected:     ghostDetected != 0,
		CreatedAt:         timeFromMicros(createdAt),
		UpdatedAt:         timeFromMicros(updatedAt),
	}
	if deltaID.Valid {
		value := deltaflow.DeltaID(deltaID.String)
		job.DeltaID = &value
	}
	if lastError.Valid {
		v := lastError.String
		job.LastError = &v
	}
	if lastErrorCode.Valid {
		v := lastErrorCode.String
		job.LastErrorCode = &v
	}
	if lockedBy.Valid {
		v := lockedBy.String
		job.LockedBy = &v
	}
	if lockedUntil.Valid {
		t := timeFromMicros(lockedUntil.Int64)
		job.LockedUntil = &t
	}
	if syncedAt.Valid {
		t := timeFromMicros(syncedAt.Int64)
		job.SyncedAt = &t
	}
	if deadAt.Valid {
		t := timeFromMicros(deadAt.Int64)
		job.DeadAt = &t
	}

	return job, true, nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func timePtrMicros(value *time.Time) any {
	if value == nil {
		return nil
	}
	return microsFromTime(*value)
}

func isOutboxDeltaMappedViolation(err error) bool {
	if err == nil {
		return false
	}
	if !connectors.IsUniqueViolation(err) {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "deltaflow_sync_jobs.delta_id") || strings.Contains(message, "deltaflow_sync_jobs_outbox_delta_unique")
}
