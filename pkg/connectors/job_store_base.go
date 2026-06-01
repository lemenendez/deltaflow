package connectors

import (
	"database/sql"
	"encoding/json"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

const defaultJobStoreMaxAttempts = 5

type JobStoreBaseConfig struct {
	Now         func() time.Time
	MaxAttempts int
}

type JobStoreBase struct {
	DB  *sql.DB
	cfg JobStoreBaseConfig
}

func NewJobStoreBase(db *sql.DB, cfg JobStoreBaseConfig) JobStoreBase {
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultJobStoreMaxAttempts
	}
	return JobStoreBase{DB: db, cfg: cfg}
}

func (b *JobStoreBase) Now() time.Time {
	return b.cfg.Now().UTC()
}

func (b *JobStoreBase) DefaultMaxAttempts() int {
	return b.cfg.MaxAttempts
}

func (b *JobStoreBase) PrepareJobForCreate(job deltaflow.SyncJob) (deltaflow.SyncJob, error) {
	now := b.Now()

	hash, err := projectionKeyHash(job.ProjectionKey)
	if err != nil {
		return deltaflow.SyncJob{}, err
	}
	job.ProjectionKeyHash = hash

	if job.Origin == "" {
		job.Origin = deltaflow.JobOriginUnknown
	}
	if job.Origin == deltaflow.JobOriginOutbox && job.DeltaID == nil {
		return deltaflow.SyncJob{}, deltaflow.ErrOutboxJobNeedsDelta
	}
	if job.State == "" {
		job.State = deltaflow.StatePending
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = b.cfg.MaxAttempts
	}
	if job.AvailableAt.IsZero() {
		job.AvailableAt = now
	} else {
		job.AvailableAt = job.AvailableAt.UTC()
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	} else {
		job.CreatedAt = job.CreatedAt.UTC()
	}
	job.UpdatedAt = now

	return job, nil
}

func (b *JobStoreBase) ScanSyncJob(scanner rowScanner) (*deltaflow.SyncJob, bool, error) {
	var (
		id                string
		syncID            string
		deltaID           sql.NullString
		origin            string
		projectionType    string
		projectionKeyJSON []byte
		projectionKeyHash string
		state             string
		attemptCount      int
		maxAttempts       int
		lastError         sql.NullString
		lastErrorCode     sql.NullString
		availableAt       time.Time
		lockedBy          sql.NullString
		lockedUntil       sql.NullTime
		ghostDetected     bool
		syncedAt          sql.NullTime
		deadAt            sql.NullTime
		createdAt         time.Time
		updatedAt         time.Time
	)

	err := scanner.Scan(
		&id,
		&syncID,
		&deltaID,
		&origin,
		&projectionType,
		&projectionKeyJSON,
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
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var projectionKey deltaflow.ProjectionKey
	if len(projectionKeyJSON) > 0 {
		if err := json.Unmarshal(projectionKeyJSON, &projectionKey); err != nil {
			return nil, false, err
		}
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
		AvailableAt:       availableAt.UTC(),
		GhostDetected:     ghostDetected,
		CreatedAt:         createdAt.UTC(),
		UpdatedAt:         updatedAt.UTC(),
	}
	if deltaID.Valid {
		value := deltaflow.DeltaID(deltaID.String)
		job.DeltaID = &value
	}
	if lastError.Valid {
		value := lastError.String
		job.LastError = &value
	}
	if lastErrorCode.Valid {
		value := lastErrorCode.String
		job.LastErrorCode = &value
	}
	if lockedBy.Valid {
		value := lockedBy.String
		job.LockedBy = &value
	}
	if lockedUntil.Valid {
		value := lockedUntil.Time.UTC()
		job.LockedUntil = &value
	}
	if syncedAt.Valid {
		value := syncedAt.Time.UTC()
		job.SyncedAt = &value
	}
	if deadAt.Valid {
		value := deadAt.Time.UTC()
		job.DeadAt = &value
	}

	return job, true, nil
}

func ErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
