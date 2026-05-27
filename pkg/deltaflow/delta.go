package deltaflow

import "time"

type Delta struct {
	ID                string
	SyncID            string
	ProjectionType    ProjectionType
	ProjectionKey     ProjectionKey
	ProjectionKeyHash string

	State        DeltaState
	AttemptCount int
	MaxAttempts  int

	LastError     *string
	LastErrorCode *string

	AvailableAt time.Time
	LockedBy    *string
	LockedUntil *time.Time

	GhostDetected bool

	SyncedAt *time.Time
	DeadAt   *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}
