package deltaflow

import "time"

// SyncID is an identifier for a synchronization process, which may encompass multiple deltas. It can be used to group related deltas together, such as those generated from the same source event or batch process.
// example: contacts-to-elasticsearch
type SyncID string

type DeltaID string

// Delta represents the business-facing outbox record.
// example, the outbox table
type Delta struct {
	ID     DeltaID
	SyncID SyncID

	Origin            OriginOperationType
	ProjectionType    ProjectionType
	ProjectionKey     ProjectionKey
	ProjectionKeyHash ProjectionKeyHash
	State             DeltaState

	OccurredAt   time.Time
	CreatedAt    time.Time
	DispatchedAt *time.Time

	Metadata map[string]any
}

type SyncJob struct {
	ID      SyncJobID
	SyncID  SyncID
	DeltaID *DeltaID
	Origin  SyncJobOriginType

	ProjectionType    ProjectionType
	ProjectionKey     ProjectionKey
	ProjectionKeyHash ProjectionKeyHash

	State SyncJobState

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
