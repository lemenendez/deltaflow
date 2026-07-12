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
	DedupWindow       DedupWindow
	DedupKey          DedupKey
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

// NewBackfillDelta constructs a Delta for application-owned backfill scans.
// The caller still owns source enumeration, checkpointing, and batching.
func NewBackfillDelta(syncID SyncID, origin OriginOperationType, identity ProjectionIdentity, window DedupWindow) (Delta, error) {
	if syncID == "" {
		return Delta{}, ErrSyncIDRequired
	}
	if origin == "" {
		return Delta{}, ErrOriginRequired
	}
	if identity.Type == "" {
		return Delta{}, ErrProjectionTypeRequired
	}
	if len(identity.Key) == 0 {
		return Delta{}, ErrProjectionKeyRequired
	}
	if window == "" {
		return Delta{}, ErrDedupWindowRequired
	}

	return Delta{
		SyncID:         syncID,
		Origin:         origin,
		ProjectionType: identity.Type,
		ProjectionKey:  cloneProjectionKey(identity.Key),
		DedupWindow:    window,
	}, nil
}

func cloneProjectionKey(key ProjectionKey) ProjectionKey {
	if key == nil {
		return nil
	}

	cloned := make(ProjectionKey, len(key))
	for name, raw := range key {
		if raw == nil {
			cloned[name] = nil
			continue
		}
		cloned[name] = append([]byte(nil), raw...)
	}

	return cloned
}
