package deltaflow

import (
	"context"
	"encoding/json"
	"time"
)

// OriginOperationType operation done by the origin system that caused the delta to be created.
// This is used for tracking the source of changes and understanding the context in which a delta was generated.
type OriginOperationType string

const (
	OriginOperationInserted     OriginOperationType = "inserted"
	OriginOperationUpdated      OriginOperationType = "updated"
	OriginOperationDeleted      OriginOperationType = "deleted"
	OriginOperationChildChanged OriginOperationType = "child_changed"
)

// JobOriginType is the origin of the job that created the delta,
// which can be used for auditing, debugging, and understanding the context of changes.
// It helps to categorize deltas based on their source, such as whether they were generated from a backfill process, a manual operation, or an outbox event.
type SyncJobOriginType string

const (
	JobOriginBackfill SyncJobOriginType = "backfill"
	JobOriginReplay   SyncJobOriginType = "replay"
	JobOriginManual   SyncJobOriginType = "manual"
	JobOriginOutbox   SyncJobOriginType = "outbox"
	JobOriginUnknown  SyncJobOriginType = "unknown"
)

// DeltaState represents the current state of a delta in the synchronization process.
// It indicates where the delta is in its lifecycle, such as whether it's waiting to be processed, currently being handled, or has been marked as ignored due to errors or other conditions.
type DeltaState string

const (
	DeltaPending    DeltaState = "pending"
	DeltaDispatched DeltaState = "dispatched"
	DeltaIgnored    DeltaState = "ignored"
)

// SyncJobState represents the current state of a synchronization job, which can encompass multiple deltas. It indicates the overall progress and status of the synchronization process, such as whether it's still pending, actively processing, has completed successfully, is retrying after failures, or has been marked as dead due to unrecoverable errors.
type SyncJobState string

const (
	StatePending    SyncJobState = "pending"
	StateProcessing SyncJobState = "processing"
	StateSynced     SyncJobState = "synced"
	StateRetrying   SyncJobState = "retrying"
	StateDead       SyncJobState = "dead"
)

type SyncJobID string

// ProjectionType is the type of projection, used to categorize and identify the kind of projection being handled.
// examples, Contact, Employee, Order, etc.
type ProjectionType string

// ProjectionOperationType is the intent operation in the ProjectorApplier.
type ProjectionOperationType string

// ProjectionKey stores projection identity key components as JSON values.
// Its JSON encoding canonicalizes each embedded value before marshaling so the
// serialized form is stable for hashing and persistence.
type ProjectionKey map[string]json.RawMessage

type ProjectionIdentity struct {
	Type ProjectionType
	Key  ProjectionKey
}

type Projection struct {
	Identity  ProjectionIdentity
	Payload   []byte
	MediaType string
	Checksum  string
}

const (
	ProjectionOpUpsert ProjectionOperationType = "upsert"
	ProjectionOpDelete ProjectionOperationType = "delete"
)

type ProjectionKeyHash string

type ProjectionOperation struct {
	Type       ProjectionOperationType
	Identity   ProjectionIdentity
	Projection *Projection
}

type Projector interface {
	Project(ctx context.Context, identity ProjectionIdentity) (Projection, error)
}

type ProjectionApplier interface {
	Apply(ctx context.Context, op ProjectionOperation) error
}

type ProjectorFunc func(context.Context, ProjectionIdentity) (Projection, error)

func (f ProjectorFunc) Project(ctx context.Context, id ProjectionIdentity) (Projection, error) {
	return f(ctx, id)
}

type ProjectionApplierFunc func(context.Context, ProjectionOperation) error

func (f ProjectionApplierFunc) Apply(ctx context.Context, op ProjectionOperation) error {
	return f(ctx, op)
}

type Engine interface {
	Run(ctx context.Context) error
}

type DeltaStore interface {
	Enqueue(ctx context.Context, delta Delta) (*Delta, error)

	Get(ctx context.Context, deltaID DeltaID) (*Delta, bool, error)

	Pull(ctx context.Context, limit int) ([]*Delta, error)

	MarkDispatched(ctx context.Context, deltaID DeltaID) error
}

type JobStore interface {
	Create(ctx context.Context, job SyncJob) (*SyncJob, error)

	Get(ctx context.Context, jobID SyncJobID) (*SyncJob, bool, error)

	ClaimNext(ctx context.Context, workerID string, lockFor time.Duration) (*SyncJob, error)

	MarkSynced(ctx context.Context, jobID SyncJobID, ghostDetected bool) error

	MarkRetrying(ctx context.Context, jobID SyncJobID, err error, nextRunAt time.Time) error

	MarkDead(ctx context.Context, jobID SyncJobID, err error) error
}

type DispatchStore interface {
	DispatchPending(ctx context.Context, limit int) ([]*SyncJob, error)
}
