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

// SyncJobOriginType identifies what created a SyncJob,
// such as outbox dispatch, backfill, replay, manual operation, or an unknown source.
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
	Pull(ctx context.Context, syncID SyncID, limit int) ([]*Delta, error)
	MarkDispatched(ctx context.Context, deltaID DeltaID) error
}

type JobStore interface {
	Create(ctx context.Context, job SyncJob) (*SyncJob, error)

	Get(ctx context.Context, jobID SyncJobID) (*SyncJob, bool, error)

	ClaimNext(ctx context.Context, syncID SyncID, workerID string, lockFor time.Duration) (*SyncJob, error)

	RenewLease(ctx context.Context, jobID SyncJobID, workerID string, lockFor time.Duration) error

	MarkSynced(ctx context.Context, jobID SyncJobID, workerID string, ghostDetected bool) error

	MarkRetrying(ctx context.Context, jobID SyncJobID, workerID string, err error, nextRunAt time.Time) error

	MarkDead(ctx context.Context, jobID SyncJobID, workerID string, err error) error
}

// JobLeaseQueries exposes optional operational lease query helpers.
// Implementations may be type-asserted from JobStore where supported.
type JobLeaseQueries interface {
	// ListActiveLeases returns processing jobs with a non-expired lease.
	// If syncID is empty, all syncs are considered. A non-positive limit returns no rows.
	ListActiveLeases(ctx context.Context, syncID SyncID, limit int) ([]*SyncJob, error)

	// ListExpiredProcessingLeases returns processing jobs with expired or missing leases.
	// If syncID is empty, all syncs are considered. A non-positive limit returns no rows.
	ListExpiredProcessingLeases(ctx context.Context, syncID SyncID, limit int) ([]*SyncJob, error)

	// ListNearExpiryLeases returns processing jobs with leases expiring within the provided window.
	// If syncID is empty, all syncs are considered. A non-positive limit returns no rows.
	ListNearExpiryLeases(ctx context.Context, syncID SyncID, within time.Duration, limit int) ([]*SyncJob, error)
}

// Optional operational lease queries can be discovered by type assertion:
//
//	if leases, ok := jobStore.(JobLeaseQueries); ok {
//		jobs, err := leases.ListActiveLeases(ctx, syncID, 100)
//		...
//	}

// JobLeaseOperatorActions exposes optional operator-focused lease controls.
// Implementations may be type-asserted from JobStore where supported.
type JobLeaseOperatorActions interface {
	// ForceReleaseExpiredLease clears lease ownership on an expired processing job.
	// The auditReason must be non-empty.
	ForceReleaseExpiredLease(ctx context.Context, jobID SyncJobID, auditReason string) error

	// RequeueExpiredLease moves an expired processing job to retrying and clears its lease.
	// The auditReason must be non-empty.
	RequeueExpiredLease(ctx context.Context, jobID SyncJobID, nextRunAt time.Time, auditReason string) error
}

// Optional operator controls can be discovered by type assertion:
//
//	if ops, ok := jobStore.(JobLeaseOperatorActions); ok {
//		err := ops.ForceReleaseExpiredLease(ctx, jobID, "manual intervention")
//		...
//	}

type DispatchStore interface {
	DispatchPending(ctx context.Context, syncID SyncID, limit int) ([]*SyncJob, error)
}
