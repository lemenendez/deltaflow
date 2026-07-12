package deltaflow

import "errors"

var (
	// Returned by backfill helpers when callers omit the target sync identifier.
	ErrSyncIDRequired = errors.New("sync id is required")
	// Returned by backfill helpers when callers omit the source origin operation.
	ErrOriginRequired = errors.New("origin operation is required")
	// Returned by backfill helpers when callers omit the projection type.
	ErrProjectionTypeRequired = errors.New("projection type is required")
	// Returned by backfill helpers when callers omit the projection key.
	ErrProjectionKeyRequired = errors.New("projection key is required")
	// Returned by Projector implementations when a projection does not exist.
	ErrProjectionNotFound = errors.New("projection not found")
	// Returned by DeltaStore mutators when the referenced delta cannot be found.
	ErrDeltaNotFound = errors.New("delta not found")
	// Returned by JobStore mutators when the referenced job cannot be found.
	ErrJobNotFound = errors.New("job not found")
	// Returned by JobStore.ClaimNext when lockFor is zero or negative.
	ErrInvalidLockFor = errors.New("lock duration must be positive")
	// Returned by JobStore lease mutators when the caller no longer owns the lease.
	ErrJobLeaseNotOwned = errors.New("job lease not owned")
	// Returned by DeltaStore.Enqueue when callers provide a non-empty Delta.ID.
	ErrDeltaIDProvided = errors.New("delta id must be empty")
	// Returned by EnqueueBatch when a delta does not provide a dedup window.
	ErrDedupWindowRequired = errors.New("dedup window is required")
	// Returned by EnqueueBatch when deltas do not all use the same window.
	ErrMixedDedupWindows = errors.New("enqueue batch must use one dedup window")
	// Returned by EnqueueBatch when the batch exceeds the store's configured maximum.
	ErrEnqueueBatchTooLarge = errors.New("enqueue batch exceeds maximum size")
	// Returned by JobStore.Create when an outbox job omits DeltaID.
	ErrOutboxJobNeedsDelta = errors.New("outbox job requires delta id")
	// Returned by JobStore.Create when callers provide a non-empty SyncJob.ID.
	ErrJobIDProvided = errors.New("job id must be empty")
	// Returned by JobStore.Create when an outbox job reuses a mapped delta.
	ErrDeltaAlreadyMapped = errors.New("delta already mapped to job")
	// Returned by lease query helpers when the near-expiry window is negative.
	ErrInvalidLeaseWindow = errors.New("lease window must be non-negative")
	// Returned by operator lease actions when an audit reason is empty.
	ErrAuditReasonRequired = errors.New("audit reason is required")
	// Returned by operator lease actions when the target job is not eligible for expired-lease actions.
	ErrJobLeaseNotExpired = errors.New("job not eligible for expired lease action")
)
