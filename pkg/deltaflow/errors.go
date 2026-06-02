package deltaflow

import "errors"

var (
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
	// Returned by JobStore.Create when an outbox job omits DeltaID.
	ErrOutboxJobNeedsDelta = errors.New("outbox job requires delta id")
	// Returned by JobStore.Create when callers provide a non-empty SyncJob.ID.
	ErrJobIDProvided = errors.New("job id must be empty")
	// Returned by JobStore.Create when an outbox job reuses a mapped delta.
	ErrDeltaAlreadyMapped = errors.New("delta already mapped to job")
	// Returned by lease query helpers when the near-expiry window is negative.
	ErrInvalidLeaseWindow = errors.New("lease window must be non-negative")
)
