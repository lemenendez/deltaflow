package deltaflow

import "errors"

var (
	ErrProjectionNotFound  = errors.New("projection not found")
	ErrDeltaNotFound       = errors.New("delta not found")
	ErrJobNotFound         = errors.New("job not found")
	ErrInvalidLockFor      = errors.New("lock duration must be positive")
	ErrDeltaIDProvided     = errors.New("delta id must be empty")
	ErrOutboxJobNeedsDelta = errors.New("outbox job requires delta id")
	ErrJobIDProvided       = errors.New("job id must be empty")
	ErrDeltaAlreadyMapped  = errors.New("delta already mapped to job")
)
