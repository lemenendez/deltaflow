package deltaflow

import "errors"

var (
	ErrProjectionNotFound  = errors.New("projection not found")
	ErrDeltaNotFound       = errors.New("delta not found")
	ErrJobNotFound         = errors.New("job not found")
	ErrInvalidLockFor      = errors.New("lock duration must be positive")
	ErrOutboxJobNeedsDelta = errors.New("outbox job requires delta id")
	ErrDeltaAlreadyExists  = errors.New("delta already exists")
	ErrJobAlreadyExists    = errors.New("job already exists")
	ErrDeltaAlreadyMapped  = errors.New("delta already mapped to job")
)
