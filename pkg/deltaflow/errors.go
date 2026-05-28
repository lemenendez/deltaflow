package deltaflow

import "errors"

var (
	ErrProjectionNotFound = errors.New("projection not found")
	ErrDeltaNotFound      = errors.New("delta not found")
	ErrInvalidLockFor     = errors.New("lock duration must be positive")
)
