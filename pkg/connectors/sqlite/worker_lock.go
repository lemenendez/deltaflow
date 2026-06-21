package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var ErrWorkerAlreadyRunning = errors.New("sqlite worker lock is already held")
var ErrWorkerLockNotOwned = errors.New("sqlite worker lock is not owned by worker")

func AcquireWorkerLock(ctx context.Context, db *sql.DB, workerID string, leaseFor time.Duration) (func(context.Context) error, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite worker lock requires database")
	}
	if workerID == "" {
		return nil, fmt.Errorf("sqlite worker lock requires worker id")
	}
	if leaseFor <= 0 {
		return nil, fmt.Errorf("sqlite worker lock requires positive lease")
	}

	now := time.Now().UTC()
	acquiredAt := microsFromTime(now)
	expiresAt := microsFromTime(now.Add(leaseFor))

	res, err := db.ExecContext(ctx, `
INSERT INTO deltaflow_worker_locks (lock_name, worker_id, acquired_at_micros, expires_at_micros)
VALUES ('singleton', ?, ?, ?)
ON CONFLICT(lock_name) DO UPDATE SET
	worker_id = excluded.worker_id,
	acquired_at_micros = excluded.acquired_at_micros,
	expires_at_micros = excluded.expires_at_micros
WHERE deltaflow_worker_locks.expires_at_micros <= ?`, workerID, acquiredAt, expiresAt, acquiredAt)
	if err != nil {
		return nil, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected == 0 {
		return nil, ErrWorkerAlreadyRunning
	}

	release := func(releaseCtx context.Context) error {
		_, err := db.ExecContext(releaseCtx, `DELETE FROM deltaflow_worker_locks WHERE lock_name = 'singleton' AND worker_id = ?`, workerID)
		return err
	}

	return release, nil
}

func RenewWorkerLock(ctx context.Context, db *sql.DB, workerID string, leaseFor time.Duration) error {
	if db == nil {
		return fmt.Errorf("sqlite worker lock requires database")
	}
	if workerID == "" {
		return fmt.Errorf("sqlite worker lock requires worker id")
	}
	if leaseFor <= 0 {
		return fmt.Errorf("sqlite worker lock requires positive lease")
	}

	expiresAt := microsFromTime(time.Now().UTC().Add(leaseFor))
	res, err := db.ExecContext(ctx, `
UPDATE deltaflow_worker_locks
SET expires_at_micros = ?
WHERE lock_name = 'singleton' AND worker_id = ?`, expiresAt, workerID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrWorkerLockNotOwned
	}

	return nil
}
