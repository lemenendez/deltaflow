package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestRenewWorkerLockExtendsLease(t *testing.T) {
	db := openSQLiteTestDB(t)
	ctx := context.Background()

	release, err := AcquireWorkerLock(ctx, db, "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("AcquireWorkerLock error: %v", err)
	}
	t.Cleanup(func() {
		_ = release(context.Background())
	})

	before, err := lockExpiryMicros(ctx, db)
	if err != nil {
		t.Fatalf("lockExpiryMicros before renew error: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	if err := RenewWorkerLock(ctx, db, "worker-a", 250*time.Millisecond); err != nil {
		t.Fatalf("RenewWorkerLock error: %v", err)
	}

	after, err := lockExpiryMicros(ctx, db)
	if err != nil {
		t.Fatalf("lockExpiryMicros after renew error: %v", err)
	}
	if after <= before {
		t.Fatalf("expires_at_micros = %d, want > %d after renewal", after, before)
	}
}

func TestRenewWorkerLockRejectsLostOwnership(t *testing.T) {
	db := openSQLiteTestDB(t)
	ctx := context.Background()

	releaseA, err := AcquireWorkerLock(ctx, db, "worker-a", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("AcquireWorkerLock(worker-a) error: %v", err)
	}
	t.Cleanup(func() {
		_ = releaseA(context.Background())
	})

	waitForSingletonLockExpiry(t, ctx, db, 2*time.Second, 5*time.Millisecond)

	releaseB, err := AcquireWorkerLock(ctx, db, "worker-b", time.Second)
	if err != nil {
		t.Fatalf("AcquireWorkerLock(worker-b) error: %v", err)
	}
	t.Cleanup(func() {
		_ = releaseB(context.Background())
	})

	err = RenewWorkerLock(ctx, db, "worker-a", time.Second)
	if !errors.Is(err, ErrWorkerLockNotOwned) {
		t.Fatalf("RenewWorkerLock(worker-a) error = %v, want ErrWorkerLockNotOwned", err)
	}
}

func waitForSingletonLockExpiry(t *testing.T, ctx context.Context, db *sql.DB, timeout time.Duration, pollInterval time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		expiresAtMicros, err := lockExpiryMicros(ctx, db)
		if err != nil {
			t.Fatalf("lockExpiryMicros during wait error: %v", err)
		}
		if expiresAtMicros <= microsFromTime(time.Now().UTC()) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("singleton lock did not expire within %v (expires_at_micros=%d)", timeout, expiresAtMicros)
		}
		time.Sleep(pollInterval)
	}
}

func lockExpiryMicros(ctx context.Context, db *sql.DB) (int64, error) {
	var expiresAt int64
	if err := db.QueryRowContext(ctx, `
SELECT expires_at_micros
FROM deltaflow_worker_locks
WHERE lock_name = 'singleton'`).Scan(&expiresAt); err != nil {
		return 0, err
	}
	return expiresAt, nil
}
