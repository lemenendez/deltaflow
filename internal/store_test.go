package internal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestMemoryDeltaStoreInsertsPendingDelta(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDeltaStore()

	inserted, err := store.Insert(ctx, deltaflow.Delta{
		SyncID:         "sync",
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Insert returned error: %v", err)
	}

	if inserted.ID == "" {
		t.Fatal("inserted ID is empty")
	}
	if inserted.State != deltaflow.StatePending {
		t.Fatalf("state = %s, want %s", inserted.State, deltaflow.StatePending)
	}
	if inserted.MaxAttempts != 5 {
		t.Fatalf("max_attempts = %d, want 5", inserted.MaxAttempts)
	}
}

func TestMemoryDeltaStoreClaimNextSkipsUnavailableRetry(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDeltaStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	_, err := store.Insert(ctx, deltaflow.Delta{
		ID:             "retry-later",
		SyncID:         "sync",
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
		State:       deltaflow.StateRetrying,
		AvailableAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("Insert retry-later returned error: %v", err)
	}

	ready, err := store.Insert(ctx, deltaflow.Delta{
		ID:             "ready",
		SyncID:         "sync",
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
	})
	if err != nil {
		t.Fatalf("Insert ready returned error: %v", err)
	}

	claimed, err := store.ClaimNext(ctx, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimNext returned nil")
	}
	if claimed.ID != ready.ID {
		t.Fatalf("claimed ID = %s, want %s", claimed.ID, ready.ID)
	}
	if claimed.State != deltaflow.StateProcessing {
		t.Fatalf("claimed state = %s, want %s", claimed.State, deltaflow.StateProcessing)
	}
}

func TestMemoryDeltaStoreClaimNextRespectsProcessingLease(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDeltaStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	lockedBy := "worker-1"
	lockedUntil := now.Add(time.Minute)
	inserted, err := store.Insert(ctx, deltaflow.Delta{
		ID:             "processing",
		SyncID:         "sync",
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
		State:       deltaflow.StateProcessing,
		LockedBy:    &lockedBy,
		LockedUntil: &lockedUntil,
	})
	if err != nil {
		t.Fatalf("Insert processing returned error: %v", err)
	}

	claimed, err := store.ClaimNext(ctx, "worker-2", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext before lease expiry returned error: %v", err)
	}
	if claimed != nil {
		t.Fatalf("ClaimNext before lease expiry returned delta %q, want nil", claimed.ID)
	}

	now = lockedUntil.Add(time.Nanosecond)
	claimed, err = store.ClaimNext(ctx, "worker-2", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext after lease expiry returned error: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimNext after lease expiry returned nil")
	}
	if claimed.ID != inserted.ID {
		t.Fatalf("claimed ID = %s, want %s", claimed.ID, inserted.ID)
	}
	if claimed.LockedBy == nil || *claimed.LockedBy != "worker-2" {
		t.Fatalf("locked_by = %v, want worker-2", claimed.LockedBy)
	}
	if claimed.LockedUntil == nil || !claimed.LockedUntil.Equal(now.Add(time.Minute)) {
		t.Fatalf("locked_until = %v, want %v", claimed.LockedUntil, now.Add(time.Minute))
	}
}

func TestMemoryDeltaStoreRejectsDuplicateIDs(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryDeltaStore()
	delta := deltaflow.Delta{
		ID:             "duplicate",
		SyncID:         "sync",
		ProjectionType: "Contact",
	}

	if _, err := store.Insert(ctx, delta); err != nil {
		t.Fatalf("first Insert returned error: %v", err)
	}
	if _, err := store.Insert(ctx, delta); !errors.Is(err, ErrDeltaAlreadyExists) {
		t.Fatalf("second Insert error = %v, want %v", err, ErrDeltaAlreadyExists)
	}
}
