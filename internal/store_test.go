package internal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestDeltaMemoryStoreEnqueuePendingDelta(t *testing.T) {
	ctx := context.Background()
	store := NewDeltaMemoryStore()

	inserted, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if inserted.ID == "" {
		t.Fatal("inserted ID is empty")
	}
	if inserted.State != deltaflow.DeltaPending {
		t.Fatalf("state = %s, want %s", inserted.State, deltaflow.DeltaPending)
	}
	if inserted.CreatedAt.IsZero() {
		t.Fatal("created_at is zero")
	}
	if inserted.OccurredAt.IsZero() {
		t.Fatal("occurred_at is zero")
	}
}

func TestDeltaMemoryStoreEnqueueBatchIsWindowIdempotent(t *testing.T) {
	store := NewDeltaMemoryStore()
	deltas := []deltaflow.Delta{
		{SyncID: "sync", ProjectionType: "Customer", ProjectionKey: deltaflow.ProjectionKey{"id": json.RawMessage(`"1"`)}, DedupWindow: "customers-2026"},
		{SyncID: "sync", ProjectionType: "Customer", ProjectionKey: deltaflow.ProjectionKey{"id": json.RawMessage(`"2"`)}, DedupWindow: "customers-2026"},
		{SyncID: "sync", ProjectionType: "Customer", ProjectionKey: deltaflow.ProjectionKey{"id": json.RawMessage(`"1"`)}, DedupWindow: "customers-2026"},
	}

	first, err := store.EnqueueBatch(context.Background(), deltas)
	if err != nil {
		t.Fatalf("first EnqueueBatch: %v", err)
	}
	if first.RequestedCount != 3 || first.InsertedCount != 2 || first.DuplicateCount != 1 {
		t.Fatalf("first result = %#v", first)
	}
	second, err := store.EnqueueBatch(context.Background(), deltas)
	if err != nil {
		t.Fatalf("second EnqueueBatch: %v", err)
	}
	if second.InsertedCount != 0 || second.DuplicateCount != 3 {
		t.Fatalf("second result = %#v", second)
	}

	other := deltas[:1]
	other[0].DedupWindow = "customers-2027"
	third, err := store.EnqueueBatch(context.Background(), other)
	if err != nil {
		t.Fatalf("other-window EnqueueBatch: %v", err)
	}
	if third.InsertedCount != 1 {
		t.Fatalf("other-window result = %#v", third)
	}
}

func TestDeltaMemoryStoreEnqueueReturnsExistingWindowDuplicate(t *testing.T) {
	store := NewDeltaMemoryStore()
	delta := deltaflow.Delta{SyncID: "sync", ProjectionType: "Customer", ProjectionKey: deltaflow.ProjectionKey{"id": json.RawMessage(`"1"`)}, DedupWindow: "window"}
	first, err := store.Enqueue(context.Background(), delta)
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	second, err := store.Enqueue(context.Background(), delta)
	if err != nil {
		t.Fatalf("second Enqueue: %v", err)
	}
	if second.ID != first.ID || second.DedupKey == "" {
		t.Fatalf("duplicate = %#v, first = %#v", second, first)
	}
}

func TestDeltaMemoryStoreEnqueueBatchGuardrails(t *testing.T) {
	store := NewDeltaMemoryStore()
	_, err := store.EnqueueBatch(context.Background(), []deltaflow.Delta{{}})
	if !errors.Is(err, deltaflow.ErrDedupWindowRequired) {
		t.Fatalf("missing window error = %v", err)
	}
	_, err = store.EnqueueBatch(context.Background(), []deltaflow.Delta{{DedupWindow: "a"}, {DedupWindow: "b"}})
	if !errors.Is(err, deltaflow.ErrMixedDedupWindows) {
		t.Fatalf("mixed window error = %v", err)
	}
	limited := NewDeltaMemoryStoreWithConfig(DeltaMemoryStoreConfig{MaxEnqueueBatchSize: 1})
	_, err = limited.EnqueueBatch(context.Background(), []deltaflow.Delta{{DedupWindow: "a"}, {DedupWindow: "a"}})
	if !errors.Is(err, deltaflow.ErrEnqueueBatchTooLarge) {
		t.Fatalf("oversized batch error = %v", err)
	}
}

func TestDeltaMemoryStoreEnqueueComputesProjectionKeyHash(t *testing.T) {
	ctx := context.Background()
	store := NewDeltaMemoryStore()

	inserted, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
			"tenant_id":  json.RawMessage(`"t1"`),
		},
	})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if inserted.ProjectionKeyHash == "" {
		t.Fatal("projection_key_hash is empty")
	}

	encoded, err := json.Marshal(inserted.ProjectionKey)
	if err != nil {
		t.Fatalf("Marshal projection key returned error: %v", err)
	}
	sum := sha256.Sum256(encoded)
	want := deltaflow.ProjectionKeyHash(hex.EncodeToString(sum[:]))
	if inserted.ProjectionKeyHash != want {
		t.Fatalf("projection_key_hash = %s, want %s", inserted.ProjectionKeyHash, want)
	}
}

func TestDeltaMemoryStoreEnqueueOverridesCallerProjectionKeyHash(t *testing.T) {
	ctx := context.Background()
	store := NewDeltaMemoryStore()

	inserted, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:            "sync",
		Origin:            deltaflow.OriginOperationInserted,
		ProjectionType:    "Contact",
		ProjectionKeyHash: "stale-hash",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
			"tenant_id":  json.RawMessage(`"t1"`),
		},
	})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	encoded, err := json.Marshal(inserted.ProjectionKey)
	if err != nil {
		t.Fatalf("Marshal projection key returned error: %v", err)
	}
	sum := sha256.Sum256(encoded)
	want := deltaflow.ProjectionKeyHash(hex.EncodeToString(sum[:]))
	if inserted.ProjectionKeyHash != want {
		t.Fatalf("projection_key_hash = %s, want %s", inserted.ProjectionKeyHash, want)
	}
	if inserted.ProjectionKeyHash == "stale-hash" {
		t.Fatal("projection_key_hash preserved caller-supplied stale value")
	}
}

func TestDeltaMemoryStorePullReturnsPendingInOrder(t *testing.T) {
	ctx := context.Background()
	store := NewDeltaMemoryStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	_, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
		OccurredAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("Enqueue later returned error: %v", err)
	}

	ready, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
		OccurredAt: now,
	})
	if err != nil {
		t.Fatalf("Enqueue ready returned error: %v", err)
	}
	foreign, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "other-sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"3"`),
		},
		OccurredAt: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("Enqueue foreign returned error: %v", err)
	}

	pulled, err := store.Pull(ctx, "sync", 1)
	if err != nil {
		t.Fatalf("Pull returned error: %v", err)
	}
	if len(pulled) != 1 {
		t.Fatalf("pulled = %d items, want 1", len(pulled))
	}
	if pulled[0].ID != ready.ID {
		t.Fatalf("pulled ID = %s, want %s", pulled[0].ID, ready.ID)
	}
	if pulled[0].State != deltaflow.DeltaPending {
		t.Fatalf("pulled state = %s, want %s", pulled[0].State, deltaflow.DeltaPending)
	}
	if pulled[0].ID == foreign.ID {
		t.Fatal("Pull ignored syncID and returned foreign delta")
	}
}

func TestDeltaMemoryStoreMarkDispatched(t *testing.T) {
	ctx := context.Background()
	store := NewDeltaMemoryStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	inserted, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if err := store.MarkDispatched(ctx, inserted.ID); err != nil {
		t.Fatalf("MarkDispatched returned error: %v", err)
	}

	got, ok, err := store.Get(ctx, inserted.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !ok {
		t.Fatalf("delta %q not found", inserted.ID)
	}
	if got.State != deltaflow.DeltaDispatched {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.DeltaDispatched)
	}
	if got.DispatchedAt == nil || !got.DispatchedAt.Equal(now) {
		t.Fatalf("dispatched_at = %v, want %v", got.DispatchedAt, now)
	}
}

func TestDeltaMemoryStoreRejectsProvidedID(t *testing.T) {
	ctx := context.Background()
	store := NewDeltaMemoryStore()
	_, err := store.Enqueue(ctx, deltaflow.Delta{
		ID:             "duplicate",
		SyncID:         "sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
	})
	if !errors.Is(err, deltaflow.ErrDeltaIDProvided) {
		t.Fatalf("Enqueue error = %v, want %v", err, deltaflow.ErrDeltaIDProvided)
	}
}

func TestDeltaMemoryStoreClonesMetadataDeeply(t *testing.T) {
	ctx := context.Background()
	store := NewDeltaMemoryStore()
	child := map[string]any{"value": []any{"nested"}}
	tags := []string{"alpha", "beta"}
	attrs := map[string]string{"role": "admin"}
	numbers := map[int]any{1: []string{"one"}}
	metadata := map[string]any{
		"labels":   []any{"a", child, nil},
		"props":    map[string]any{"child": child},
		"nullable": map[string]any{"inner": nil},
		"tags":     tags,
		"attrs":    attrs,
		"numbers":  numbers,
	}

	inserted, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		Metadata:       metadata,
	})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	child["value"].([]any)[0] = "mutated"
	metadata["labels"].([]any)[0] = "mutated"
	tags[0] = "mutated"
	attrs["role"] = "mutated"
	numbers[1].([]string)[0] = "mutated"

	got, ok, err := store.Get(ctx, inserted.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !ok {
		t.Fatalf("delta %q not found", inserted.ID)
	}

	labels := got.Metadata["labels"].([]any)
	if labels[0] != "a" {
		t.Fatalf("labels[0] = %v, want %q", labels[0], "a")
	}
	if nested := labels[1].(map[string]any)["value"].([]any)[0]; nested != "nested" {
		t.Fatalf("nested metadata = %v, want %q", nested, "nested")
	}
	if labels[2] != nil {
		t.Fatalf("labels[2] = %v, want nil", labels[2])
	}
	if got.Metadata["nullable"].(map[string]any)["inner"] != nil {
		t.Fatalf("nullable.inner = %v, want nil", got.Metadata["nullable"])
	}
	if got.Metadata["tags"].([]string)[0] != "alpha" {
		t.Fatalf("tags[0] = %v, want %q", got.Metadata["tags"], "alpha")
	}
	if got.Metadata["attrs"].(map[string]string)["role"] != "admin" {
		t.Fatalf("attrs.role = %v, want %q", got.Metadata["attrs"], "admin")
	}
	if got.Metadata["numbers"].(map[int]any)[1].([]string)[0] != "one" {
		t.Fatalf("numbers[1][0] = %v, want %q", got.Metadata["numbers"], "one")
	}

	labels[0] = "changed-again"
	gotAgain, ok, err := store.Get(ctx, inserted.ID)
	if err != nil {
		t.Fatalf("second Get returned error: %v", err)
	}
	if !ok {
		t.Fatalf("delta %q not found on second get", inserted.ID)
	}
	if gotAgain.Metadata["labels"].([]any)[0] != "a" {
		t.Fatalf("stored metadata was mutated through Get result: %v", gotAgain.Metadata["labels"])
	}
	gotTags := got.Metadata["tags"].([]string)
	gotTags[0] = "changed-again"
	if gotAgain.Metadata["tags"].([]string)[0] != "alpha" {
		t.Fatalf("stored tags were mutated through Get result: %v", gotAgain.Metadata["tags"])
	}
}

func TestJobMemoryStoreClaimNextRespectsAvailabilityAndLease(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	_, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		State:          deltaflow.StateRetrying,
		AvailableAt:    now.Add(time.Minute),
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Create retry-later returned error: %v", err)
	}

	ready, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
	})
	if err != nil {
		t.Fatalf("Create ready returned error: %v", err)
	}
	foreign, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         "other-sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"3"`),
		},
		CreatedAt: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("Create foreign returned error: %v", err)
	}

	claimed, err := store.ClaimNext(ctx, "sync", "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimNext returned nil")
	}
	if claimed.ID != ready.ID {
		t.Fatalf("claimed ID = %s, want %s", claimed.ID, ready.ID)
	}
	if claimed.ID == foreign.ID {
		t.Fatal("ClaimNext ignored syncID and claimed foreign job")
	}
	if claimed.State != deltaflow.StateProcessing {
		t.Fatalf("state = %s, want %s", claimed.State, deltaflow.StateProcessing)
	}

	claimedAgain, err := store.ClaimNext(ctx, "sync", "worker-2", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext while leased returned error: %v", err)
	}
	if claimedAgain != nil {
		t.Fatalf("ClaimNext while leased returned job %q, want nil", claimedAgain.ID)
	}

	now = now.Add(2 * time.Minute)
	claimedExpired, err := store.ClaimNext(ctx, "sync", "worker-2", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext after expiry returned error: %v", err)
	}
	if claimedExpired == nil {
		t.Fatal("ClaimNext after expiry returned nil")
	}
	if claimedExpired.ID != ready.ID {
		t.Fatalf("claimed ID after expiry = %s, want %s", claimedExpired.ID, ready.ID)
	}
	if claimedExpired.LockedBy == nil || *claimedExpired.LockedBy != "worker-2" {
		t.Fatalf("locked_by = %v, want worker-2", claimedExpired.LockedBy)
	}
	if claimedExpired.LockedUntil == nil || !claimedExpired.LockedUntil.Equal(now.Add(time.Minute)) {
		t.Fatalf("locked_until = %v, want %v", claimedExpired.LockedUntil, now.Add(time.Minute))
	}
	if got, ok, err := store.Get(ctx, claimedExpired.ID); err != nil || !ok || got.State != deltaflow.StateProcessing {
		t.Fatalf("Get after claim = (%v, %v, %v), want processing state", got, ok, err)
	}
	if _, err := store.ClaimNext(ctx, "sync", "worker-1", 0); !errors.Is(err, deltaflow.ErrInvalidLockFor) {
		t.Fatalf("ClaimNext zero lock error = %v, want %v", err, deltaflow.ErrInvalidLockFor)
	}
	if _, err := store.Create(ctx, deltaflow.SyncJob{ID: "ready", SyncID: "sync", Origin: deltaflow.JobOriginManual}); !errors.Is(err, deltaflow.ErrJobIDProvided) {
		t.Fatalf("Create explicit ID error = %v, want %v", err, deltaflow.ErrJobIDProvided)
	}
}

func TestJobMemoryStoreClaimNextBatchRespectsLimitAndOrder(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	first, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		State:          deltaflow.StatePending,
		AvailableAt:    now.Add(-3 * time.Minute),
		CreatedAt:      now.Add(-3 * time.Minute),
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Create first returned error: %v", err)
	}
	second, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		State:          deltaflow.StatePending,
		AvailableAt:    now.Add(-2 * time.Minute),
		CreatedAt:      now.Add(-2 * time.Minute),
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"2"`),
		},
	})
	if err != nil {
		t.Fatalf("Create second returned error: %v", err)
	}
	third, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		State:          deltaflow.StatePending,
		AvailableAt:    now.Add(-1 * time.Minute),
		CreatedAt:      now.Add(-1 * time.Minute),
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"3"`),
		},
	})
	if err != nil {
		t.Fatalf("Create third returned error: %v", err)
	}

	claimed, err := store.ClaimNextBatch(ctx, "sync", "worker-1", 2, time.Minute)
	if err != nil {
		t.Fatalf("ClaimNextBatch returned error: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("ClaimNextBatch len = %d, want 2", len(claimed))
	}
	if claimed[0].ID != first.ID || claimed[1].ID != second.ID {
		t.Fatalf("ClaimNextBatch order = [%s, %s], want [%s, %s]", claimed[0].ID, claimed[1].ID, first.ID, second.ID)
	}

	next, err := store.ClaimNextBatch(ctx, "sync", "worker-2", 2, time.Minute)
	if err != nil {
		t.Fatalf("second ClaimNextBatch returned error: %v", err)
	}
	if len(next) != 1 {
		t.Fatalf("second ClaimNextBatch len = %d, want 1", len(next))
	}
	if next[0].ID != third.ID {
		t.Fatalf("second ClaimNextBatch order = [%s], want [%s]", next[0].ID, third.ID)
	}
}

func TestJobMemoryStoreRenewLeaseAndOwnershipChecks(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	job, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	claimed, err := store.ClaimNext(ctx, "sync", "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimNext returned nil")
	}

	if err := store.RenewLease(ctx, job.ID, "worker-1", 2*time.Minute); err != nil {
		t.Fatalf("RenewLease returned error: %v", err)
	}
	got, ok, err := store.Get(ctx, job.ID)
	if err != nil || !ok {
		t.Fatalf("Get after renew = ok=%v err=%v", ok, err)
	}
	if got.LockedUntil == nil || !got.LockedUntil.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("locked_until = %v, want %v", got.LockedUntil, now.Add(2*time.Minute))
	}

	if err := store.MarkSynced(ctx, job.ID, "worker-2", false); !errors.Is(err, deltaflow.ErrJobLeaseNotOwned) {
		t.Fatalf("MarkSynced wrong owner error = %v, want %v", err, deltaflow.ErrJobLeaseNotOwned)
	}

	now = now.Add(3 * time.Minute)
	if err := store.RenewLease(ctx, job.ID, "worker-1", 2*time.Minute); !errors.Is(err, deltaflow.ErrJobLeaseNotOwned) {
		t.Fatalf("RenewLease after expiry error = %v, want %v", err, deltaflow.ErrJobLeaseNotOwned)
	}
}

func TestJobMemoryStoreClaimNextTelemetryInvalidLockFor(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	telemetry := &leaseTelemetrySpy{}
	store.LeaseTelemetry = telemetry

	_, err := store.ClaimNext(ctx, "sync", "worker-1", 0)
	if !errors.Is(err, deltaflow.ErrInvalidLockFor) {
		t.Fatalf("ClaimNext invalid lock error = %v, want %v", err, deltaflow.ErrInvalidLockFor)
	}
	if len(telemetry.claimResults) != 1 || telemetry.claimResults[0] != deltaflow.LeaseTelemetryResultInvalidLockFor {
		t.Fatalf("claim results = %#v, want [%s]", telemetry.claimResults, deltaflow.LeaseTelemetryResultInvalidLockFor)
	}
	if telemetry.reclaimCount != 0 {
		t.Fatalf("reclaim count = %d, want 0", telemetry.reclaimCount)
	}
	if len(telemetry.ownershipChecks) != 0 {
		t.Fatalf("ownership checks = %#v, want none", telemetry.ownershipChecks)
	}
}

func TestJobMemoryStoreClaimNextTelemetryEmpty(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	telemetry := &leaseTelemetrySpy{}
	store.LeaseTelemetry = telemetry

	claimed, err := store.ClaimNext(ctx, "sync", "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext empty returned error: %v", err)
	}
	if claimed != nil {
		t.Fatalf("ClaimNext empty returned %#v, want nil", claimed)
	}
	if len(telemetry.claimResults) != 1 || telemetry.claimResults[0] != deltaflow.LeaseTelemetryResultEmpty {
		t.Fatalf("claim results = %#v, want [%s]", telemetry.claimResults, deltaflow.LeaseTelemetryResultEmpty)
	}
	if telemetry.reclaimCount != 0 {
		t.Fatalf("reclaim count = %d, want 0", telemetry.reclaimCount)
	}
}

func TestJobMemoryStoreClaimNextTelemetryExpiredReclaim(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	telemetry := &leaseTelemetrySpy{}
	store.LeaseTelemetry = telemetry
	base := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base }

	lockedBy := "worker-1"
	expired := base.Add(-time.Minute)
	store.jobs["job-1"] = &deltaflow.SyncJob{
		ID:          "job-1",
		SyncID:      "sync",
		Origin:      deltaflow.JobOriginManual,
		State:       deltaflow.StateProcessing,
		LockedBy:    &lockedBy,
		LockedUntil: &expired,
		AvailableAt: base.Add(-2 * time.Minute),
		CreatedAt:   base.Add(-3 * time.Minute),
		UpdatedAt:   base.Add(-2 * time.Minute),
	}

	claimed, err := store.ClaimNext(ctx, "sync", "worker-2", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext expired reclaim returned error: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimNext expired reclaim returned nil")
	}
	if len(telemetry.claimResults) != 1 || telemetry.claimResults[0] != deltaflow.LeaseTelemetryResultSuccess {
		t.Fatalf("claim results = %#v, want [%s]", telemetry.claimResults, deltaflow.LeaseTelemetryResultSuccess)
	}
	if telemetry.reclaimCount != 1 {
		t.Fatalf("reclaim count = %d, want 1", telemetry.reclaimCount)
	}
}

func TestJobMemoryStoreOwnershipRejectedTelemetry(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	telemetry := &leaseTelemetrySpy{}
	store.LeaseTelemetry = telemetry
	base := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base }

	lockedBy := "worker-1"
	lockedUntil := base.Add(time.Minute)
	store.jobs["job-1"] = &deltaflow.SyncJob{
		ID:          "job-1",
		SyncID:      "sync",
		Origin:      deltaflow.JobOriginManual,
		State:       deltaflow.StateProcessing,
		LockedBy:    &lockedBy,
		LockedUntil: &lockedUntil,
		AvailableAt: base.Add(-time.Minute),
		CreatedAt:   base.Add(-2 * time.Minute),
		UpdatedAt:   base.Add(-time.Minute),
	}

	err := store.MarkSynced(ctx, "job-1", "worker-2", false)
	if !errors.Is(err, deltaflow.ErrJobLeaseNotOwned) {
		t.Fatalf("MarkSynced wrong owner error = %v, want %v", err, deltaflow.ErrJobLeaseNotOwned)
	}
	if len(telemetry.ownershipChecks) != 1 {
		t.Fatalf("ownership checks = %#v, want 1 entry", telemetry.ownershipChecks)
	}
	check := telemetry.ownershipChecks[0]
	if check.transition != deltaflow.LeaseTelemetryTransitionMarkSynced || check.result != deltaflow.LeaseTelemetryOwnershipRejected {
		t.Fatalf("ownership check = %#v, want %s/%s", check, deltaflow.LeaseTelemetryTransitionMarkSynced, deltaflow.LeaseTelemetryOwnershipRejected)
	}
}

func TestJobMemoryStoreRenewLeaseTelemetrySuccess(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	telemetry := &leaseTelemetrySpy{}
	store.LeaseTelemetry = telemetry
	base := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base }

	lockedBy := "worker-1"
	lockedUntil := base.Add(time.Minute)
	store.jobs["job-1"] = &deltaflow.SyncJob{
		ID:          "job-1",
		SyncID:      "sync",
		Origin:      deltaflow.JobOriginManual,
		State:       deltaflow.StateProcessing,
		LockedBy:    &lockedBy,
		LockedUntil: &lockedUntil,
		AvailableAt: base.Add(-time.Minute),
		CreatedAt:   base.Add(-2 * time.Minute),
		UpdatedAt:   base.Add(-time.Minute),
	}

	if err := store.RenewLease(ctx, "job-1", lockedBy, 2*time.Minute); err != nil {
		t.Fatalf("RenewLease returned error: %v", err)
	}

	if len(telemetry.renewResults) != 1 || telemetry.renewResults[0] != deltaflow.LeaseTelemetryResultSuccess {
		t.Fatalf("renew results = %#v, want [%s]", telemetry.renewResults, deltaflow.LeaseTelemetryResultSuccess)
	}
	if len(telemetry.renewDurations) != 1 {
		t.Fatalf("renew durations = %#v, want 1 entry", telemetry.renewDurations)
	}
	if telemetry.renewDurations[0] < 0 {
		t.Fatalf("renew duration = %v, want >= 0", telemetry.renewDurations[0])
	}
}

func TestJobMemoryStoreRenewLeaseTelemetryOwnershipRejected(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	telemetry := &leaseTelemetrySpy{}
	store.LeaseTelemetry = telemetry
	base := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base }

	lockedBy := "worker-1"
	lockedUntil := base.Add(time.Minute)
	store.jobs["job-1"] = &deltaflow.SyncJob{
		ID:          "job-1",
		SyncID:      "sync",
		Origin:      deltaflow.JobOriginManual,
		State:       deltaflow.StateProcessing,
		LockedBy:    &lockedBy,
		LockedUntil: &lockedUntil,
		AvailableAt: base.Add(-time.Minute),
		CreatedAt:   base.Add(-2 * time.Minute),
		UpdatedAt:   base.Add(-time.Minute),
	}

	err := store.RenewLease(ctx, "job-1", "worker-2", 2*time.Minute)
	if !errors.Is(err, deltaflow.ErrJobLeaseNotOwned) {
		t.Fatalf("RenewLease wrong owner error = %v, want %v", err, deltaflow.ErrJobLeaseNotOwned)
	}

	if len(telemetry.renewResults) != 1 || telemetry.renewResults[0] != deltaflow.LeaseTelemetryResultLeaseNotOwned {
		t.Fatalf("renew results = %#v, want [%s]", telemetry.renewResults, deltaflow.LeaseTelemetryResultLeaseNotOwned)
	}
	if len(telemetry.renewDurations) != 1 {
		t.Fatalf("renew durations = %#v, want 1 entry", telemetry.renewDurations)
	}
	if telemetry.renewDurations[0] < 0 {
		t.Fatalf("renew duration = %v, want >= 0", telemetry.renewDurations[0])
	}
}

func TestJobMemoryStoreRenewLeaseUsesSingleNowForOwnershipAndUpdate(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	base := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)

	jobID := deltaflow.SyncJobID("job-1")
	lockedBy := "worker-1"
	lockedUntil := base.Add(time.Millisecond)
	store.jobs[jobID] = &deltaflow.SyncJob{
		ID:          jobID,
		SyncID:      "sync",
		Origin:      deltaflow.JobOriginManual,
		State:       deltaflow.StateProcessing,
		LockedBy:    &lockedBy,
		LockedUntil: &lockedUntil,
		UpdatedAt:   base,
	}

	callCount := 0
	store.now = func() time.Time {
		callCount++
		if callCount == 1 {
			return base
		}
		return base.Add(2 * time.Millisecond)
	}

	if err := store.RenewLease(ctx, jobID, lockedBy, time.Minute); err != nil {
		t.Fatalf("RenewLease returned error: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("now() call count = %d, want 1", callCount)
	}

	got, ok, err := store.Get(ctx, jobID)
	if err != nil || !ok {
		t.Fatalf("Get after renew = ok=%v err=%v", ok, err)
	}
	wantLockedUntil := base.Add(time.Minute)
	if got.LockedUntil == nil || !got.LockedUntil.Equal(wantLockedUntil) {
		t.Fatalf("locked_until = %v, want %v", got.LockedUntil, wantLockedUntil)
	}
	if !got.UpdatedAt.Equal(base) {
		t.Fatalf("updated_at = %v, want %v", got.UpdatedAt, base)
	}
}

func TestJobMemoryStoreLeaseQueryHelpers(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	worker := "worker-1"
	nearUntil := now.Add(20 * time.Second)
	activeUntil := now.Add(10 * time.Minute)
	expiredUntil := now.Add(-10 * time.Second)

	store.jobs["near"] = &deltaflow.SyncJob{ID: "near", SyncID: "sync-a", State: deltaflow.StateProcessing, LockedBy: &worker, LockedUntil: &nearUntil, UpdatedAt: now.Add(-3 * time.Second)}
	store.jobs["active"] = &deltaflow.SyncJob{ID: "active", SyncID: "sync-a", State: deltaflow.StateProcessing, LockedBy: &worker, LockedUntil: &activeUntil, UpdatedAt: now.Add(-2 * time.Second)}
	store.jobs["expired"] = &deltaflow.SyncJob{ID: "expired", SyncID: "sync-a", State: deltaflow.StateProcessing, LockedBy: &worker, LockedUntil: &expiredUntil, UpdatedAt: now.Add(-time.Second)}
	store.jobs["nolock"] = &deltaflow.SyncJob{ID: "nolock", SyncID: "sync-a", State: deltaflow.StateProcessing, LockedBy: &worker, LockedUntil: nil, UpdatedAt: now}
	store.jobs["other-sync"] = &deltaflow.SyncJob{ID: "other-sync", SyncID: "sync-b", State: deltaflow.StateProcessing, LockedBy: &worker, LockedUntil: &nearUntil, UpdatedAt: now}
	store.jobs["pending"] = &deltaflow.SyncJob{ID: "pending", SyncID: "sync-a", State: deltaflow.StatePending, UpdatedAt: now}

	active, err := store.ListActiveLeases(ctx, "sync-a", 10)
	if err != nil {
		t.Fatalf("ListActiveLeases returned error: %v", err)
	}
	if len(active) != 2 || active[0].ID != "near" || active[1].ID != "active" {
		t.Fatalf("active leases = %#v, want near,active", leaseIDs(active))
	}

	expired, err := store.ListExpiredProcessingLeases(ctx, "sync-a", 10)
	if err != nil {
		t.Fatalf("ListExpiredProcessingLeases returned error: %v", err)
	}
	if len(expired) != 2 || expired[0].ID != "nolock" || expired[1].ID != "expired" {
		t.Fatalf("expired leases = %#v, want nolock,expired", leaseIDs(expired))
	}

	near, err := store.ListNearExpiryLeases(ctx, "sync-a", 30*time.Second, 10)
	if err != nil {
		t.Fatalf("ListNearExpiryLeases returned error: %v", err)
	}
	if len(near) != 1 || near[0].ID != "near" {
		t.Fatalf("near-expiry leases = %#v, want near", leaseIDs(near))
	}

	if _, err := store.ListNearExpiryLeases(ctx, "sync-a", -time.Second, 10); !errors.Is(err, deltaflow.ErrInvalidLeaseWindow) {
		t.Fatalf("negative window error = %v, want %v", err, deltaflow.ErrInvalidLeaseWindow)
	}
}

func TestJobMemoryStoreOperatorLeaseActions(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	worker := "worker-1"
	expired := now.Add(-10 * time.Second)
	active := now.Add(time.Minute)

	store.jobs["expired"] = &deltaflow.SyncJob{ID: "expired", SyncID: "sync", State: deltaflow.StateProcessing, LockedBy: &worker, LockedUntil: &expired, UpdatedAt: now.Add(-time.Minute)}
	store.jobs["active"] = &deltaflow.SyncJob{ID: "active", SyncID: "sync", State: deltaflow.StateProcessing, LockedBy: &worker, LockedUntil: &active, UpdatedAt: now.Add(-time.Minute)}

	if err := store.ForceReleaseExpiredLease(ctx, "expired", "manual recovery"); err != nil {
		t.Fatalf("ForceReleaseExpiredLease returned error: %v", err)
	}
	released, ok, err := store.Get(ctx, "expired")
	if err != nil || !ok {
		t.Fatalf("Get released = (%v, %v, %v)", released, ok, err)
	}
	if released.LockedBy != nil || released.LockedUntil != nil {
		t.Fatalf("released lock = (%v, %v), want nil,nil", released.LockedBy, released.LockedUntil)
	}
	if released.LastError == nil || *released.LastError != "operator_force_release: manual recovery" {
		t.Fatalf("released last_error = %v, want operator_force_release", released.LastError)
	}

	nextRun := now.Add(30 * time.Second)
	if err := store.RequeueExpiredLease(ctx, "expired", nextRun, "requeue for retry"); err != nil {
		t.Fatalf("RequeueExpiredLease returned error: %v", err)
	}
	requeued, ok, err := store.Get(ctx, "expired")
	if err != nil || !ok {
		t.Fatalf("Get requeued = (%v, %v, %v)", requeued, ok, err)
	}
	if requeued.State != deltaflow.StateRetrying {
		t.Fatalf("requeued state = %s, want %s", requeued.State, deltaflow.StateRetrying)
	}
	if !requeued.AvailableAt.Equal(nextRun) {
		t.Fatalf("requeued available_at = %v, want %v", requeued.AvailableAt, nextRun)
	}
	if requeued.LastError == nil || *requeued.LastError != "operator_requeue: requeue for retry" {
		t.Fatalf("requeued last_error = %v, want operator_requeue", requeued.LastError)
	}

	if err := store.ForceReleaseExpiredLease(ctx, "active", "manual release"); !errors.Is(err, deltaflow.ErrJobLeaseNotExpired) {
		t.Fatalf("ForceReleaseExpiredLease active lease error = %v, want %v", err, deltaflow.ErrJobLeaseNotExpired)
	}
	if err := store.RequeueExpiredLease(ctx, "active", nextRun, "manual requeue"); !errors.Is(err, deltaflow.ErrJobLeaseNotExpired) {
		t.Fatalf("RequeueExpiredLease active lease error = %v, want %v", err, deltaflow.ErrJobLeaseNotExpired)
	}
	if err := store.ForceReleaseExpiredLease(ctx, "expired", "   "); !errors.Is(err, deltaflow.ErrAuditReasonRequired) {
		t.Fatalf("ForceReleaseExpiredLease empty reason error = %v, want %v", err, deltaflow.ErrAuditReasonRequired)
	}
}

func TestJobMemoryStoreRequeueClaimedPreservesAttemptCount(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	worker := "worker-1"
	lockedUntil := now.Add(time.Minute)
	store.jobs["job-1"] = &deltaflow.SyncJob{
		ID:           "job-1",
		SyncID:       "sync",
		Origin:       deltaflow.JobOriginManual,
		State:        deltaflow.StateProcessing,
		AttemptCount: 2,
		LockedBy:     &worker,
		LockedUntil:  &lockedUntil,
		AvailableAt:  now.Add(-time.Minute),
		CreatedAt:    now.Add(-2 * time.Minute),
		UpdatedAt:    now.Add(-time.Minute),
	}

	nextRunAt := now.Add(30 * time.Second)
	if err := store.RequeueClaimed(ctx, "job-1", worker, errors.New("batch canceled"), nextRunAt); err != nil {
		t.Fatalf("RequeueClaimed returned error: %v", err)
	}

	got, ok, err := store.Get(ctx, "job-1")
	if err != nil || !ok {
		t.Fatalf("Get after RequeueClaimed = ok=%v err=%v", ok, err)
	}
	if got.State != deltaflow.StateRetrying {
		t.Fatalf("state = %s, want %s", got.State, deltaflow.StateRetrying)
	}
	if got.AttemptCount != 2 {
		t.Fatalf("attempt_count = %d, want 2", got.AttemptCount)
	}
	if !got.AvailableAt.Equal(nextRunAt) {
		t.Fatalf("available_at = %v, want %v", got.AvailableAt, nextRunAt)
	}
	if got.LastError == nil || *got.LastError != "batch canceled" {
		t.Fatalf("last_error = %v, want %q", got.LastError, "batch canceled")
	}
	if got.LockedBy != nil || got.LockedUntil != nil {
		t.Fatal("lock was not cleared after RequeueClaimed")
	}
}

func TestJobMemoryStoreCreateRejectsOutboxJobWithoutDeltaID(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()

	_, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:         "sync",
		Origin:         deltaflow.JobOriginOutbox,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
		},
	})
	if !errors.Is(err, deltaflow.ErrOutboxJobNeedsDelta) {
		t.Fatalf("Create error = %v, want %v", err, deltaflow.ErrOutboxJobNeedsDelta)
	}
}

func TestJobMemoryStoreCreateInvalidOutboxJobDoesNotConsumeAutoID(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()

	_, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID: "sync",
		Origin: deltaflow.JobOriginOutbox,
	})
	if !errors.Is(err, deltaflow.ErrOutboxJobNeedsDelta) {
		t.Fatalf("Create invalid outbox job error = %v, want %v", err, deltaflow.ErrOutboxJobNeedsDelta)
	}

	inserted, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID: "sync",
		Origin: deltaflow.JobOriginManual,
	})
	if err != nil {
		t.Fatalf("Create autogenerated job returned error: %v", err)
	}
	if inserted.ID != "job-1" {
		t.Fatalf("inserted ID = %s, want %s", inserted.ID, "job-1")
	}
}

func TestJobMemoryStoreCreateDuplicateMappingDoesNotConsumeAutoID(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	deltaID := deltaflow.DeltaID("delta-1")

	_, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:  "sync",
		DeltaID: cloneDeltaIDPtr(&deltaID),
		Origin:  deltaflow.JobOriginOutbox,
	})
	if err != nil {
		t.Fatalf("Create mapped job returned error: %v", err)
	}

	_, err = store.Create(ctx, deltaflow.SyncJob{
		SyncID:  "sync",
		DeltaID: cloneDeltaIDPtr(&deltaID),
		Origin:  deltaflow.JobOriginOutbox,
	})
	if !errors.Is(err, deltaflow.ErrDeltaAlreadyMapped) {
		t.Fatalf("Create duplicate mapping error = %v, want %v", err, deltaflow.ErrDeltaAlreadyMapped)
	}

	inserted, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID: "sync",
		Origin: deltaflow.JobOriginManual,
	})
	if err != nil {
		t.Fatalf("Create autogenerated job returned error: %v", err)
	}
	if inserted.ID != "job-2" {
		t.Fatalf("inserted ID = %s, want %s", inserted.ID, "job-2")
	}
}

func TestJobMemoryStoreCreateAllowsManualJobSharingOutboxDeltaID(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()
	deltaID := deltaflow.DeltaID("delta-1")

	outboxJob, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:  "sync",
		DeltaID: cloneDeltaIDPtr(&deltaID),
		Origin:  deltaflow.JobOriginOutbox,
	})
	if err != nil {
		t.Fatalf("Create outbox job returned error: %v", err)
	}

	manualJob, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID:  "sync",
		DeltaID: cloneDeltaIDPtr(&deltaID),
		Origin:  deltaflow.JobOriginManual,
	})
	if err != nil {
		t.Fatalf("Create manual job returned error: %v", err)
	}
	if manualJob.DeltaID == nil || *manualJob.DeltaID != deltaID {
		t.Fatalf("manual job delta_id = %v, want %s", manualJob.DeltaID, deltaID)
	}
	if mappedID, ok := store.jobByDelta[deltaID]; !ok {
		t.Fatalf("jobByDelta missing mapping for %s", deltaID)
	} else if mappedID != outboxJob.ID {
		t.Fatalf("jobByDelta[%s] = %s, want %s", deltaID, mappedID, outboxJob.ID)
	}
}

func TestJobMemoryStoreCreateAutoIDSkipsExistingGeneratedIDs(t *testing.T) {
	ctx := context.Background()
	store := NewJobMemoryStore()

	store.jobs["job-1"] = &deltaflow.SyncJob{ID: "job-1", SyncID: "sync", Origin: deltaflow.JobOriginManual}

	inserted, err := store.Create(ctx, deltaflow.SyncJob{
		SyncID: "sync",
		Origin: deltaflow.JobOriginManual,
	})
	if err != nil {
		t.Fatalf("Create autogenerated ID returned error: %v", err)
	}
	if inserted.ID != "job-2" {
		t.Fatalf("inserted ID = %s, want %s", inserted.ID, "job-2")
	}
}

func TestMemoryDispatchStoreDispatchPendingSkipsOccupiedGeneratedIDs(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	dispatcher := NewMemoryDispatchStore(deltaStore, jobStore, nil)

	first := enqueueDeltaForDispatch(t, ctx, deltaStore, "delta-1")
	second := enqueueDeltaForDispatch(t, ctx, deltaStore, "delta-2")

	jobStore.jobs["job-1"] = &deltaflow.SyncJob{ID: "job-1", SyncID: "sync", Origin: deltaflow.JobOriginOutbox}

	jobs, err := dispatcher.DispatchPending(ctx, "sync", 10)
	if err != nil {
		t.Fatalf("DispatchPending returned error: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("jobs len = %d, want 2", len(jobs))
	}
	if jobs[0].ID != "job-2" || jobs[1].ID != "job-3" {
		t.Fatalf("generated job IDs = [%s, %s], want [job-2, job-3]", jobs[0].ID, jobs[1].ID)
	}

	firstGot, ok, err := deltaStore.Get(ctx, first.ID)
	if err != nil || !ok {
		t.Fatalf("Get first delta = (%v, %v, %v)", firstGot, ok, err)
	}
	if firstGot.State != deltaflow.DeltaDispatched {
		t.Fatalf("first state = %s, want %s", firstGot.State, deltaflow.DeltaDispatched)
	}

	secondGot, ok, err := deltaStore.Get(ctx, second.ID)
	if err != nil || !ok {
		t.Fatalf("Get second delta = (%v, %v, %v)", secondGot, ok, err)
	}
	if secondGot.State != deltaflow.DeltaDispatched {
		t.Fatalf("second state = %s, want %s", secondGot.State, deltaflow.DeltaDispatched)
	}

	if len(jobStore.jobByDelta) != 2 {
		t.Fatalf("jobByDelta len = %d, want 2", len(jobStore.jobByDelta))
	}
}

func TestMemoryDispatchStoreDispatchPendingCarriesComputedProjectionKeyHash(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	dispatcher := NewMemoryDispatchStore(deltaStore, jobStore, nil)

	inserted := enqueueDeltaForDispatch(t, ctx, deltaStore, "delta-hash")
	if inserted.ProjectionKeyHash == "" {
		t.Fatal("enqueued delta projection_key_hash is empty")
	}

	jobs, err := dispatcher.DispatchPending(ctx, "sync", 10)
	if err != nil {
		t.Fatalf("DispatchPending returned error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs len = %d, want 1", len(jobs))
	}
	if jobs[0].ProjectionKeyHash == "" {
		t.Fatal("dispatched job projection_key_hash is empty")
	}
	if jobs[0].ProjectionKeyHash != inserted.ProjectionKeyHash {
		t.Fatalf("job projection_key_hash = %s, want %s", jobs[0].ProjectionKeyHash, inserted.ProjectionKeyHash)
	}
}

func TestMemoryDispatchStoreIgnoresAlreadyMappedDelta(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	dispatcher := NewMemoryDispatchStore(deltaStore, jobStore, nil)

	mapped := enqueueDeltaForDispatch(t, ctx, deltaStore, "delta-mapped")
	fresh := enqueueDeltaForDispatch(t, ctx, deltaStore, "delta-fresh")

	_, err := jobStore.Create(ctx, deltaflow.SyncJob{
		SyncID:      mapped.SyncID,
		DeltaID:     cloneDeltaIDPtr(&mapped.ID),
		Origin:      deltaflow.JobOriginOutbox,
		State:       deltaflow.StatePending,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("Create mapped job returned error: %v", err)
	}

	jobs, err := dispatcher.DispatchPending(ctx, "sync", 10)
	if err != nil {
		t.Fatalf("DispatchPending returned error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs len = %d, want 1", len(jobs))
	}
	if jobs[0].DeltaID == nil || *jobs[0].DeltaID != fresh.ID {
		t.Fatalf("job delta_id = %v, want %s", jobs[0].DeltaID, fresh.ID)
	}

	mappedGot, ok, err := deltaStore.Get(ctx, mapped.ID)
	if err != nil || !ok {
		t.Fatalf("Get mapped delta = (%v, %v, %v)", mappedGot, ok, err)
	}
	if mappedGot.State != deltaflow.DeltaDispatched {
		t.Fatalf("mapped state = %s, want %s", mappedGot.State, deltaflow.DeltaDispatched)
	}

	freshGot, ok, err := deltaStore.Get(ctx, fresh.ID)
	if err != nil || !ok {
		t.Fatalf("Get fresh delta = (%v, %v, %v)", freshGot, ok, err)
	}
	if freshGot.State != deltaflow.DeltaDispatched {
		t.Fatalf("fresh state = %s, want %s", freshGot.State, deltaflow.DeltaDispatched)
	}
}

func TestMemoryDispatchStoreIgnoresMappedDeltaWithOccupiedGeneratedID(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	dispatcher := NewMemoryDispatchStore(deltaStore, jobStore, nil)

	mapped := enqueueDeltaForDispatch(t, ctx, deltaStore, "delta-mapped")
	fresh := enqueueDeltaForDispatch(t, ctx, deltaStore, "delta-fresh")

	_, err := jobStore.Create(ctx, deltaflow.SyncJob{
		SyncID:      mapped.SyncID,
		DeltaID:     cloneDeltaIDPtr(&mapped.ID),
		Origin:      deltaflow.JobOriginOutbox,
		State:       deltaflow.StatePending,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("Create mapped job returned error: %v", err)
	}

	jobStore.jobs["job-1"] = &deltaflow.SyncJob{ID: "job-1", SyncID: "sync", Origin: deltaflow.JobOriginOutbox}

	jobs, err := dispatcher.DispatchPending(ctx, "sync", 10)
	if err != nil {
		t.Fatalf("DispatchPending returned error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs len = %d, want 1", len(jobs))
	}
	if jobs[0].ID != "job-2" {
		t.Fatalf("generated job ID = %s, want %s", jobs[0].ID, "job-2")
	}
	if jobs[0].DeltaID == nil || *jobs[0].DeltaID != fresh.ID {
		t.Fatalf("job delta_id = %v, want %s", jobs[0].DeltaID, fresh.ID)
	}

	mappedGot, ok, err := deltaStore.Get(ctx, mapped.ID)
	if err != nil || !ok {
		t.Fatalf("Get mapped delta = (%v, %v, %v)", mappedGot, ok, err)
	}
	if mappedGot.State != deltaflow.DeltaDispatched {
		t.Fatalf("mapped state = %s, want %s", mappedGot.State, deltaflow.DeltaDispatched)
	}

	freshGot, ok, err := deltaStore.Get(ctx, fresh.ID)
	if err != nil || !ok {
		t.Fatalf("Get fresh delta = (%v, %v, %v)", freshGot, ok, err)
	}
	if freshGot.State != deltaflow.DeltaDispatched {
		t.Fatalf("fresh state = %s, want %s", freshGot.State, deltaflow.DeltaDispatched)
	}
}

func TestMemoryDispatchStoreManualJobWithDeltaIDDoesNotBlockDispatch(t *testing.T) {
	ctx := context.Background()
	deltaStore := NewDeltaMemoryStore()
	jobStore := NewJobMemoryStore()
	dispatcher := NewMemoryDispatchStore(deltaStore, jobStore, nil)

	pending := enqueueDeltaForDispatch(t, ctx, deltaStore, "delta-1")

	_, err := jobStore.Create(ctx, deltaflow.SyncJob{
		SyncID:      pending.SyncID,
		DeltaID:     cloneDeltaIDPtr(&pending.ID),
		Origin:      deltaflow.JobOriginManual,
		State:       deltaflow.StatePending,
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("Create manual job returned error: %v", err)
	}
	if _, exists := jobStore.jobByDelta[pending.ID]; exists {
		t.Fatalf("jobByDelta unexpectedly contains manual mapping for %s", pending.ID)
	}

	jobs, err := dispatcher.DispatchPending(ctx, "sync", 10)
	if err != nil {
		t.Fatalf("DispatchPending returned error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs len = %d, want 1", len(jobs))
	}
	if jobs[0].DeltaID == nil || *jobs[0].DeltaID != pending.ID {
		t.Fatalf("job delta_id = %v, want %s", jobs[0].DeltaID, pending.ID)
	}

	if mappedID, ok := jobStore.jobByDelta[pending.ID]; !ok {
		t.Fatalf("jobByDelta missing mapping for %s", pending.ID)
	} else if mappedID != jobs[0].ID {
		t.Fatalf("jobByDelta[%s] = %s, want %s", pending.ID, mappedID, jobs[0].ID)
	}
}

func enqueueDeltaForDispatch(t *testing.T, ctx context.Context, store *DeltaMemoryStore, contactID string) *deltaflow.Delta {
	t.Helper()

	delta, err := store.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "sync",
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"` + contactID + `"`),
		},
	})
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	return delta
}

func leaseIDs(jobs []*deltaflow.SyncJob) []deltaflow.SyncJobID {
	ids := make([]deltaflow.SyncJobID, 0, len(jobs))
	for _, job := range jobs {
		if job == nil {
			continue
		}
		ids = append(ids, job.ID)
	}
	return ids
}

type leaseTelemetryOwnershipCheck struct {
	transition string
	result     string
}

type leaseTelemetrySpy struct {
	claimResults    []string
	renewResults    []string
	renewDurations  []time.Duration
	ownershipChecks []leaseTelemetryOwnershipCheck
	reclaimCount    int
}

func (s *leaseTelemetrySpy) ObserveLeaseClaim(result string) {
	s.claimResults = append(s.claimResults, result)
}

func (s *leaseTelemetrySpy) ObserveLeaseRenew(result string, duration time.Duration) {
	s.renewResults = append(s.renewResults, result)
	s.renewDurations = append(s.renewDurations, duration)
}

func (s *leaseTelemetrySpy) ObserveLeaseOwnershipCheck(transition string, result string) {
	s.ownershipChecks = append(s.ownershipChecks, leaseTelemetryOwnershipCheck{
		transition: transition,
		result:     result,
	})
}

func (s *leaseTelemetrySpy) ObserveLeaseReclaim() {
	s.reclaimCount++
}
