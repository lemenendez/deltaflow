package deltaflow_test

import (
	"encoding/json"
	"errors"
	"testing"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestNewBackfillDeltaBuildsReadyToEnqueueDelta(t *testing.T) {
	identity := deltaflow.ProjectionIdentity{
		Type: "Customer",
		Key: deltaflow.ProjectionKey{
			"customer_id": json.RawMessage(`"42"`),
		},
	}

	delta, err := deltaflow.NewBackfillDelta("customers-sync", deltaflow.OriginOperationUpdated, identity, "customers-2026")
	if err != nil {
		t.Fatalf("NewBackfillDelta returned error: %v", err)
	}

	if delta.SyncID != "customers-sync" {
		t.Fatalf("SyncID = %q, want customers-sync", delta.SyncID)
	}
	if delta.Origin != deltaflow.OriginOperationUpdated {
		t.Fatalf("Origin = %q, want %q", delta.Origin, deltaflow.OriginOperationUpdated)
	}
	if delta.ProjectionType != identity.Type {
		t.Fatalf("ProjectionType = %q, want %q", delta.ProjectionType, identity.Type)
	}
	if delta.DedupWindow != "customers-2026" {
		t.Fatalf("DedupWindow = %q, want customers-2026", delta.DedupWindow)
	}
	if got := string(delta.ProjectionKey["customer_id"]); got != `"42"` {
		t.Fatalf("ProjectionKey customer_id = %s, want \"42\"", got)
	}

	identity.Key["customer_id"] = json.RawMessage(`"mutated"`)
	if got := string(delta.ProjectionKey["customer_id"]); got != `"42"` {
		t.Fatalf("ProjectionKey should be cloned, got %s after caller mutation", got)
	}
	if delta.ID != "" || delta.DedupKey != "" || !delta.OccurredAt.IsZero() || !delta.CreatedAt.IsZero() {
		t.Fatalf("delta should remain store-normalized only, got %#v", delta)
	}
	if delta.State != "" {
		t.Fatalf("State = %q, want zero value before enqueue", delta.State)
	}
	if delta.ProjectionKeyHash != "" {
		t.Fatalf("ProjectionKeyHash = %q, want zero value before enqueue", delta.ProjectionKeyHash)
	}
	if delta.Metadata != nil {
		t.Fatalf("Metadata = %#v, want nil", delta.Metadata)
	}
	if delta.DispatchedAt != nil {
		t.Fatal("DispatchedAt should be nil")
	}
}

func TestNewBackfillDeltaValidatesRequiredFields(t *testing.T) {
	identity := deltaflow.ProjectionIdentity{
		Type: "Customer",
		Key:  deltaflow.ProjectionKey{"customer_id": json.RawMessage(`"42"`)},
	}

	tests := []struct {
		name string
		do   func() (deltaflow.Delta, error)
		want error
	}{
		{
			name: "missing sync id",
			do: func() (deltaflow.Delta, error) {
				return deltaflow.NewBackfillDelta("", deltaflow.OriginOperationUpdated, identity, "customers-2026")
			},
			want: deltaflow.ErrSyncIDRequired,
		},
		{
			name: "missing origin",
			do: func() (deltaflow.Delta, error) {
				return deltaflow.NewBackfillDelta("customers-sync", "", identity, "customers-2026")
			},
			want: deltaflow.ErrOriginRequired,
		},
		{
			name: "missing projection type",
			do: func() (deltaflow.Delta, error) {
				return deltaflow.NewBackfillDelta("customers-sync", deltaflow.OriginOperationUpdated, deltaflow.ProjectionIdentity{Key: identity.Key}, "customers-2026")
			},
			want: deltaflow.ErrProjectionTypeRequired,
		},
		{
			name: "missing projection key",
			do: func() (deltaflow.Delta, error) {
				return deltaflow.NewBackfillDelta("customers-sync", deltaflow.OriginOperationUpdated, deltaflow.ProjectionIdentity{Type: "Customer"}, "customers-2026")
			},
			want: deltaflow.ErrProjectionKeyRequired,
		},
		{
			name: "missing dedup window",
			do: func() (deltaflow.Delta, error) {
				return deltaflow.NewBackfillDelta("customers-sync", deltaflow.OriginOperationUpdated, identity, "")
			},
			want: deltaflow.ErrDedupWindowRequired,
		},
		{
			name: "nil projection key",
			do: func() (deltaflow.Delta, error) {
				return deltaflow.NewBackfillDelta("customers-sync", deltaflow.OriginOperationUpdated, deltaflow.ProjectionIdentity{Type: "Customer", Key: nil}, "customers-2026")
			},
			want: deltaflow.ErrProjectionKeyRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.do()
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}
