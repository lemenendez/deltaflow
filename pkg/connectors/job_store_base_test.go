package connectors

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestJobStoreBasePrepareJobForCreateComputesProjectionKeyHash(t *testing.T) {
	base := NewJobStoreBase(nil, JobStoreBaseConfig{
		Now: func() time.Time { return time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC) },
	})

	prepared, err := base.PrepareJobForCreate(deltaflow.SyncJob{
		SyncID:         "sync-1",
		ProjectionType: "Contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
			"tenant_id":  json.RawMessage(`"t1"`),
		},
	})
	if err != nil {
		t.Fatalf("PrepareJobForCreate returned error: %v", err)
	}

	if prepared.ProjectionKeyHash == "" {
		t.Fatal("projection_key_hash is empty")
	}

	encoded, err := json.Marshal(prepared.ProjectionKey)
	if err != nil {
		t.Fatalf("Marshal projection key returned error: %v", err)
	}
	sum := sha256.Sum256(encoded)
	want := deltaflow.ProjectionKeyHash(hex.EncodeToString(sum[:]))
	if prepared.ProjectionKeyHash != want {
		t.Fatalf("projection_key_hash = %s, want %s", prepared.ProjectionKeyHash, want)
	}
}

func TestJobStoreBasePrepareJobForCreateOverridesCallerProjectionKeyHash(t *testing.T) {
	base := NewJobStoreBase(nil, JobStoreBaseConfig{})

	prepared, err := base.PrepareJobForCreate(deltaflow.SyncJob{
		SyncID:            "sync-1",
		ProjectionType:    "Contact",
		ProjectionKeyHash: "stale-hash",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"1"`),
			"tenant_id":  json.RawMessage(`"t1"`),
		},
	})
	if err != nil {
		t.Fatalf("PrepareJobForCreate returned error: %v", err)
	}

	encoded, err := json.Marshal(prepared.ProjectionKey)
	if err != nil {
		t.Fatalf("Marshal projection key returned error: %v", err)
	}
	sum := sha256.Sum256(encoded)
	want := deltaflow.ProjectionKeyHash(hex.EncodeToString(sum[:]))
	if prepared.ProjectionKeyHash != want {
		t.Fatalf("projection_key_hash = %s, want %s", prepared.ProjectionKeyHash, want)
	}
	if prepared.ProjectionKeyHash == "stale-hash" {
		t.Fatal("projection_key_hash preserved caller-supplied stale value")
	}
}
