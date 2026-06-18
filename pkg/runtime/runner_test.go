package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/lemenendez/deltaflow/internal"
	"github.com/lemenendez/deltaflow/internal/config"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestRunOnceBuildsAndExecutesPipelines(t *testing.T) {
	deltaStore := internal.NewDeltaMemoryStore()
	jobStore := internal.NewJobMemoryStore()
	dispatch := internal.NewMemoryDispatchStore(deltaStore, jobStore, nil)

	projectionKey := deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"c-1"`)}
	_, err := deltaStore.Enqueue(context.Background(), deltaflow.Delta{
		SyncID:         deltaflow.SyncID("contacts-sync"),
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: deltaflow.ProjectionType("contact"),
		ProjectionKey:  projectionKey,
	})
	if err != nil {
		t.Fatalf("Enqueue error: %v", err)
	}

	registry := NewRegistry()
	registry.RegisterProjector("contact-projector", func(context.Context, PipelineSpec) (deltaflow.Projector, error) {
		return deltaflow.ProjectorFunc(func(_ context.Context, id deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: id, Payload: []byte(`{"ok":true}`), MediaType: "application/json"}, nil
		}), nil
	})

	applied := 0
	registry.RegisterApplier("elasticsearch", func(context.Context, PipelineSpec) (deltaflow.ProjectionApplier, error) {
		return deltaflow.ProjectionApplierFunc(func(_ context.Context, op deltaflow.ProjectionOperation) error {
			applied++
			if op.Type != deltaflow.ProjectionOpUpsert {
				t.Fatalf("op.Type = %q", op.Type)
			}
			return nil
		}), nil
	})

	cfg := &config.Config{
		Store: config.StoreConfig{Type: "postgres", DSN: "postgres://unused"},
		Workers: config.WorkersConfig{
			Concurrency: 1,
			LeaseTTL:    "30s",
		},
		Pipelines: []config.PipelineConfig{
			{
				Name:   "contacts",
				SyncID: "contacts-sync",
				Source: config.SourceConfig{Type: "postgres-outbox", ProjectionType: "contact"},
				Projector: config.ProjectorConfig{
					Name: "contact-projector",
				},
				Target:  config.TargetConfig{Type: "elasticsearch", Index: "contacts"},
				Applier: config.ApplierConfig{Mode: "upsert"},
			},
		},
	}

	built, err := BuildFromConfig(context.Background(), cfg, registry, WorkerDeps{
		JobStore:   jobStore,
		Dispatcher: dispatch,
		WorkerID:   "worker-test",
		LockFor:    5 * time.Second,
		PullSize:   1,
	})
	if err != nil {
		t.Fatalf("BuildFromConfig error: %v", err)
	}

	if err := RunOnce(context.Background(), built); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}
	if applied != 1 {
		t.Fatalf("applied = %d, want 1", applied)
	}
}
