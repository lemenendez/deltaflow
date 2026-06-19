package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lemenendez/deltaflow/internal"
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

	cfg := &BuildConfig{
		Store: BuildStoreConfig{Type: "postgres", DSN: "postgres://unused"},
		Pipelines: []BuildPipelineConfig{
			{
				Name:   "contacts",
				SyncID: "contacts-sync",
				Source: BuildSourceConfig{Type: "postgres-outbox", ProjectionType: "contact"},
				Projector: BuildProjectorConfig{
					Name: "contact-projector",
				},
				Target:  BuildTargetConfig{Type: "elasticsearch", Index: "contacts"},
				Applier: BuildApplierConfig{Mode: "upsert"},
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

func TestBuildFromConfigRequiresDispatcherForOutboxSource(t *testing.T) {
	jobStore := internal.NewJobMemoryStore()

	registry := NewRegistry()
	registry.RegisterProjector("contact-projector", func(context.Context, PipelineSpec) (deltaflow.Projector, error) {
		return deltaflow.ProjectorFunc(func(_ context.Context, id deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: id, Payload: []byte(`{"ok":true}`), MediaType: "application/json"}, nil
		}), nil
	})
	registry.RegisterApplier("elasticsearch", func(context.Context, PipelineSpec) (deltaflow.ProjectionApplier, error) {
		return deltaflow.ProjectionApplierFunc(func(_ context.Context, _ deltaflow.ProjectionOperation) error {
			return nil
		}), nil
	})

	cfg := &BuildConfig{
		Store: BuildStoreConfig{Type: "postgres", DSN: "postgres://unused"},
		Pipelines: []BuildPipelineConfig{
			{
				Name:   "contacts",
				SyncID: "contacts-sync",
				Source: BuildSourceConfig{Type: "postgres-outbox", ProjectionType: "contact"},
				Projector: BuildProjectorConfig{
					Name: "contact-projector",
				},
				Target:  BuildTargetConfig{Type: "elasticsearch", Index: "contacts"},
				Applier: BuildApplierConfig{Mode: "upsert"},
			},
		},
	}

	_, err := BuildFromConfig(context.Background(), cfg, registry, WorkerDeps{
		JobStore: jobStore,
		WorkerID: "worker-test",
		LockFor:  5 * time.Second,
		PullSize: 1,
	})
	if err == nil {
		t.Fatal("BuildFromConfig error = nil")
	}
	if !strings.Contains(err.Error(), "runtime dispatcher is required for source.type=postgres-outbox") {
		t.Fatalf("error = %v", err)
	}
}

func TestBuildFromConfigCarriesSourceProjectionTypeIntoSpec(t *testing.T) {
	jobStore := internal.NewJobMemoryStore()
	deltaStore := internal.NewDeltaMemoryStore()
	dispatch := internal.NewMemoryDispatchStore(deltaStore, jobStore, nil)

	var projectorSpec PipelineSpec
	var applierSpec PipelineSpec

	registry := NewRegistry()
	registry.RegisterProjector("contact-projector", func(_ context.Context, spec PipelineSpec) (deltaflow.Projector, error) {
		projectorSpec = spec
		return deltaflow.ProjectorFunc(func(_ context.Context, id deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{Identity: id, Payload: []byte(`{"ok":true}`), MediaType: "application/json"}, nil
		}), nil
	})
	registry.RegisterApplier("elasticsearch", func(_ context.Context, spec PipelineSpec) (deltaflow.ProjectionApplier, error) {
		applierSpec = spec
		return deltaflow.ProjectionApplierFunc(func(_ context.Context, _ deltaflow.ProjectionOperation) error { return nil }), nil
	})

	cfg := &BuildConfig{
		Store: BuildStoreConfig{Type: "postgres", DSN: "postgres://unused"},
		Pipelines: []BuildPipelineConfig{
			{
				Name:   "contacts",
				SyncID: "contacts-sync",
				Source: BuildSourceConfig{Type: "postgres-outbox", ProjectionType: "contact"},
				Projector: BuildProjectorConfig{
					Name: "contact-projector",
				},
				Target:  BuildTargetConfig{Type: "elasticsearch", Index: "contacts"},
				Applier: BuildApplierConfig{Mode: "upsert"},
			},
		},
	}

	_, err := BuildFromConfig(context.Background(), cfg, registry, WorkerDeps{
		JobStore:   jobStore,
		Dispatcher: dispatch,
		WorkerID:   "worker-test",
		LockFor:    5 * time.Second,
		PullSize:   1,
	})
	if err != nil {
		t.Fatalf("BuildFromConfig error: %v", err)
	}

	if projectorSpec.SourceProjectionType != "contact" {
		t.Fatalf("projector spec source projection type = %q, want %q", projectorSpec.SourceProjectionType, "contact")
	}
	if applierSpec.SourceProjectionType != "contact" {
		t.Fatalf("applier spec source projection type = %q, want %q", applierSpec.SourceProjectionType, "contact")
	}
}
