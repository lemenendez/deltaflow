package runtime

import (
	"context"
	"errors"
	"testing"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestRegistryResolvePipelineRequiresRegisteredFactories(t *testing.T) {
	r := NewRegistry()
	_, err := r.ResolvePipeline(context.Background(), PipelineSpec{
		Name:          "contacts",
		SyncID:        deltaflow.SyncID("contacts"),
		ProjectorName: "contact-projector",
		TargetType:    "elasticsearch",
	})
	if err == nil {
		t.Fatal("ResolvePipeline error = nil")
	}
	if !errors.Is(err, ErrProjectorNotRegistered) {
		t.Fatalf("error = %v", err)
	}
}

func TestRegistryResolvePipelineBuildsRuntime(t *testing.T) {
	r := NewRegistry()

	if err := r.RegisterProjector("contact-projector", func(context.Context, PipelineSpec) (deltaflow.Projector, error) {
		return deltaflow.ProjectorFunc(func(context.Context, deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{}, nil
		}), nil
	}); err != nil {
		t.Fatalf("RegisterProjector error: %v", err)
	}
	if err := r.RegisterApplier("elasticsearch", func(context.Context, PipelineSpec) (deltaflow.ProjectionApplier, error) {
		return deltaflow.ProjectionApplierFunc(func(context.Context, deltaflow.ProjectionOperation) error { return nil }), nil
	}); err != nil {
		t.Fatalf("RegisterApplier error: %v", err)
	}

	resolved, err := r.ResolvePipeline(context.Background(), PipelineSpec{
		Name:          "contacts",
		SyncID:        deltaflow.SyncID("contacts"),
		ProjectorName: "contact-projector",
		TargetType:    "elasticsearch",
	})
	if err != nil {
		t.Fatalf("ResolvePipeline error: %v", err)
	}
	if resolved.Projector == nil {
		t.Fatal("Projector = nil")
	}
	if resolved.Applier == nil {
		t.Fatal("Applier = nil")
	}
}
