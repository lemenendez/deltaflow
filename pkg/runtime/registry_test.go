package runtime

import (
	"context"
	"errors"
	"strings"
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

	r.RegisterProjector("contact-projector", func(context.Context, PipelineSpec) (deltaflow.Projector, error) {
		return deltaflow.ProjectorFunc(func(context.Context, deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{}, nil
		}), nil
	})
	r.RegisterApplier("elasticsearch", func(context.Context, PipelineSpec) (deltaflow.ProjectionApplier, error) {
		return deltaflow.ProjectionApplierFunc(func(context.Context, deltaflow.ProjectionOperation) error { return nil }), nil
	})

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

func TestRegistryLookupsAreExact(t *testing.T) {
	r := NewRegistry()

	r.RegisterProjector("contact-projector", func(context.Context, PipelineSpec) (deltaflow.Projector, error) {
		return deltaflow.ProjectorFunc(func(context.Context, deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{}, nil
		}), nil
	})
	r.RegisterApplier("elasticsearch", func(context.Context, PipelineSpec) (deltaflow.ProjectionApplier, error) {
		return deltaflow.ProjectionApplierFunc(func(context.Context, deltaflow.ProjectionOperation) error { return nil }), nil
	})

	if !r.HasProjector("contact-projector") {
		t.Fatal("HasProjector exact match = false, want true")
	}
	if r.HasProjector("Contact-Projector") {
		t.Fatal("HasProjector case-mismatched lookup = true, want false")
	}
	if r.HasProjector(" contact-projector ") {
		t.Fatal("HasProjector whitespace-mismatched lookup = true, want false")
	}
	if !r.HasApplier("elasticsearch") {
		t.Fatal("HasApplier exact match = false, want true")
	}
	if r.HasApplier("ElasticSearch") {
		t.Fatal("HasApplier case-mismatched lookup = true, want false")
	}
	if r.HasApplier(" elasticsearch ") {
		t.Fatal("HasApplier whitespace-mismatched lookup = true, want false")
	}
}

func TestRegistryRegisterProjectorPanicsOnDuplicate(t *testing.T) {
	r := NewRegistry()
	r.RegisterProjector("contact-projector", func(context.Context, PipelineSpec) (deltaflow.Projector, error) {
		return deltaflow.ProjectorFunc(func(context.Context, deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{}, nil
		}), nil
	})

	defer func() {
		rcv := recover()
		if rcv == nil {
			t.Fatal("panic = nil")
		}
		if !strings.Contains(rcv.(string), "runtime projector already registered") {
			t.Fatalf("panic = %q", rcv)
		}
	}()

	r.RegisterProjector("contact-projector", func(context.Context, PipelineSpec) (deltaflow.Projector, error) {
		return nil, nil
	})
}

func TestRegistryRegisterApplierPanicsOnDuplicate(t *testing.T) {
	r := NewRegistry()
	r.RegisterApplier("elasticsearch", func(context.Context, PipelineSpec) (deltaflow.ProjectionApplier, error) {
		return deltaflow.ProjectionApplierFunc(func(context.Context, deltaflow.ProjectionOperation) error { return nil }), nil
	})

	defer func() {
		rcv := recover()
		if rcv == nil {
			t.Fatal("panic = nil")
		}
		if !strings.Contains(rcv.(string), "runtime applier already registered") {
			t.Fatalf("panic = %q", rcv)
		}
	}()

	r.RegisterApplier("elasticsearch", func(context.Context, PipelineSpec) (deltaflow.ProjectionApplier, error) {
		return nil, nil
	})
}
