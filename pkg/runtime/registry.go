package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

var (
	ErrProjectorNotRegistered = errors.New("runtime projector not registered")
	ErrApplierNotRegistered   = errors.New("runtime applier not registered")
)

type PipelineSpec struct {
	Name                 string
	SyncID               deltaflow.SyncID
	StoreType            string
	StoreDSN             string
	StoreDB              *sql.DB
	ProjectorName        string
	SourceType           string
	SourceProjectionType string
	TargetType           string
	TargetIndex          string
	ApplierMode          string
}

type ProjectorFactory func(ctx context.Context, spec PipelineSpec) (deltaflow.Projector, error)

type ApplierFactory func(ctx context.Context, spec PipelineSpec) (deltaflow.ProjectionApplier, error)

type PipelineRuntime struct {
	Spec      PipelineSpec
	Projector deltaflow.Projector
	Applier   deltaflow.ProjectionApplier
}

type Registry struct {
	projectors map[string]ProjectorFactory
	appliers   map[string]ApplierFactory
}

func NewRegistry() *Registry {
	return &Registry{
		projectors: make(map[string]ProjectorFactory),
		appliers:   make(map[string]ApplierFactory),
	}
}

func (r *Registry) RegisterProjector(name string, factory ProjectorFactory) {
	if r == nil {
		panic("runtime registry is required")
	}
	if factory == nil {
		panic("runtime projector factory is required")
	}
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		panic("runtime projector name is required")
	}
	if trimmed != name {
		panic(fmt.Sprintf("runtime projector name must not include leading/trailing whitespace: %q", name))
	}
	if r.projectors == nil {
		r.projectors = make(map[string]ProjectorFactory)
	}
	if _, exists := r.projectors[name]; exists {
		panic(fmt.Sprintf("runtime projector already registered: %q", name))
	}
	r.projectors[name] = factory
}

func (r *Registry) RegisterApplier(targetType string, factory ApplierFactory) {
	if r == nil {
		panic("runtime registry is required")
	}
	if factory == nil {
		panic("runtime applier factory is required")
	}
	trimmed := strings.TrimSpace(targetType)
	if trimmed == "" {
		panic("runtime applier target type is required")
	}
	if trimmed != targetType {
		panic(fmt.Sprintf("runtime applier target type must not include leading/trailing whitespace: %q", targetType))
	}
	if r.appliers == nil {
		r.appliers = make(map[string]ApplierFactory)
	}
	if _, exists := r.appliers[targetType]; exists {
		panic(fmt.Sprintf("runtime applier already registered: %q", targetType))
	}
	r.appliers[targetType] = factory
}

func (r *Registry) ResolvePipeline(ctx context.Context, spec PipelineSpec) (PipelineRuntime, error) {
	if r == nil {
		return PipelineRuntime{}, errors.New("runtime registry is required")
	}

	projectorFactory, ok := r.lookupProjector(spec.ProjectorName)
	if !ok {
		return PipelineRuntime{}, fmt.Errorf("pipeline %q: %w: %q", spec.Name, ErrProjectorNotRegistered, spec.ProjectorName)
	}
	applierFactory, ok := r.lookupApplier(spec.TargetType)
	if !ok {
		return PipelineRuntime{}, fmt.Errorf("pipeline %q: %w: %q", spec.Name, ErrApplierNotRegistered, spec.TargetType)
	}

	projector, err := projectorFactory(ctx, spec)
	if err != nil {
		return PipelineRuntime{}, fmt.Errorf("pipeline %q: build projector %q: %w", spec.Name, spec.ProjectorName, err)
	}
	if projector == nil {
		return PipelineRuntime{}, fmt.Errorf("pipeline %q: projector %q resolved to nil", spec.Name, spec.ProjectorName)
	}

	applier, err := applierFactory(ctx, spec)
	if err != nil {
		return PipelineRuntime{}, fmt.Errorf("pipeline %q: build applier target %q: %w", spec.Name, spec.TargetType, err)
	}
	if applier == nil {
		return PipelineRuntime{}, fmt.Errorf("pipeline %q: applier for target %q resolved to nil", spec.Name, spec.TargetType)
	}

	return PipelineRuntime{
		Spec:      spec,
		Projector: projector,
		Applier:   applier,
	}, nil
}

func (r *Registry) lookupProjector(name string) (ProjectorFactory, bool) {
	factory, ok := r.projectors[name]
	return factory, ok
}

func (r *Registry) HasProjector(name string) bool {
	_, ok := r.lookupProjector(name)
	return ok
}

func (r *Registry) lookupApplier(targetType string) (ApplierFactory, bool) {
	factory, ok := r.appliers[targetType]
	return factory, ok
}

func (r *Registry) HasApplier(targetType string) bool {
	_, ok := r.lookupApplier(targetType)
	return ok
}
