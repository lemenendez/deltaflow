package runtime

import (
	"context"
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
	Name          string
	SyncID        deltaflow.SyncID
	StoreType     string
	StoreDSN      string
	ProjectorName string
	SourceType    string
	TargetType    string
	TargetIndex   string
	ApplierMode   string
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
	key := normalizeKey(name)
	if key == "" {
		panic("runtime projector name is required")
	}
	if _, exists := r.projectors[key]; exists {
		panic(fmt.Sprintf("runtime projector already registered: %q", name))
	}
	r.projectors[key] = factory
}

func (r *Registry) RegisterApplier(targetType string, factory ApplierFactory) {
	if r == nil {
		panic("runtime registry is required")
	}
	if factory == nil {
		panic("runtime applier factory is required")
	}
	key := normalizeKey(targetType)
	if key == "" {
		panic("runtime applier target type is required")
	}
	if _, exists := r.appliers[key]; exists {
		panic(fmt.Sprintf("runtime applier already registered: %q", targetType))
	}
	r.appliers[key] = factory
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
	key := normalizeKey(name)
	factory, ok := r.projectors[key]
	return factory, ok
}

func (r *Registry) HasProjector(name string) bool {
	_, ok := r.lookupProjector(name)
	return ok
}

func (r *Registry) lookupApplier(targetType string) (ApplierFactory, bool) {
	key := normalizeKey(targetType)
	factory, ok := r.appliers[key]
	return factory, ok
}

func (r *Registry) HasApplier(targetType string) bool {
	_, ok := r.lookupApplier(targetType)
	return ok
}

func normalizeKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
