package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type BuildConfig struct {
	Store     BuildStoreConfig
	Pipelines []BuildPipelineConfig
}

type BuildStoreConfig struct {
	Type string
	DSN  string
}

type BuildPipelineConfig struct {
	Name      string
	SyncID    string
	Source    BuildSourceConfig
	Projector BuildProjectorConfig
	Target    BuildTargetConfig
	Applier   BuildApplierConfig
}

type BuildSourceConfig struct {
	Type           string
	ProjectionType string
}

type BuildProjectorConfig struct {
	Name string
}

type BuildTargetConfig struct {
	Type  string
	Index string
}

type BuildApplierConfig struct {
	Mode string
}

type WorkerDeps struct {
	JobStore    deltaflow.JobStore
	Dispatcher  deltaflow.DispatchStore
	StoreDB     *sql.DB
	WorkerID    string
	LockFor     time.Duration
	PullSize    int
	BatchSize   int
	Concurrency int
}

type PipelineRunner struct {
	Name   string
	SyncID deltaflow.SyncID
	Worker *deltaflow.SyncWorker
}

type BuildResult struct {
	Pipelines []PipelineRunner
}

func BuildFromConfig(ctx context.Context, cfg *BuildConfig, registry *Registry, deps WorkerDeps) (*BuildResult, error) {
	if cfg == nil {
		return nil, errors.New("runtime config is required")
	}
	if registry == nil {
		return nil, errors.New("runtime registry is required")
	}
	if deps.JobStore == nil {
		return nil, errors.New("runtime job store is required")
	}
	if deps.WorkerID == "" {
		return nil, errors.New("runtime worker id is required")
	}
	if deps.LockFor <= 0 {
		return nil, errors.New("runtime lock_for must be positive")
	}

	runners := make([]PipelineRunner, 0, len(cfg.Pipelines))
	for _, p := range cfg.Pipelines {
		if p.Source.Type == "postgres-outbox" && deps.Dispatcher == nil {
			return nil, fmt.Errorf("pipeline %q: runtime dispatcher is required for source.type=postgres-outbox", p.Name)
		}

		spec := PipelineSpec{
			Name:                 p.Name,
			SyncID:               deltaflow.SyncID(p.SyncID),
			StoreType:            cfg.Store.Type,
			StoreDSN:             cfg.Store.DSN,
			StoreDB:              deps.StoreDB,
			ProjectorName:        p.Projector.Name,
			SourceType:           p.Source.Type,
			SourceProjectionType: p.Source.ProjectionType,
			TargetType:           p.Target.Type,
			TargetIndex:          p.Target.Index,
			ApplierMode:          p.Applier.Mode,
		}
		resolved, err := registry.ResolvePipeline(ctx, spec)
		if err != nil {
			return nil, err
		}

		worker := &deltaflow.SyncWorker{
			JobStore:    deps.JobStore,
			Dispatcher:  deps.Dispatcher,
			Projector:   resolved.Projector,
			Applier:     resolved.Applier,
			SyncID:      resolved.Spec.SyncID,
			WorkerID:    deps.WorkerID,
			LockFor:     deps.LockFor,
			PullSize:    deps.PullSize,
			BatchSize:   deps.BatchSize,
			Concurrency: deps.Concurrency,
		}

		runners = append(runners, PipelineRunner{
			Name:   resolved.Spec.Name,
			SyncID: resolved.Spec.SyncID,
			Worker: worker,
		})
	}

	return &BuildResult{Pipelines: runners}, nil
}

func RunOnce(ctx context.Context, built *BuildResult) error {
	if built == nil {
		return errors.New("runtime build result is required")
	}

	for _, pipeline := range built.Pipelines {
		if pipeline.Worker == nil {
			return fmt.Errorf("pipeline %q: worker is nil", pipeline.Name)
		}
		if err := pipeline.Worker.RunOnce(ctx); err != nil {
			return fmt.Errorf("pipeline %q run_once: %w", pipeline.Name, err)
		}
	}

	return nil
}
