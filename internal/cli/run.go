package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	runtimepkg "github.com/lemenendez/deltaflow/pkg/runtime"
	"github.com/spf13/cobra"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func newRunCommand(opts *options) *cobra.Command {
	var workerID string

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run one worker cycle for each configured pipeline",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := opts.loadConfig()
			if err != nil {
				return err
			}
			if opts.runtimeRegistry == nil {
				return fmt.Errorf("runtime registry is required")
			}
			for _, p := range cfg.Pipelines {
				if !opts.runtimeRegistry.HasProjector(p.Projector.Name) {
					return fmt.Errorf("pipeline %q: %w: %q", p.Name, runtimepkg.ErrProjectorNotRegistered, p.Projector.Name)
				}
				if !opts.runtimeRegistry.HasApplier(p.Target.Type) {
					return fmt.Errorf("pipeline %q: %w: %q", p.Name, runtimepkg.ErrApplierNotRegistered, p.Target.Type)
				}
			}

			leaseTTL, err := cfg.Workers.LeaseTTLDuration()
			if err != nil {
				return err
			}

			if workerID == "" {
				host, hostErr := os.Hostname()
				if hostErr != nil || host == "" {
					host = "unknown-host"
				}
				workerID = fmt.Sprintf("deltaflow-cli-%s", host)
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()

			if strings.TrimSpace(cfg.Store.Type) != "postgres" {
				return fmt.Errorf("run requires store.type=postgres")
			}
			dsn := strings.TrimSpace(cfg.Store.DSN)
			if dsn == "" {
				return fmt.Errorf("run requires store.dsn to be set for store.type=postgres")
			}

			db, err := sql.Open("pgx", dsn)
			if err != nil {
				return err
			}
			defer db.Close()

			if err := db.PingContext(ctx); err != nil {
				return fmt.Errorf("connect postgres: %w", err)
			}

			deltaStore := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
			jobCfg := pgstore.JobStoreConfig{}
			if cfg.Workers.MaxAttempts != nil {
				jobCfg.MaxAttempts = *cfg.Workers.MaxAttempts
			}
			jobStore := pgstore.NewJobStore(db, jobCfg)
			dispatchStore := pgstore.NewDispatchStore(deltaStore, jobStore, pgstore.DispatchStoreConfig{})

			pullSize := 1
			if cfg.Workers.PullSize != nil {
				pullSize = *cfg.Workers.PullSize
			}
			batchSize := 1
			if cfg.Workers.BatchSize != nil {
				batchSize = *cfg.Workers.BatchSize
			}

			runtimeCfg := &runtimepkg.BuildConfig{
				Store: runtimepkg.BuildStoreConfig{
					Type: strings.TrimSpace(cfg.Store.Type),
					DSN:  dsn,
				},
				Pipelines: make([]runtimepkg.BuildPipelineConfig, 0, len(cfg.Pipelines)),
			}
			for _, p := range cfg.Pipelines {
				runtimeCfg.Pipelines = append(runtimeCfg.Pipelines, runtimepkg.BuildPipelineConfig{
					Name:   p.Name,
					SyncID: p.SyncID,
					Source: runtimepkg.BuildSourceConfig{
						Type:           p.Source.Type,
						ProjectionType: p.Source.ProjectionType,
					},
					Projector: runtimepkg.BuildProjectorConfig{
						Name: p.Projector.Name,
					},
					Target: runtimepkg.BuildTargetConfig{
						Type:  p.Target.Type,
						Index: p.Target.Index,
					},
					Applier: runtimepkg.BuildApplierConfig{
						Mode: p.Applier.Mode,
					},
				})
			}

			built, err := runtimepkg.BuildFromConfig(ctx, runtimeCfg, opts.runtimeRegistry, runtimepkg.WorkerDeps{
				JobStore:    jobStore,
				Dispatcher:  dispatchStore,
				StoreDB:     db,
				WorkerID:    workerID,
				LockFor:     leaseTTL,
				PullSize:    pullSize,
				BatchSize:   batchSize,
				Concurrency: cfg.Workers.Concurrency,
			})
			if err != nil {
				return err
			}

			if err := runtimepkg.RunOnce(ctx, built); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "run complete: processed %d pipeline(s)\n", len(built.Pipelines))
			return nil
		},
	}

	cmd.Flags().StringVar(&workerID, "worker-id", "", "worker identity for lease ownership")

	return cmd
}
