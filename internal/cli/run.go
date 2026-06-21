package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/lemenendez/deltaflow/internal/config"
	"github.com/lemenendez/deltaflow/pkg/connectors"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	sqlitestore "github.com/lemenendez/deltaflow/pkg/connectors/sqlite"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	runtimepkg "github.com/lemenendez/deltaflow/pkg/runtime"
	"github.com/spf13/cobra"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
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

			storeType := strings.TrimSpace(cfg.Store.Type)
			if storeType != "postgres" && storeType != "sqlite" {
				return fmt.Errorf("run requires store.type=postgres or store.type=sqlite")
			}
			dsn := strings.TrimSpace(cfg.Store.DSN)
			if dsn == "" {
				return fmt.Errorf("run requires store.dsn to be set")
			}
			if storeType == "sqlite" && cfg.Workers.Concurrency != 1 {
				return fmt.Errorf("sqlite supports only workers.concurrency=1")
			}

			driverName := "pgx"
			if storeType == "sqlite" {
				driverName = "sqlite"
			}

			db, err := sql.Open(driverName, dsn)
			if err != nil {
				return err
			}
			defer db.Close()

			if err := db.PingContext(ctx); err != nil {
				if storeType == "sqlite" {
					return fmt.Errorf("connect sqlite: %w", err)
				}
				return fmt.Errorf("connect postgres: %w", err)
			}

			var (
				jobStore      deltaflow.JobStore
				dispatchStore deltaflow.DispatchStore
			)

			if storeType == "postgres" {
				deltaStore := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
				jobCfg := pgstore.JobStoreConfig{}
				if cfg.Workers.MaxAttempts != nil {
					jobCfg.MaxAttempts = *cfg.Workers.MaxAttempts
				}
				job := pgstore.NewJobStore(db, jobCfg)
				jobStore = job
				dispatchStore = pgstore.NewDispatchStore(deltaStore, job, pgstore.DispatchStoreConfig{})
			} else {
				if _, err := sqlitestore.ApplyMigrations(ctx, db); err != nil {
					return err
				}

				releaseLock, err := sqlitestore.AcquireWorkerLock(ctx, db, workerID, leaseTTL)
				if err != nil {
					if errors.Is(err, sqlitestore.ErrWorkerAlreadyRunning) {
						return fmt.Errorf("sqlite worker already running for this database; stop the other worker or use postgres for multi-worker deployments")
					}
					return err
				}
				defer func() {
					releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
					defer cancelRelease()
					_ = releaseLock(releaseCtx)
				}()

				deltaStore := sqlitestore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
				jobCfg := sqlitestore.JobStoreConfig{}
				if cfg.Workers.MaxAttempts != nil {
					jobCfg.MaxAttempts = *cfg.Workers.MaxAttempts
				}
				job := sqlitestore.NewJobStore(db, jobCfg)
				jobStore = job
				dispatchStore = sqlitestore.NewDispatchStore(deltaStore, job, sqlitestore.DispatchStoreConfig{})
			}

			pullSize, batchSize := workerSizing(cfg.Workers)

			runtimeCfg := &runtimepkg.BuildConfig{
				Store: runtimepkg.BuildStoreConfig{
					Type: storeType,
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
					Projector: runtimepkg.BuildProjectorConfig{Name: p.Projector.Name},
					Target: runtimepkg.BuildTargetConfig{
						Type:  p.Target.Type,
						Index: p.Target.Index,
					},
					Applier: runtimepkg.BuildApplierConfig{Mode: p.Applier.Mode},
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

func workerSizing(workers config.WorkersConfig) (pullSize int, batchSize int) {
	// Keep pull size unset unless explicitly configured so SyncWorker can derive it.
	pullSize = 0
	if workers.PullSize != nil {
		pullSize = *workers.PullSize
	}

	batchSize = 1
	if workers.BatchSize != nil {
		batchSize = *workers.BatchSize
	}

	return pullSize, batchSize
}
