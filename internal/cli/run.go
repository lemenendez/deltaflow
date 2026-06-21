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
			runCtx, cancelRun := context.WithCancel(ctx)
			defer cancelRun()

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

			if err := db.PingContext(runCtx); err != nil {
				if storeType == "sqlite" {
					return fmt.Errorf("connect sqlite: %w", err)
				}
				return fmt.Errorf("connect postgres: %w", err)
			}

			var (
				jobStore      deltaflow.JobStore
				dispatchStore deltaflow.DispatchStore
				lockErrCh     <-chan error
				stopHeartbeat func()
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
				if _, err := sqlitestore.ApplyMigrations(runCtx, db); err != nil {
					return err
				}

				releaseLock, err := sqlitestore.AcquireWorkerLock(runCtx, db, workerID, leaseTTL)
				if err != nil {
					if errors.Is(err, sqlitestore.ErrWorkerAlreadyRunning) {
						return fmt.Errorf("sqlite worker already running for this database; stop the other worker or use postgres for multi-worker deployments")
					}
					return err
				}
				defer func() {
					releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(runCtx), 2*time.Second)
					defer cancelRelease()
					_ = releaseLock(releaseCtx)
				}()

				stopHeartbeat, lockErrCh = startSQLiteWorkerLockHeartbeat(runCtx, db, workerID, leaseTTL)
				defer stopHeartbeat()
				go func() {
					err, ok := <-lockErrCh
					if ok && err != nil {
						cancelRun()
					}
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

			built, err := runtimepkg.BuildFromConfig(runCtx, runtimeCfg, opts.runtimeRegistry, runtimepkg.WorkerDeps{
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

			runErr := runtimepkg.RunOnce(runCtx, built)
			if lockErr := firstHeartbeatError(lockErrCh); lockErr != nil {
				if runErr == nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
					return lockErr
				}
			}
			if runErr != nil {
				return runErr
			}

			if err := firstHeartbeatError(lockErrCh); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "run complete: processed %d pipeline(s)\n", len(built.Pipelines))
			return nil
		},
	}

	cmd.Flags().StringVar(&workerID, "worker-id", "", "worker identity for lease ownership")

	return cmd
}

func startSQLiteWorkerLockHeartbeat(parent context.Context, db *sql.DB, workerID string, leaseTTL time.Duration) (func(), <-chan error) {
	heartbeatCtx, cancel := context.WithCancel(parent)
	errCh := make(chan error, 1)
	interval := sqliteLockHeartbeatInterval(leaseTTL)
	ticker := time.NewTicker(interval)
	renewTimeout := minDuration(interval, 2*time.Second)

	go func() {
		defer ticker.Stop()
		defer close(errCh)

		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				renewCtx, cancelRenew := context.WithTimeout(context.WithoutCancel(heartbeatCtx), renewTimeout)
				err := sqlitestore.RenewWorkerLock(renewCtx, db, workerID, leaseTTL)
				cancelRenew()
				if err != nil {
					errCh <- fmt.Errorf("sqlite worker lock heartbeat failed: %w", err)
					return
				}
			}
		}
	}()

	return cancel, errCh
}

func sqliteLockHeartbeatInterval(leaseTTL time.Duration) time.Duration {
	if leaseTTL <= 0 {
		return time.Second
	}
	half := leaseTTL / 2
	if half < time.Second {
		return time.Second
	}
	if half > 10*time.Second {
		return 10 * time.Second
	}
	return half
}

func firstHeartbeatError(errCh <-chan error) error {
	if errCh == nil {
		return nil
	}
	select {
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		return err
	default:
		return nil
	}
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
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
