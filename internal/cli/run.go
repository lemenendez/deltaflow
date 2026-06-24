package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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
			configurePoolForStoreType(db, storeType)

			if err := db.PingContext(runCtx); err != nil {
				if storeType == "sqlite" {
					return fmt.Errorf("connect sqlite: %w", err)
				}
				return fmt.Errorf("connect postgres: %w", err)
			}
			if storeType == "sqlite" {
				if _, err := db.ExecContext(runCtx, `PRAGMA foreign_keys=ON`); err != nil {
					return fmt.Errorf("enable sqlite foreign keys: %w", err)
				}
			}

			var (
				jobStore       deltaflow.JobStore
				dispatchStore  deltaflow.DispatchStore
				stopHeartbeat  func()
				heartbeatWatch *sqliteHeartbeatWatcher
			)
			stopHeartbeat = func() {}

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

				var lockErrCh <-chan error
				stopHeartbeat, lockErrCh = startSQLiteWorkerLockHeartbeat(runCtx, db, workerID, leaseTTL)
				heartbeatWatch = startSQLiteHeartbeatWatcher(lockErrCh, cancelRun)
				defer stopHeartbeat()
				defer heartbeatWatch.wait()

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
			stopHeartbeat()
			if heartbeatWatch != nil {
				heartbeatWatch.wait()
				if lockErr := heartbeatWatch.err(); lockErr != nil {
					if runErr == nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
						return lockErr
					}
				}
			}
			if runErr != nil {
				return runErr
			}

			if heartbeatWatch != nil {
				if err := heartbeatWatch.err(); err != nil {
					return err
				}
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
		return half
	}
	if half > 10*time.Second {
		return 10 * time.Second
	}
	return half
}

type sqliteHeartbeatWatcher struct {
	done chan struct{}
	mu   sync.Mutex
	errV error
}

func startSQLiteHeartbeatWatcher(errCh <-chan error, cancelRun context.CancelFunc) *sqliteHeartbeatWatcher {
	w := &sqliteHeartbeatWatcher{done: make(chan struct{})}
	go func() {
		defer close(w.done)
		err, ok := <-errCh
		if !ok || err == nil {
			return
		}
		w.mu.Lock()
		w.errV = err
		w.mu.Unlock()
		cancelRun()
	}()
	return w
}

func (w *sqliteHeartbeatWatcher) wait() {
	if w == nil {
		return
	}
	<-w.done
}

func (w *sqliteHeartbeatWatcher) err() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.errV
}

func minDuration(a time.Duration, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func configurePoolForStoreType(db *sql.DB, storeType string) {
	if db == nil {
		return
	}
	if strings.TrimSpace(storeType) != "sqlite" {
		return
	}

	// Keep sqlite on one underlying connection so pragmas and lock state are consistent.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
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
