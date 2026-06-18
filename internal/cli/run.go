package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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
				return fmt.Errorf("runtime registry is required; use NewRootCommandWithRegistry in host binaries")
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

			db, err := sql.Open("pgx", cfg.Store.DSN)
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

			built, err := runtimepkg.BuildFromConfig(ctx, cfg, opts.runtimeRegistry, runtimepkg.WorkerDeps{
				JobStore:   jobStore,
				Dispatcher: dispatchStore,
				WorkerID:   workerID,
				LockFor:    leaseTTL,
				PullSize:   pullSize,
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
