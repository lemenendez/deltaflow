package cli

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	"github.com/spf13/cobra"
)

func newMigrateCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Apply DeltaFlow Postgres schema migrations",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := opts.loadConfig()
			if err != nil {
				return err
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

			applied, err := pgstore.ApplyMigrations(ctx, db)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "applied %d migrations\n", len(applied))
			return nil
		},
	}
}
