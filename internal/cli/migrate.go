package cli

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	sqlitestore "github.com/lemenendez/deltaflow/pkg/connectors/sqlite"
	"github.com/spf13/cobra"

	_ "modernc.org/sqlite"
)

func newMigrateCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Apply DeltaFlow schema migrations",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := opts.loadConfig()
			if err != nil {
				return err
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()

			storeType := strings.TrimSpace(cfg.Store.Type)
			driverName := "pgx"
			if storeType == "sqlite" {
				driverName = "sqlite"
			}

			db, err := sql.Open(driverName, cfg.Store.DSN)
			if err != nil {
				return err
			}
			defer db.Close()
			configurePoolForStoreType(db, storeType)

			if err := db.PingContext(ctx); err != nil {
				if storeType == "sqlite" {
					return fmt.Errorf("connect sqlite: %w", err)
				}
				return fmt.Errorf("connect postgres: %w", err)
			}

			if storeType == "sqlite" {
				applied, err := sqlitestore.ApplyMigrations(ctx, db)
				if err != nil {
					return err
				}

				fmt.Fprintf(cmd.OutOrStdout(), "applied %d migrations\n", len(applied))
				return nil
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
