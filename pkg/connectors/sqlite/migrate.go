package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const migrationRollbackTimeout = 2 * time.Second

type AppliedMigration struct {
	Name string
}

func ApplyMigrations(ctx context.Context, db *sql.DB) ([]AppliedMigration, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite migrations require database")
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	applied := make([]AppliedMigration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		name := entry.Name()
		if name == "000007_add_delta_dedup.sql" {
			if err := applyDeltaDedupMigration(ctx, db); err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			applied = append(applied, AppliedMigration{Name: name})
			continue
		}
		sqlBytes, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, err
		}
		if err := execMigrationSQL(ctx, db, string(sqlBytes)); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		applied = append(applied, AppliedMigration{Name: name})
	}

	return applied, nil
}

func applyDeltaDedupMigration(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	columns := make(map[string]bool)
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(deltaflow_deltas)`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return err
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !columns["dedup_window"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE deltaflow_deltas ADD COLUMN dedup_window TEXT`); err != nil {
			return err
		}
	}
	if !columns["dedup_key"] {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE deltaflow_deltas ADD COLUMN dedup_key TEXT`); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS deltaflow_deltas_dedup_key_uidx ON deltaflow_deltas (dedup_key) WHERE dedup_key IS NOT NULL`); err != nil {
		return err
	}
	return tx.Commit()
}

func execMigrationSQL(ctx context.Context, db *sql.DB, sqlText string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, sqlText); err != nil {
		// Migration files manage BEGIN/COMMIT; roll back on this same session
		// so the pooled connection cannot be returned with an open transaction.
		// Use a detached bounded context so cleanup ignores caller cancellation
		// but cannot block forever.
		rollbackCtx, cancelRollback := context.WithTimeout(context.Background(), migrationRollbackTimeout)
		defer cancelRollback()
		_, _ = conn.ExecContext(rollbackCtx, "ROLLBACK")
		return err
	}

	return nil
}
