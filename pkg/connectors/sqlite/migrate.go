package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

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

func execMigrationSQL(ctx context.Context, db *sql.DB, sqlText string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, sqlText); err != nil {
		// Migration files manage BEGIN/COMMIT; roll back on this same session
		// so the pooled connection cannot be returned with an open transaction.
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return err
	}

	return nil
}
