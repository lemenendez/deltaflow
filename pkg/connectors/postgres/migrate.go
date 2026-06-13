package postgres

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

// ApplyMigrations applies the embedded DeltaFlow Postgres migrations in file
// name order. The current SQL files are idempotent and can be applied more
// than once.
func ApplyMigrations(ctx context.Context, db *sql.DB) ([]AppliedMigration, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres migrations require database")
	}
	if err := ensureUUIDV7(ctx, db); err != nil {
		return nil, err
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
		if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		applied = append(applied, AppliedMigration{Name: name})
	}

	return applied, nil
}

func ensureUUIDV7(ctx context.Context, db *sql.DB) error {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regprocedure('uuidv7()') IS NOT NULL`).Scan(&exists); err != nil {
		return fmt.Errorf("check uuidv7 compatibility: %w", err)
	}
	if exists {
		return nil
	}

	if _, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto`); err != nil {
		return fmt.Errorf("prepare uuidv7 compatibility extension: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION uuidv7() RETURNS uuid LANGUAGE SQL AS $$ SELECT gen_random_uuid(); $$;`); err != nil {
		return fmt.Errorf("prepare uuidv7 compatibility function: %w", err)
	}
	return nil
}
