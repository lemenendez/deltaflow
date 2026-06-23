package sqlite

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestApplyMigrationsCreatesTables(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()

	applied, err := ApplyMigrations(context.Background(), db)
	if err != nil {
		t.Fatalf("ApplyMigrations error: %v", err)
	}
	if len(applied) == 0 {
		t.Fatal("applied = 0, want at least one migration")
	}

	for _, table := range []string{"deltaflow_deltas", "deltaflow_sync_jobs", "deltaflow_worker_locks"} {
		var count int
		err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`, table).Scan(&count)
		if err != nil {
			t.Fatalf("table lookup %q error: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("table %q count = %d, want 1", table, count)
		}
	}
}

func TestExecMigrationSQLRollsBackOnError(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()

	// Force a failure after BEGIN to simulate a partial migration script.
	err = execMigrationSQL(context.Background(), db, `
BEGIN;
SELECT definitely_missing_function();
COMMIT;
`)
	if err == nil {
		t.Fatal("execMigrationSQL error = nil, want failure")
	}

	// Verify the same pooled connection can still run a BEGIN/COMMIT script.
	if err := execMigrationSQL(context.Background(), db, `
BEGIN;
CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY);
COMMIT;
`); err != nil {
		t.Fatalf("execMigrationSQL after failure error: %v", err)
	}
}
