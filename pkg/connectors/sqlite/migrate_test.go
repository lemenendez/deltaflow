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
