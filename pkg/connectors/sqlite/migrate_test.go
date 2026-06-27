package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

var sqliteMigrationTestDriverSeq atomic.Int64

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

func TestExecMigrationSQLRollbackIgnoresCallerDeadline(t *testing.T) {
	recorder := &sqliteMigrationTestRecorder{}
	driverName := fmt.Sprintf("deltaflow_sqlite_migration_test_%d", sqliteMigrationTestDriverSeq.Add(1))
	sql.Register(driverName, &sqliteMigrationTestDriver{recorder: recorder})

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err = execMigrationSQL(ctx, db, "BEGIN;\nSELECT fail;\nCOMMIT;")
	if err == nil {
		t.Fatal("execMigrationSQL error = nil")
	}

	events := recorder.events()
	if len(events) < 2 {
		t.Fatalf("events = %v, want failed exec and rollback", events)
	}
	if !events[0].deadlineSet {
		t.Fatal("migration exec context had no deadline, want caller deadline present")
	}
	if events[1].query != "ROLLBACK" {
		t.Fatalf("second query = %q, want ROLLBACK", events[1].query)
	}
	if events[1].deadlineSet {
		t.Fatal("rollback context inherited caller deadline, want no deadline")
	}
	if events[1].connID != events[0].connID {
		t.Fatalf("rollback connID = %d, want same connection %d", events[1].connID, events[0].connID)
	}
}

type sqliteMigrationTestRecorder struct {
	mu     sync.Mutex
	nextID int
	logs   []sqliteMigrationTestEvent
}

type sqliteMigrationTestEvent struct {
	connID      int
	query       string
	deadlineSet bool
}

func (r *sqliteMigrationTestRecorder) openConn() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	return r.nextID
}

func (r *sqliteMigrationTestRecorder) record(connID int, query string, ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, deadlineSet := ctx.Deadline()
	r.logs = append(r.logs, sqliteMigrationTestEvent{connID: connID, query: query, deadlineSet: deadlineSet})
}

func (r *sqliteMigrationTestRecorder) events() []sqliteMigrationTestEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	copied := make([]sqliteMigrationTestEvent, len(r.logs))
	copy(copied, r.logs)
	return copied
}

type sqliteMigrationTestDriver struct {
	recorder *sqliteMigrationTestRecorder
}

func (d *sqliteMigrationTestDriver) Open(_ string) (driver.Conn, error) {
	return &sqliteMigrationTestConn{recorder: d.recorder, id: d.recorder.openConn()}, nil
}

type sqliteMigrationTestConn struct {
	recorder *sqliteMigrationTestRecorder
	id       int
}

func (c *sqliteMigrationTestConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c *sqliteMigrationTestConn) Close() error {
	return nil
}

func (c *sqliteMigrationTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin is not supported")
}

func (c *sqliteMigrationTestConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.recorder.record(c.id, query, ctx)
	if strings.Contains(query, "SELECT fail") {
		return nil, errors.New("migration failed")
	}
	return driver.RowsAffected(0), nil
}
