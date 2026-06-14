package postgres

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
)

var migrationTestDriverSeq atomic.Int64

func TestExecMigrationSQLRollsBackFailedMigrationOnSameConnection(t *testing.T) {
	recorder := &migrationTestRecorder{}
	driverName := fmt.Sprintf("deltaflow_migration_test_%d", migrationTestDriverSeq.Add(1))
	sql.Register(driverName, &migrationTestDriver{recorder: recorder})

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}
	defer db.Close()

	err = execMigrationSQL(context.Background(), db, "BEGIN;\nSELECT fail;\nCOMMIT;")
	if err == nil {
		t.Fatal("execMigrationSQL error = nil")
	}

	events := recorder.events()
	if len(events) < 2 {
		t.Fatalf("events = %v, want failed exec and rollback", events)
	}
	if !strings.Contains(events[0].query, "SELECT fail") {
		t.Fatalf("first query = %q, want failing migration SQL", events[0].query)
	}
	if events[1].query != "ROLLBACK" {
		t.Fatalf("second query = %q, want ROLLBACK", events[1].query)
	}
	if events[1].connID != events[0].connID {
		t.Fatalf("rollback connID = %d, want same connection %d", events[1].connID, events[0].connID)
	}
}

type migrationTestRecorder struct {
	mu     sync.Mutex
	nextID int
	logs   []migrationTestEvent
}

type migrationTestEvent struct {
	connID int
	query  string
}

func (r *migrationTestRecorder) openConn() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	return r.nextID
}

func (r *migrationTestRecorder) record(connID int, query string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logs = append(r.logs, migrationTestEvent{connID: connID, query: query})
}

func (r *migrationTestRecorder) events() []migrationTestEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	copied := make([]migrationTestEvent, len(r.logs))
	copy(copied, r.logs)
	return copied
}

type migrationTestDriver struct {
	recorder *migrationTestRecorder
}

func (d *migrationTestDriver) Open(_ string) (driver.Conn, error) {
	return &migrationTestConn{recorder: d.recorder, id: d.recorder.openConn()}, nil
}

type migrationTestConn struct {
	recorder *migrationTestRecorder
	id       int
}

func (c *migrationTestConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c *migrationTestConn) Close() error {
	return nil
}

func (c *migrationTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin is not supported")
}

func (c *migrationTestConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.recorder.record(c.id, query)
	if strings.Contains(query, "SELECT fail") {
		return nil, errors.New("migration failed")
	}
	return driver.RowsAffected(0), nil
}
