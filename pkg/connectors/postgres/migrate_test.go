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
	"time"
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
	if !strings.Contains(events[0].query, "SELECT fail") {
		t.Fatalf("first query = %q, want failing migration SQL", events[0].query)
	}
	if !events[0].deadlineSet {
		t.Fatal("migration exec context had no deadline, want caller deadline present")
	}
	if events[1].query != "ROLLBACK" {
		t.Fatalf("second query = %q, want ROLLBACK", events[1].query)
	}
	if !events[1].deadlineSet {
		t.Fatal("rollback context has no deadline, want bounded cleanup timeout")
	}
	if !events[1].deadline.Before(events[0].deadline) {
		t.Fatalf("rollback deadline = %v, want earlier than caller deadline %v", events[1].deadline, events[0].deadline)
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
	connID      int
	query       string
	deadlineSet bool
	deadline    time.Time
}

func (r *migrationTestRecorder) openConn() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++
	return r.nextID
}

func (r *migrationTestRecorder) record(connID int, query string, ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	deadline, deadlineSet := ctx.Deadline()
	r.logs = append(r.logs, migrationTestEvent{connID: connID, query: query, deadlineSet: deadlineSet, deadline: deadline})
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

func (c *migrationTestConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.recorder.record(c.id, query, ctx)
	if strings.Contains(query, "SELECT fail") {
		return nil, errors.New("migration failed")
	}
	return driver.RowsAffected(0), nil
}
