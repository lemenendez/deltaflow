package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestSQLiteSmokeSourceWriteEnqueueWorkerCycle(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteTestDB(t)

	if _, err := db.ExecContext(ctx, `CREATE TABLE contacts (id TEXT PRIMARY KEY, full_name TEXT NOT NULL)`); err != nil {
		t.Fatalf("create contacts table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, full_name) VALUES (?, ?)`, "c-1", "Alice Test"); err != nil {
		t.Fatalf("insert source contact: %v", err)
	}

	deltaStore := NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := NewJobStore(db, JobStoreConfig{})
	dispatchStore := NewDispatchStore(deltaStore, jobStore, DispatchStoreConfig{})

	_, err := deltaStore.Enqueue(ctx, deltaflow.Delta{
		SyncID:         "contacts-sync",
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: "contact",
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"c-1"`),
		},
	})
	if err != nil {
		t.Fatalf("enqueue delta: %v", err)
	}

	appliedPayloads := make([]string, 0, 1)
	worker := &deltaflow.SyncWorker{
		JobStore:   jobStore,
		Dispatcher: dispatchStore,
		Projector: deltaflow.ProjectorFunc(func(ctx context.Context, id deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			var contactID string
			if err := json.Unmarshal(id.Key["contact_id"], &contactID); err != nil {
				return deltaflow.Projection{}, err
			}

			var fullName string
			if err := db.QueryRowContext(ctx, `SELECT full_name FROM contacts WHERE id = ?`, contactID).Scan(&fullName); err != nil {
				if err == sql.ErrNoRows {
					return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
				}
				return deltaflow.Projection{}, err
			}

			payload, err := json.Marshal(map[string]any{"id": contactID, "full_name": fullName})
			if err != nil {
				return deltaflow.Projection{}, err
			}

			return deltaflow.Projection{Identity: id, Payload: payload, MediaType: "application/json"}, nil
		}),
		Applier: deltaflow.ProjectionApplierFunc(func(_ context.Context, op deltaflow.ProjectionOperation) error {
			if op.Type != deltaflow.ProjectionOpUpsert {
				t.Fatalf("op.Type = %q, want %q", op.Type, deltaflow.ProjectionOpUpsert)
			}
			appliedPayloads = append(appliedPayloads, string(op.Projection.Payload))
			return nil
		}),
		SyncID:      "contacts-sync",
		WorkerID:    "sqlite-smoke",
		LockFor:     3 * time.Second,
		BatchSize:   1,
		Concurrency: 1,
	}

	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("worker run once: %v", err)
	}

	if len(appliedPayloads) != 1 {
		t.Fatalf("applied payload count = %d, want 1", len(appliedPayloads))
	}
	if appliedPayloads[0] != `{"full_name":"Alice Test","id":"c-1"}` && appliedPayloads[0] != `{"id":"c-1","full_name":"Alice Test"}` {
		t.Fatalf("unexpected payload = %s", appliedPayloads[0])
	}

	var syncedCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deltaflow_sync_jobs WHERE state = 'synced'`).Scan(&syncedCount); err != nil {
		t.Fatalf("count synced jobs: %v", err)
	}
	if syncedCount != 1 {
		t.Fatalf("synced job count = %d, want 1", syncedCount)
	}
}
