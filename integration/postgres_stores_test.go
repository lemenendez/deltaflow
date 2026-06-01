//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/lemenendez/deltaflow/integration/testenv"
	"github.com/lemenendez/deltaflow/pkg/connectors"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

const (
	syncA = deltaflow.SyncID("it-sync-a")
	syncB = deltaflow.SyncID("it-sync-b")
)

func requireContainerIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("DELTAFLOW_IT_ENABLE") != "1" {
		t.Skip("set DELTAFLOW_IT_ENABLE=1 to run container-backed integration tests")
	}
}

func withStores(t *testing.T) (context.Context, deltaflow.DeltaStore, deltaflow.JobStore, deltaflow.DispatchStore) {
	t.Helper()
	requireContainerIntegration(t)

	ctx := context.Background()
	provider := testenv.NewFromEnv(t)
	db, cleanup := provider.Open(ctx, t)
	t.Cleanup(cleanup)

	truncateAll(t, ctx, db)

	deltaStore := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := pgstore.NewJobStore(db, pgstore.JobStoreConfig{MaxAttempts: 3})
	dispatchStore := pgstore.NewDispatchStore(deltaStore, jobStore, pgstore.DispatchStoreConfig{})

	return ctx, deltaStore, jobStore, dispatchStore
}

func truncateAll(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx, "TRUNCATE deltaflow.deltaflow_sync_jobs, deltaflow.deltaflow_deltas CASCADE")
	if err != nil {
		t.Fatalf("truncateAll: %v", err)
	}
}

func contactKey(id string) deltaflow.ProjectionKey {
	return deltaflow.ProjectionKey{"contact_id": json.RawMessage(`"` + id + `"`)}
}

func TestPostgresContainer_DeltaPullIsSyncScoped(t *testing.T) {
	ctx, deltaStore, _, _ := withStores(t)

	owned, err := deltaStore.Enqueue(ctx, deltaflow.Delta{
		SyncID:         syncA,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  contactKey("1"),
	})
	if err != nil {
		t.Fatalf("enqueue owned: %v", err)
	}

	if _, err := deltaStore.Enqueue(ctx, deltaflow.Delta{
		SyncID:         syncB,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  contactKey("2"),
	}); err != nil {
		t.Fatalf("enqueue foreign: %v", err)
	}

	pulled, err := deltaStore.Pull(ctx, syncA, 10)
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(pulled) != 1 {
		t.Fatalf("len(pulled) = %d, want 1", len(pulled))
	}
	if pulled[0].ID != owned.ID {
		t.Fatalf("pulled ID = %s, want %s", pulled[0].ID, owned.ID)
	}
}

func TestPostgresContainer_DispatchCreatesJobAndMarksDelta(t *testing.T) {
	ctx, deltaStore, jobStore, dispatchStore := withStores(t)

	delta, err := deltaStore.Enqueue(ctx, deltaflow.Delta{
		SyncID:         syncA,
		Origin:         deltaflow.OriginOperationInserted,
		ProjectionType: "Contact",
		ProjectionKey:  contactKey("42"),
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	jobs, err := dispatchStore.DispatchPending(ctx, syncA, 10)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}
	if jobs[0].DeltaID == nil || *jobs[0].DeltaID != delta.ID {
		t.Fatalf("job delta_id = %v, want %s", jobs[0].DeltaID, delta.ID)
	}

	gotDelta, ok, err := deltaStore.Get(ctx, delta.ID)
	if err != nil || !ok {
		t.Fatalf("get delta after dispatch: ok=%v err=%v", ok, err)
	}
	if gotDelta.State != deltaflow.DeltaDispatched {
		t.Fatalf("delta state = %s, want dispatched", gotDelta.State)
	}

	gotJob, ok, err := jobStore.Get(ctx, jobs[0].ID)
	if err != nil || !ok {
		t.Fatalf("get job after dispatch: ok=%v err=%v", ok, err)
	}
	if gotJob.State != deltaflow.StatePending {
		t.Fatalf("job state = %s, want pending", gotJob.State)
	}
}

func TestPostgresContainer_ClaimRetryAndReclaimLease(t *testing.T) {
	ctx, _, jobStore, _ := withStores(t)

	created, err := jobStore.Create(ctx, deltaflow.SyncJob{
		SyncID:         syncA,
		Origin:         deltaflow.JobOriginManual,
		ProjectionType: "Contact",
		ProjectionKey:  contactKey("9"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	firstClaim, err := jobStore.ClaimNext(ctx, syncA, "worker-1", 2*time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if firstClaim == nil {
		t.Fatal("first claim returned nil")
	}

	notStealable, err := jobStore.ClaimNext(ctx, syncA, "worker-2", time.Minute)
	if err != nil {
		t.Fatalf("second claim while leased: %v", err)
	}
	if notStealable != nil {
		t.Fatalf("expected nil while lease active, got %s", notStealable.ID)
	}

	nextRun := time.Now().UTC().Add(-1 * time.Second)
	if err := jobStore.MarkRetrying(ctx, created.ID, errors.New("transient"), nextRun); err != nil {
		t.Fatalf("mark retrying: %v", err)
	}

	reclaimed, err := jobStore.ClaimNext(ctx, syncA, "worker-3", time.Minute)
	if err != nil {
		t.Fatalf("claim after retry available: %v", err)
	}
	if reclaimed == nil {
		t.Fatal("expected claim after retry availability")
	}
	if reclaimed.ID != created.ID {
		t.Fatalf("claimed ID = %s, want %s", reclaimed.ID, created.ID)
	}
	if reclaimed.LockedBy == nil || *reclaimed.LockedBy != "worker-3" {
		t.Fatalf("locked_by = %v, want worker-3", reclaimed.LockedBy)
	}
}
