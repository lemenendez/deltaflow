package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lemenendez/deltaflow/internal"
	"github.com/lemenendez/deltaflow/pkg/connectors"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type runStats struct {
	Enqueued   int
	WorkerRuns int
	Upserts    int
	Deletes    int
	Ghosts     int
}

func main() {
	ctx := context.Background()

	dsn := os.Getenv("DELTAFLOW_PG_DSN")
	if dsn == "" {
		dsn = "postgres://deltaflow:deltaflow@postgres:5432/deltaflow?sslmode=disable"
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("ping db: %v", err)
	}

	deltaStore := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := pgstore.NewJobStore(db, pgstore.JobStoreConfig{MaxAttempts: 3})
	dispatchStore := pgstore.NewDispatchStore(deltaStore, jobStore, pgstore.DispatchStoreConfig{})

	s := buildDemoScenario()
	syncID := attachRunScopedSyncID(&s)
	projector := &countingProjector{projectFn: s.source.project}
	applier := &countingApplier{applyFn: s.target.apply}

	worker := internal.SyncWorker{
		JobStore:   jobStore,
		Dispatcher: dispatchStore,
		Projector:  projector,
		Applier:    applier,
		SyncID:     syncID,
		WorkerID:   "playground-02-worker",
		LockFor:    30 * time.Second,
		PullSize:   len(s.deltas) + 4,
	}

	stats, err := runWorkerLoop(ctx, s, syncID, deltaStore, &worker, applier, projector)
	if err != nil {
		log.Fatalf("scenario failed: %v", err)
	}

	fmt.Println("DeltaFlow playground 02-postgres")
	fmt.Printf("scenario=%s\n", s.name)
	fmt.Printf("enqueued=%d worker_runs=%d upserts=%d deletes=%d ghosts=%d\n", stats.Enqueued, stats.WorkerRuns, stats.Upserts, stats.Deletes, stats.Ghosts)
	fmt.Printf("indexed=%d expected_ghosts=%d\n", len(s.target.docs), s.expectedGhosts)
	printOneSample(s.target.docs)
}

func runWorkerLoop(
	ctx context.Context,
	scenario contactSyncScenario,
	syncID deltaflow.SyncID,
	deltaStore deltaflow.DeltaStore,
	worker *internal.SyncWorker,
	applier *countingApplier,
	projector *countingProjector,
) (runStats, error) {
	stats := runStats{}
	worker.SyncID = syncID

	for _, delta := range scenario.deltas {
		if _, err := deltaStore.Enqueue(ctx, delta); err != nil {
			return stats, err
		}
		stats.Enqueued++
	}

	// For this fixed demo, each RunOnce processes at most one claimed job.
	// Running exactly len(deltas) times keeps the example clean and deterministic.
	for i := 0; i < len(scenario.deltas); i++ {
		if err := worker.RunOnce(ctx); err != nil {
			return stats, err
		}
		stats.WorkerRuns++
	}

	stats.Upserts = applier.upserts
	stats.Deletes = applier.deletes
	stats.Ghosts = projector.ghostDeletes

	return stats, nil
}

func attachRunScopedSyncID(scenario *contactSyncScenario) deltaflow.SyncID {
	runSyncID := deltaflow.SyncID(fmt.Sprintf("contacts-to-crm-cache-%d", time.Now().UTC().UnixNano()))
	for i := range scenario.deltas {
		scenario.deltas[i].SyncID = runSyncID
	}
	return runSyncID
}

type countingProjector struct {
	projectFn     func(context.Context, deltaflow.ProjectionIdentity) (deltaflow.Projection, error)
	ghostDeletes  int
}

func (p *countingProjector) Project(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
	projection, err := p.projectFn(ctx, identity)
	if errors.Is(err, deltaflow.ErrProjectionNotFound) {
		p.ghostDeletes++
	}
	return projection, err
}

type countingApplier struct {
	applyFn func(context.Context, deltaflow.ProjectionOperation) error
	upserts int
	deletes int
}

func (a *countingApplier) Apply(ctx context.Context, op deltaflow.ProjectionOperation) error {
	if err := a.applyFn(ctx, op); err != nil {
		return err
	}

	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		a.upserts++
	case deltaflow.ProjectionOpDelete:
		a.deletes++
	}

	return nil
}
