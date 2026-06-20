package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
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
	mode := flag.String("mode", "demo", "run mode: demo or bench")
	seed := flag.Int64("seed", 42, "deterministic benchmark seed")
	universe := flag.Int("universe", 1000, "number of source entities")
	mutations := flag.Int("mutations", 50000, "number of deltas/jobs to process")
	ghostEvery := flag.Int("ghost-every", 10, "every Nth mutation uses a missing source key; 0 disables ghosts")
	concurrency := flag.String("concurrency", "1,2,4,8", "comma-separated worker concurrency values")
	batchSize := flag.String("batch", "1,8,16,32", "comma-separated worker batch size values")
	lockFor := flag.Duration("lock-for", 30*time.Second, "lease duration for benchmark jobs")
	flag.Parse()

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
	queryDB = db

	if *mode == "bench" {
		cfg := postgresBenchmarkConfig{
			Seed:         *seed,
			Universe:     *universe,
			Mutations:    *mutations,
			GhostEvery:   *ghostEvery,
			Concurrency:  *concurrency,
			BatchSize:    *batchSize,
			LockFor:      *lockFor,
			WorkerIDBase: "playground-02-bench-worker",
		}
		if err := runPostgresBenchmark(ctx, db, cfg); err != nil {
			log.Fatalf("benchmark failed: %v", err)
		}
		return
	}

	deltaStore := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := pgstore.NewJobStore(db, pgstore.JobStoreConfig{MaxAttempts: 3})
	dispatchStore := pgstore.NewDispatchStore(deltaStore, jobStore, pgstore.DispatchStoreConfig{})

	s := buildDemoScenario()
	syncID := attachRunScopedSyncID(&s)
	projector := &countingProjector{projectFn: s.source.project}
	applier := &countingApplier{applyFn: s.target.apply}

	worker := deltaflow.SyncWorker{
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
	worker *deltaflow.SyncWorker,
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
	projectFn    func(context.Context, deltaflow.ProjectionIdentity) (deltaflow.Projection, error)
	ghostDeletes int
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
