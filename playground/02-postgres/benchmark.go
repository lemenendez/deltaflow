package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type postgresBenchmarkConfig struct {
	Seed         int64
	Universe     int
	Mutations    int
	GhostEvery   int
	Concurrency  string
	BatchSize    string
	LockFor      time.Duration
	WorkerIDBase string
}

type postgresBenchScenario struct {
	Seed       int64
	Universe   int
	Mutations  int
	GhostCount int
	Source     *contactSourceStore
	Target     *contactTargetIndex
	Deltas     []deltaflow.Delta
}

type postgresBenchResult struct {
	Concurrency int
	BatchSize   int
	Duration    time.Duration
	JobsPerSec  float64
	WorkerRuns  int
	Synced      int
	Dead        int
	Retrying    int
	Ghosts      int
}

func runPostgresBenchmark(ctx context.Context, db *sql.DB, cfg postgresBenchmarkConfig) error {
	if cfg.Universe <= 0 {
		return errors.New("universe must be positive")
	}
	if cfg.Mutations <= 0 {
		return errors.New("mutations must be positive")
	}
	if cfg.LockFor <= 0 {
		return errors.New("lock-for must be positive")
	}
	if strings.TrimSpace(cfg.WorkerIDBase) == "" {
		cfg.WorkerIDBase = "playground-02-bench-worker"
	}

	concurrencyValues, err := parseIntList(cfg.Concurrency)
	if err != nil {
		return fmt.Errorf("parse concurrency: %w", err)
	}
	batchValues, err := parseIntList(cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("parse batch sizes: %w", err)
	}

	deltaStore := pgstore.NewDeltaStore(db, connectors.DeltaStoreConfig{})
	jobStore := pgstore.NewJobStore(db, pgstore.JobStoreConfig{MaxAttempts: 3})
	dispatchStore := pgstore.NewDispatchStore(deltaStore, jobStore, pgstore.DispatchStoreConfig{})

	scenario := buildPostgresBenchmarkScenario(cfg.Seed, cfg.Universe, cfg.Mutations, cfg.GhostEvery)
	fmt.Println("DeltaFlow playground 02-postgres benchmark")
	fmt.Printf("seed=%d universe=%d mutations=%d ghosts=%d\n", scenario.Seed, scenario.Universe, scenario.Mutations, scenario.GhostCount)
	fmt.Printf("baseline=(concurrency=1,batch=1) candidates=(%s)x(%s)\n\n", cfg.Concurrency, cfg.BatchSize)

	pairs := make([]struct {
		Concurrency int
		BatchSize   int
	}, 0, len(concurrencyValues)*len(batchValues)+1)
	pairs = append(pairs, struct {
		Concurrency int
		BatchSize   int
	}{Concurrency: 1, BatchSize: 1})
	for _, c := range concurrencyValues {
		for _, b := range batchValues {
			if c == 1 && b == 1 {
				continue
			}
			pairs = append(pairs, struct {
				Concurrency int
				BatchSize   int
			}{Concurrency: c, BatchSize: b})
		}
	}

	results := make([]postgresBenchResult, 0, len(pairs))
	for _, pair := range pairs {
		fmt.Printf("running case c=%d b=%d ...\n", pair.Concurrency, pair.BatchSize)
		result, err := runPostgresBenchmarkCase(ctx, deltaStore, jobStore, dispatchStore, scenario, pair.Concurrency, pair.BatchSize, cfg.LockFor, cfg.WorkerIDBase)
		if err != nil {
			return err
		}
		fmt.Printf("finished case c=%d b=%d in %s (worker_runs=%d)\n", result.Concurrency, result.BatchSize, result.Duration.Round(time.Millisecond), result.WorkerRuns)
		results = append(results, result)
	}

	baseline := results[0]
	fmt.Printf("case c=%d b=%d  duration=%s  jobs_per_sec=%.2f  worker_runs=%d  synced=%d dead=%d retrying=%d ghosts=%d\n",
		baseline.Concurrency, baseline.BatchSize, baseline.Duration.Round(time.Millisecond), baseline.JobsPerSec, baseline.WorkerRuns, baseline.Synced, baseline.Dead, baseline.Retrying, baseline.Ghosts)
	for i := 1; i < len(results); i++ {
		r := results[i]
		relative := baseline.Duration.Seconds() / r.Duration.Seconds()
		fmt.Printf("case c=%d b=%d  duration=%s  jobs_per_sec=%.2f  speedup=%.2fx  worker_runs=%d  synced=%d dead=%d retrying=%d ghosts=%d\n",
			r.Concurrency, r.BatchSize, r.Duration.Round(time.Millisecond), r.JobsPerSec, relative, r.WorkerRuns, r.Synced, r.Dead, r.Retrying, r.Ghosts)
	}

	fmt.Println()
	fmt.Println("Tip: keep seed, universe and mutations fixed while tuning concurrency and batch size.")
	return nil
}

func runPostgresBenchmarkCase(
	ctx context.Context,
	deltaStore deltaflow.DeltaStore,
	jobStore deltaflow.JobStore,
	dispatchStore deltaflow.DispatchStore,
	scenario postgresBenchScenario,
	concurrency int,
	batchSize int,
	lockFor time.Duration,
	workerIDBase string,
) (postgresBenchResult, error) {
	syncID := deltaflow.SyncID(fmt.Sprintf("contacts-bench-%d-c%d-b%d", time.Now().UTC().UnixNano(), concurrency, batchSize))
	target := scenario.Target.cloneEmpty()
	projector := &countingProjector{projectFn: scenario.Source.project}
	applier := &countingApplier{applyFn: target.apply}

	worker := &deltaflow.SyncWorker{
		JobStore:    jobStore,
		Dispatcher:  dispatchStore,
		Projector:   projector,
		Applier:     applier,
		SyncID:      syncID,
		WorkerID:    workerIDBase,
		LockFor:     lockFor,
		PullSize:    max(1024, concurrency*batchSize*8),
		BatchSize:   batchSize,
		Concurrency: concurrency,
	}

	for i := range scenario.Deltas {
		d := scenario.Deltas[i]
		d.SyncID = syncID
		if _, err := deltaStore.Enqueue(ctx, d); err != nil {
			return postgresBenchResult{}, fmt.Errorf("enqueue delta %d: %w", i+1, err)
		}
	}

	start := time.Now()
	workerRuns, err := runUntilComplete(ctx, worker, scenario.Mutations, syncID)
	if err != nil {
		return postgresBenchResult{}, err
	}
	duration := time.Since(start)

	counts, err := queryJobStateCounts(ctx, syncID)
	if err != nil {
		return postgresBenchResult{}, err
	}
	jobsPerSec := float64(scenario.Mutations) / duration.Seconds()

	return postgresBenchResult{
		Concurrency: concurrency,
		BatchSize:   batchSize,
		Duration:    duration,
		JobsPerSec:  jobsPerSec,
		WorkerRuns:  workerRuns,
		Synced:      counts.Synced,
		Dead:        counts.Dead,
		Retrying:    counts.Retrying,
		Ghosts:      projector.ghostDeletes,
	}, nil
}

type jobStateCounts struct {
	Synced   int
	Dead     int
	Retrying int
}

func queryJobStateCounts(ctx context.Context, syncID deltaflow.SyncID) (jobStateCounts, error) {
	counts := jobStateCounts{}
	row := queryDB.QueryRowContext(ctx, `
SELECT
	COUNT(*) FILTER (WHERE state = 'synced') AS synced_count,
	COUNT(*) FILTER (WHERE state = 'dead') AS dead_count,
	COUNT(*) FILTER (WHERE state = 'retrying') AS retrying_count
FROM deltaflow.deltaflow_sync_jobs
WHERE sync_id = $1`, syncID)
	if err := row.Scan(&counts.Synced, &counts.Dead, &counts.Retrying); err != nil {
		return jobStateCounts{}, err
	}
	return counts, nil
}

var queryDB *sql.DB

func runUntilComplete(ctx context.Context, worker *deltaflow.SyncWorker, totalJobs int, syncID deltaflow.SyncID) (int, error) {
	maxRuns := totalJobs + 5000
	checkEvery := max(16, worker.Concurrency*worker.BatchSize*2)
	if checkEvery <= 0 {
		checkEvery = 16
	}
	for run := 1; run <= maxRuns; run++ {
		if err := worker.RunOnce(ctx); err != nil {
			return run, err
		}
		if run%checkEvery != 0 && run != maxRuns {
			continue
		}
		counts, err := queryJobStateCounts(ctx, syncID)
		if err != nil {
			return run, err
		}
		if counts.Synced+counts.Dead >= totalJobs {
			return run, nil
		}
	}
	return maxRuns, fmt.Errorf("benchmark did not complete after %d worker runs", maxRuns)
}

func buildPostgresBenchmarkScenario(seed int64, universe, mutations, ghostEvery int) postgresBenchScenario {
	rng := rand.New(rand.NewSource(seed))
	source := &contactSourceStore{contacts: make(map[string]ContactProfile, universe)}
	for i := 0; i < universe; i++ {
		id := fmt.Sprintf("con-%06d", i+1)
		source.contacts[id] = ContactProfile{
			ContactID: id,
			FullName:  fmt.Sprintf("Contact %06d", i+1),
			Email:     fmt.Sprintf("contact-%06d@example.com", i+1),
			Phone:     fmt.Sprintf("+1-555-%04d", i%10000),
			UpdatedAt: fixedTime(i + 1).Format(time.RFC3339),
		}
	}

	deltas := make([]deltaflow.Delta, 0, mutations)
	ghostCount := 0
	for i := 0; i < mutations; i++ {
		var contactID string
		if ghostEvery > 0 && (i+1)%ghostEvery == 0 {
			ghostCount++
			contactID = fmt.Sprintf("ghost-%06d", i+1)
		} else {
			pick := rng.Intn(universe) + 1
			contactID = fmt.Sprintf("con-%06d", pick)
		}
		raw, err := json.Marshal(contactID)
		if err != nil {
			panic(err)
		}
		now := fixedTime(i + 1)
		deltas = append(deltas, deltaflow.Delta{
			SyncID:         deltaflow.SyncID("contacts-bench-template"),
			Origin:         deltaflow.OriginOperationUpdated,
			ProjectionType: deltaflow.ProjectionType("Contact"),
			ProjectionKey:  deltaflow.ProjectionKey{"contact_id": raw},
			State:          deltaflow.DeltaPending,
			OccurredAt:     now,
			CreatedAt:      now,
			Metadata:       map[string]any{"example": "02-postgres-bench"},
		})
	}

	return postgresBenchScenario{
		Seed:       seed,
		Universe:   universe,
		Mutations:  mutations,
		GhostCount: ghostCount,
		Source:     source,
		Target:     &contactTargetIndex{docs: make(map[string][]byte, universe)},
		Deltas:     deltas,
	}
}

func parseIntList(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	out := make([]int, 0, len(parts))
	seen := make(map[int]struct{}, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		n, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("%q is not an integer", trimmed)
		}
		if n <= 0 {
			return nil, fmt.Errorf("%q must be positive", trimmed)
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, errors.New("no values provided")
	}
	sort.Ints(out)
	return out, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
