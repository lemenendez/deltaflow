package main

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/lemenendez/deltaflow/internal"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	"github.com/lemenendez/deltaflow/playground/internal/playpg"
)

type demoResult struct {
	Scenario        *scenario
	Enqueued        int
	WorkerStats     playpg.WorkerLoopStats
	JobCounts       playpg.JobCounts
	ProjectorGhosts int64
	Views           map[string][]byte
	SearchQueue     []string
	RedisOrderQueue []string
	TargetUpserts   int
	TargetDeletes   int
	TargetFailures  int
	Digest          string
	Timings         playpg.RunTimings
	WorkerLogPath   string
}

func runDemo(ctx context.Context, dsn string) (demoResult, error) {
	totalStart := time.Now()
	setupStart := totalStart

	fileLogger, err := playpg.OpenFileLogger(os.Getenv("DELTAFLOW_WORKER_LOG"))
	if err != nil {
		return demoResult{}, err
	}
	defer fileLogger.Close()
	fileLogger.Logger.Info("playground_run_started", "sync_id", syncID, "scenario", "04-postgres-crm")

	stores, err := playpg.OpenStoresWithOptions(ctx, dsn, playpg.OpenStoresOptions{
		MaxAttempts: maxAttempts,
		LeaseLogger: fileLogger.Logger,
	})
	if err != nil {
		return demoResult{}, err
	}
	defer stores.DB.Close()

	if err := playpg.ResetSync(ctx, stores.DB, syncID); err != nil {
		return demoResult{}, err
	}

	scenario, err := buildScenario(ctx, stores)
	if err != nil {
		return demoResult{}, err
	}
	projector := &countingProjector{projectFn: scenario.source.project}
	setupElapsed := time.Since(setupStart)

	var writersDone atomic.Bool
	workerStatsCh := make(chan struct {
		stats playpg.WorkerLoopStats
		err   error
	}, 1)
	go func() {
		stats, err := playpg.RunWorkers(
			ctx,
			workerCount,
			func(workerID string) *internal.SyncWorker {
				worker := playpg.MakeWorker(stores, syncID, workerID, projector, deltaflow.ProjectionApplierFunc(scenario.target.apply), 64)
				worker.Logger = fileLogger.Logger
				return worker
			},
			func(ctx context.Context) (bool, error) {
				if !writersDone.Load() {
					return false, nil
				}
				return playpg.WorkComplete(ctx, stores.DB, syncID)
			},
			func(ctx context.Context) error {
				return playpg.MakeRetryingAvailable(ctx, stores.DB, syncID)
			},
		)
		workerStatsCh <- struct {
			stats playpg.WorkerLoopStats
			err   error
		}{stats: stats, err: err}
	}()

	enqueueStart := time.Now()
	writerResult, err := runWriters(ctx, stores, scenario.source, scenario.events)
	enqueueElapsed := time.Since(enqueueStart)
	writersDone.Store(true)
	if err != nil {
		select {
		case <-workerStatsCh:
		case <-ctx.Done():
		}
		return demoResult{
			Scenario:      scenario,
			Enqueued:      writerResult.Enqueued,
			Timings:       playpg.RunTimings{Setup: setupElapsed, Enqueue: enqueueElapsed, Total: time.Since(totalStart)},
			WorkerLogPath: fileLogger.Path,
		}, err
	}

	drainStart := time.Now()
	workerResult := <-workerStatsCh
	drainElapsed := time.Since(drainStart)
	if workerResult.err != nil {
		return demoResult{}, workerResult.err
	}

	counts, err := playpg.CountJobs(ctx, stores.DB, syncID)
	if err != nil {
		return demoResult{}, err
	}
	views, searchQueue, redisOrderQueue, upserts, deletes, failures := scenario.target.snapshot()
	timings := playpg.RunTimings{
		Setup:   setupElapsed,
		Enqueue: enqueueElapsed,
		Drain:   drainElapsed,
		Total:   time.Since(totalStart),
	}
	fileLogger.Logger.Info("playground_run_completed",
		"sync_id", syncID,
		"enqueued", writerResult.Enqueued,
		"jobs_synced", counts.Synced,
		"jobs_dead", counts.Dead,
		"ghost_jobs", counts.Ghosts,
		"run_once_calls", workerResult.stats.RunOnceCalls,
		"total_ms", timings.Total.Milliseconds(),
	)

	return demoResult{
		Scenario:        scenario,
		Enqueued:        writerResult.Enqueued,
		WorkerStats:     workerResult.stats,
		JobCounts:       counts,
		ProjectorGhosts: projector.ghostCount(),
		Views:           views,
		SearchQueue:     searchQueue,
		RedisOrderQueue: redisOrderQueue,
		TargetUpserts:   upserts,
		TargetDeletes:   deletes,
		TargetFailures:  failures,
		Digest:          playpg.StableDigest(views),
		Timings:         timings,
		WorkerLogPath:   fileLogger.Path,
	}, nil
}
