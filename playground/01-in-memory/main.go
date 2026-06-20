package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type runStats struct {
	Upserts int
	Deletes int
	Ghosts  int
}

func main() {
	mode := flag.String("mode", "demo", "run mode: demo or bench")
	seed := flag.Int64("seed", 42, "deterministic benchmark seed")
	universe := flag.Int("universe", 1000, "number of source entities")
	mutations := flag.Int("mutations", 100000, "number of queued jobs to process")
	ghostEvery := flag.Int("ghost-every", 10, "every Nth mutation uses a missing source key; 0 disables ghosts")
	concurrency := flag.String("concurrency", "1,2,4,8", "comma-separated worker concurrency values")
	batchSize := flag.String("batch", "1,8,16,32", "comma-separated worker batch size values")
	lockFor := flag.Duration("lock-for", 2*time.Minute, "lease duration for benchmark jobs")
	flag.Parse()

	if *mode == "bench" {
		cfg := benchmarkConfig{
			Seed:         *seed,
			Universe:     *universe,
			Mutations:    *mutations,
			GhostEvery:   *ghostEvery,
			Concurrency:  *concurrency,
			BatchSize:    *batchSize,
			LockFor:      *lockFor,
			WorkerIDBase: "bench-worker",
		}
		if err := runBenchmark(context.Background(), cfg); err != nil {
			log.Fatalf("benchmark failed: %v", err)
		}
		return
	}

	s := buildDemoScenario()
	projector := deltaflow.ProjectorFunc(s.source.project)
	applier := deltaflow.ProjectionApplierFunc(s.target.apply)

	stats, err := runDeltas(context.Background(), s.deltas, projector, applier)
	if err != nil {
		log.Fatalf("processing failed: %v", err)
	}

	fmt.Printf("DeltaFlow playground 01-in-memory\n")
	fmt.Printf("processed=%d upserts=%d deletes=%d ghosts=%d indexed=%d\n", len(s.deltas), stats.Upserts, stats.Deletes, stats.Ghosts, len(s.target.docs))
	fmt.Printf("scenario=%s source_missing=%d\n", s.name, s.ghostCount)
	printOneSample(s.target.docs)
}

func runDeltas(ctx context.Context, deltas []deltaflow.Delta, projector deltaflow.Projector, applier deltaflow.ProjectionApplier) (runStats, error) {
	stats := runStats{}
	for _, delta := range deltas {
		identity := deltaflow.ProjectionIdentity{
			Type: delta.ProjectionType,
			Key:  delta.ProjectionKey,
		}

		projection, err := projector.Project(ctx, identity)
		if err != nil {
			if errors.Is(err, deltaflow.ErrProjectionNotFound) {
				if err := applier.Apply(ctx, deltaflow.ProjectionOperation{
					Type:     deltaflow.ProjectionOpDelete,
					Identity: identity,
				}); err != nil {
					return stats, err
				}
				stats.Ghosts++
				stats.Deletes++
				continue
			}
			return stats, err
		}

		if err := applier.Apply(ctx, deltaflow.ProjectionOperation{
			Type:       deltaflow.ProjectionOpUpsert,
			Identity:   identity,
			Projection: &projection,
		}); err != nil {
			return stats, err
		}
		stats.Upserts++
	}

	return stats, nil
}
