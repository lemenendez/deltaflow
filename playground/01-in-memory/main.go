package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type runStats struct {
	Upserts int
	Deletes int
	Ghosts  int
}

func main() {
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
				stats.Ghosts++
				stats.Deletes++
				if err := applier.Apply(ctx, deltaflow.ProjectionOperation{
					Type:     deltaflow.ProjectionOpDelete,
					Identity: identity,
				}); err != nil {
					return stats, err
				}
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
