package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lemenendez/deltaflow/playground/internal/playpg"
)

func printReport(result demoResult) {
	breakdown := mutationBreakdown(result.Scenario.events)

	fmt.Println("DeltaFlow playground 03-postgres-e-commerce")
	fmt.Println()
	fmt.Println("What happened")
	fmt.Printf("- Source universe: seed %d generated %d normal products plus 1 poison product for dead-letter behavior.\n", seed, productCount)
	fmt.Printf("- Workload size: %d random product mutations plus 3 special deltas.\n", mutationCount)
	fmt.Printf("- %d customer-side actors updated durable Postgres product rows and wrote outbox deltas with DeltaStore.EnqueueInTx.\n", writerCount)
	fmt.Printf("- %d DeltaFlow workers concurrently dispatched, claimed, projected, and applied those jobs from Postgres.\n", workerCount)
	fmt.Println("- Projector reads the latest product row and builds ProductSearchDocument.")
	fmt.Println("- Applier is a simulated Elasticsearch adapter; no real Elasticsearch connector is used yet.")
	fmt.Println()

	fmt.Println("Workload")
	fmt.Printf("- Product mutations: %s.\n", formatBreakdown(breakdown))
	fmt.Println("- sku-004: simulated one temporary Elasticsearch 429, then retry succeeded.")
	fmt.Println("- sku-dead-001: simulated permanent target rejection, so the job reached dead-letter after max attempts.")
	fmt.Println("- sku-ghost-001: stale search document with no source product, so DeltaFlow issued a delete.")
	fmt.Println()

	fmt.Println("DeltaFlow result")
	fmt.Printf("- Deltas enqueued: %d of %d planned mutations.\n", result.Enqueued, len(result.Scenario.events))
	fmt.Printf("- Jobs synced: %d, dead-lettered: %d, ghosts deleted: %d.\n", result.JobCounts.Synced, result.JobCounts.Dead, result.JobCounts.Ghosts)
	fmt.Printf("- Queue drained: pending=%d retrying=%d processing=%d.\n", result.JobCounts.Pending, result.JobCounts.Retrying, result.JobCounts.Processing)
	fmt.Printf("- Worker RunOnce calls: %d.\n", result.WorkerStats.RunOnceCalls)
	fmt.Println()

	fmt.Println("Timing")
	fmt.Printf("- Setup: %s. Enqueue: %s. Worker drain: %s. Total: %s.\n",
		playpg.FormatDuration(result.Timings.Setup),
		playpg.FormatDuration(result.Timings.Enqueue),
		playpg.FormatDuration(result.Timings.Drain),
		playpg.FormatDuration(result.Timings.Total),
	)
	fmt.Printf("- Enqueue throughput: %.1f deltas/sec. Drain throughput: %.1f terminal jobs/sec.\n",
		playpg.PerSecond(result.Enqueued, result.Timings.Enqueue),
		playpg.PerSecond(result.JobCounts.Synced+result.JobCounts.Dead, result.Timings.Drain),
	)
	fmt.Printf("- Worker and lease log: %s.\n", result.WorkerLogPath)
	fmt.Println()

	fmt.Println("Simulated Elasticsearch result")
	fmt.Printf("- Upserts: %d, deletes: %d, target failures observed: %d.\n", result.TargetUpserts, result.TargetDeletes, result.TargetFailures)
	fmt.Printf("- Indexed product docs: %d. Digest: %s.\n", len(result.Docs), result.Digest)
	fmt.Println("- Note: only products touched by successful deltas appear in the simulated index.")
	if key, sample, ok := playpg.FirstSample(result.Docs); ok {
		fmt.Printf("- Sample doc %s: %s\n", key, sample)
	}
	printTopInventoryDocs(result.Docs, 3)
}

func mutationBreakdown(events []mutation) map[string]int {
	counts := make(map[string]int)
	for _, event := range events {
		counts[event.Kind]++
	}
	return counts
}

func formatBreakdown(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return strings.Join(parts, ", ")
}
