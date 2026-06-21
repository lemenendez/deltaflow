package main

import (
	"fmt"
	"sort"
	"strings"

	hostpkg "github.com/lemenendez/deltaflow/internal/host"
)

func printReport(result demoResult) {
	breakdown := mutationBreakdown(result.Scenario.events)
	retryProductID := retryProductID(result.Scenario.events)

	fmt.Println("DeltaFlow playground 03-postgres-e-commerce")
	fmt.Println()
	fmt.Println("What happened")
	fmt.Printf("- Source universe: seed %d generated %d normal products plus 1 poison product for dead-letter behavior.\n", seed, productCount)
	fmt.Printf("- Workload size: %d random product mutations plus 3 special deltas.\n", mutationCount)
	fmt.Printf("- %d application-side actors updated durable Postgres product rows and wrote outbox deltas with DeltaStore.EnqueueInTx.\n", writerCount)
	fmt.Printf("- DeltaFlow workers ran with workers.concurrency=%d and workers.batch_size=%d.\n", workerConcurrency, workerBatchSize)
	fmt.Println("- Projector reads the latest product row and builds ProductSearchDocument.")
	if elasticsearchEndpoint == "" {
		fmt.Println("- Applier is the local Elasticsearch simulator because DELTAFLOW_ES_ENDPOINT is not set.")
	} else {
		fmt.Printf("- Applier writes ProductSearchDocument operations to Elasticsearch at %s.\n", elasticsearchEndpoint)
	}
	fmt.Println()

	fmt.Println("Workload")
	fmt.Printf("- Product mutations: %s.\n", formatBreakdown(breakdown))
	if workerMaxAttempts >= 2 {
		fmt.Printf("- %s: simulated one temporary Elasticsearch 429, then retry succeeded.\n", retryProductID)
	} else {
		fmt.Printf("- %s: simulated one temporary Elasticsearch 429, but workers.max_attempts=%d marks the job dead immediately.\n", retryProductID, workerMaxAttempts)
	}
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
		hostpkg.FormatDuration(result.Timings.Setup),
		hostpkg.FormatDuration(result.Timings.Enqueue),
		hostpkg.FormatDuration(result.Timings.Drain),
		hostpkg.FormatDuration(result.Timings.Total),
	)
	fmt.Printf("- Enqueue throughput: %.1f deltas/sec. Drain throughput: %.1f terminal jobs/sec.\n",
		hostpkg.PerSecond(result.Enqueued, result.Timings.Enqueue),
		hostpkg.PerSecond(result.JobCounts.Synced+result.JobCounts.Dead, result.Timings.Drain),
	)
	fmt.Printf("- Worker and lease log: %s.\n", result.WorkerLogPath)
	fmt.Println()

	if elasticsearchEndpoint == "" {
		fmt.Println("Simulated Elasticsearch result")
	} else {
		fmt.Println("Elasticsearch result")
	}
	fmt.Printf("- Upserts: %d, deletes: %d, target failures observed: %d.\n", result.TargetUpserts, result.TargetDeletes, result.TargetFailures)
	fmt.Printf("- Indexed product docs: %d. Digest: %s.\n", len(result.Docs), result.Digest)
	fmt.Println("- Note: only products touched by successful deltas appear in the index.")
	if key, sample, ok := hostpkg.FirstSample(result.Docs); ok {
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

func retryProductID(events []mutation) string {
	for _, event := range events {
		if event.Seq == mutationCount+1 {
			return event.ProductID
		}
	}
	return "retry-product"
}
