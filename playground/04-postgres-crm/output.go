package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lemenendez/deltaflow/playground/internal/playpg"
)

func printReport(result demoResult) {
	breakdown := mutationBreakdown(result.Scenario.events)

	fmt.Println("DeltaFlow playground 04-postgres-crm")
	fmt.Println()
	fmt.Println("What happened")
	fmt.Printf("- Source universe: seed %d generated %d users, %d customers, and %d normal orders plus 1 poison order.\n", seed, userCount, customerCount, orderCount)
	fmt.Printf("- Workload size: %d random CRM mutations plus 3 special deltas.\n", mutationCount)
	fmt.Printf("- %d customer-side actors updated durable Postgres CRM rows and wrote outbox deltas with DeltaStore.EnqueueInTx.\n", writerCount)
	fmt.Printf("- %d DeltaFlow workers concurrently dispatched, claimed, projected, and applied those jobs from Postgres.\n", workerCount)
	fmt.Println("- Projector reads the latest CRM row and builds user/customer/order projections.")
	fmt.Println("- Applier simulates Redis views plus OpenSearch/order fanout; no real Redis/OpenSearch connector is used yet.")
	fmt.Println()

	fmt.Println("Workload")
	fmt.Printf("- CRM mutations: %s.\n", formatBreakdown(breakdown))
	fmt.Println("- cus-004: simulated one temporary Redis timeout, then retry succeeded.")
	fmt.Println("- ord-dead-001: simulated permanent target rejection, so the job reached dead-letter after max attempts.")
	fmt.Println("- cus-ghost-001: stale customer view with no source customer, so DeltaFlow issued a delete.")
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

	fmt.Println("Simulated Redis/OpenSearch result")
	fmt.Printf("- Upserts: %d, deletes: %d, target failures observed: %d.\n", result.TargetUpserts, result.TargetDeletes, result.TargetFailures)
	fmt.Printf("- Redis views: %d. OpenSearch queue events: %d. Redis order events: %d.\n", len(result.Views), len(result.SearchQueue), len(result.RedisOrderQueue))
	fmt.Printf("- Redis view digest: %s.\n", result.Digest)
	if key, sample, ok := playpg.FirstSample(result.Views); ok {
		fmt.Printf("- Sample view %s: %s\n", key, sample)
	}
	fmt.Printf("- Last OpenSearch queue events: %s.\n", queueTail(result.SearchQueue, 5))
}

func mutationBreakdown(events []mutation) map[string]int {
	counts := make(map[string]int)
	for _, event := range events {
		key := event.Entity + "." + event.Kind
		counts[key]++
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
