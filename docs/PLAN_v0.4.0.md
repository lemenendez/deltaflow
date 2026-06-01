# Plan v0.4.0

Goal: Postgres lease hardening + transactional app patterns.

## Scope

- [x] Insert Deltas transactionally with application writes.
- [ ] Lease renewal/heartbeat semantics.
- [ ] Lease ownership checks on job state transitions.
- [ ] Advanced lease observability and operations.
- [ ] Add new playground for concurrent workload simulation (writers + workers).

## New Playground (Proposed)

- Name: playground/03-concurrency-load
- Purpose: validate Go concurrency behavior under realistic outbox/job pressure.
- Workload model:
	- multiple goroutines writing Deltas into outbox
	- multiple workers claiming/processing SyncJobs
	- optional batch processing mode for dispatch/claim loops
- Coordination primitives:
	- channels for work/control signaling
	- sync.WaitGroup for lifecycle coordination
	- context cancellation for clean shutdown
- Measurements (console summary is enough):
	- produced deltas/sec
	- claimed jobs/sec
	- end-to-end processing latency (p50/p95)
	- retry/dead counters under injected failures

## Done Criteria

- All scope items implemented.
- Relevant unit/integration tests updated and passing.
- DESIGN and ROADMAP docs aligned with final behavior.
- Playground runs with configurable knobs (writers, workers, batch size, duration).
- A short README explains how to run the workload and read the metrics.
