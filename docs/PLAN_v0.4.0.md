# Plan v0.4.0

Goal: Postgres lease hardening + transactional app patterns.

## Scope

- [x] Insert Deltas transactionally with application writes.
- [x] Lease renewal/heartbeat semantics.
- [x] Lease ownership checks on job state transitions.
- [ ] Advanced lease observability and operations.
- [ ] Add new playground for concurrent workload simulation (writers + workers).

## Advanced Lease Observability and Operations (v0.4 Definition)

Telemetry stance for this milestone:

- Use structured logs with Go `log/slog` as the primary signal path.
- Emit low-cardinality counters/timers through a small telemetry interface so Prometheus exporters can be plugged in cleanly.
- Keep concrete Prometheus/Grafana packaging lightweight in v0.4 and expand in v0.9 metrics/logs hardening.

Scope checklist:

- [x] Add lease lifecycle structured log events (concrete `event` values): `lease_claim_rejected`, `lease_claim_failed`, `lease_claim_empty`, `lease_claimed`, `lease_renew_rejected`, `lease_renew_failed`, `lease_renewed`, `lease_transition_rejected`, `lease_transition_applied`, `worker_claim_empty`, `worker_claimed`, `worker_heartbeat_stopped`, `worker_heartbeat_renew_failed`.
- [x] Standardize core lease log fields across stores where applicable: `sync_id`, `job_id`, `worker_id`, `state`, `attempt_count`, `locked_until`, `lease_ms_remaining`, `reason`; lighter worker claim/heartbeat events may emit only the fields available at that point, and reclaim is represented as `event=lease_claimed` with `reason=expired_reclaimed`.
- [x] Add telemetry counters/timers for lease claim/renew/ownership rejection/reclaim outcomes.
- [x] Add operational queries/helpers for: active leases, expired processing leases, near-expiry leases.
- [x] Add safe operator actions for expired leases (force-release/requeue with explicit audit reason).
- [x] Add a short runbook: [worker crash, heartbeat failures, elevated ownership conflicts](RUNBOOK_v0.4.0.md).

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
