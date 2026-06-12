# v0.4.0

DeltaFlow v0.4.0 focuses on Postgres lease hardening and transactional application-write patterns. The release makes worker ownership explicit, adds heartbeat-based lease renewal, improves lease observability, and expands the playgrounds used to validate concurrent writers and workers.

## Highlights

- Added transactional Postgres outbox writes with `DeltaStore.EnqueueInTx(ctx, tx, delta)`, so application writes and delta inserts can commit atomically in the same SQL transaction.
- Added worker lease renewal through `JobStore.RenewLease`, with the worker heartbeat cancelling in-flight work when renewal fails.
- Enforced lease ownership checks on `MarkSynced`, `MarkRetrying`, and `MarkDead` by requiring the owning `worker_id` and an active lease.
- Added structured lease lifecycle logs for claim, renew, reclaim, ownership rejection, and state transition outcomes.
- Added the `LeaseTelemetry` interface for low-cardinality claim, renew, ownership, and reclaim metrics.
- Added optional lease operations APIs for active, expired, and near-expiry lease inspection, plus safe operator actions for expired leases.
- Added v0.4 runbook documentation for worker crashes, heartbeat failures, and ownership conflicts.
- Added concurrent Postgres playgrounds that exercise transactional writes, multiple writers, multiple workers, ghost deletion, retry, and dead-letter flows.

## API Changes

- `JobStore` now includes:
  - `RenewLease(ctx, jobID, workerID, lockFor)`
  - `MarkSynced(ctx, jobID, workerID, ghostDetected)`
  - `MarkRetrying(ctx, jobID, workerID, err, nextRunAt)`
  - `MarkDead(ctx, jobID, workerID, err)`
- `SyncWorkerConfig` now rejects an empty `WorkerID`.
- `LeaseTelemetry` and `NoopLeaseTelemetry()` were added for metrics integration.
- Optional lease query support is exposed through `JobLeaseQueries`:
  - `ListActiveLeases`
  - `ListExpiredProcessingLeases`
  - `ListNearExpiryLeases`
- Optional lease operator support is exposed through `JobLeaseOperatorActions`:
  - `ForceReleaseExpiredLease`
  - `RequeueExpiredLease`

## Postgres Store Changes

- Added transactional delta enqueue support via `EnqueueInTx`.
- Added lease renewal and ownership-checked state transitions to the Postgres `JobStore`.
- Added processing lease operation indexes and migrations for lease inspection and reclaim paths.
- Added active, expired, and near-expiry lease queries with optional `sync_id` filtering.
- Added safe expired-lease operator actions that require an explicit audit reason.

## Observability

- Lease lifecycle logs now use consistent `event` values such as `lease_claimed`, `lease_renewed`, `lease_renew_failed`, `lease_transition_applied`, and `lease_transition_rejected`.
- Core lease log fields were standardized where available: `sync_id`, `job_id`, `worker_id`, `state`, `attempt_count`, `locked_until`, `lease_ms_remaining`, and `reason`.
- Reclaims are observable as `lease_claimed` with `reason=expired_reclaimed`.
- Worker heartbeat events distinguish heartbeat stops from renewal failures.

## Playgrounds

- Added `playground/03-postgres-e-commerce`, a concurrent product-search workload with deterministic data, transactional writes, two DeltaFlow workers, retry, dead-letter, and ghost deletion simulation.
- Added `playground/04-postgres-crm`, a concurrent CRM read-model workload with Redis/OpenSearch-style fanout simulation, transactional writes, two DeltaFlow workers, retry, dead-letter, and ghost deletion simulation.
- Shared Postgres playground helpers were added under `playground/internal/playpg`.

## Documentation

- Updated `docs/DESIGN.md` for the v0.4 lease hardening model and transactional write flow.
- Updated `docs/ROADMAP.md` to reflect the v0.4 scope and follow-on milestones.
- Added `docs/PLAN_v0.4.0.md`.
- Added `docs/RUNBOOK_v0.4.0.md`.
- Updated `README.md` with the new playgrounds, transactional enqueue paths, and development hook setup.

## Developer Tooling

- Added `scripts/setup-git-hooks.sh`.
- Added a pre-commit hook that runs `gofmt`, protects staged Go files from simultaneous unstaged edits, runs `go vet ./...`, and runs `golangci-lint run ./...` when available.

## Upgrade Notes

- Custom `JobStore` implementations must add `RenewLease` and update `MarkSynced`, `MarkRetrying`, and `MarkDead` to accept `workerID`.
- Workers must be configured with a non-empty `WorkerID`.
- Code that performs durable application writes should prefer `EnqueueInTx` when the application mutation and outbox delta must commit together.
- Operators should use the new lease query helpers before manual intervention, and only use force-release or requeue actions on expired leases with an audit reason.

## Validation

- Added and expanded unit tests for in-memory lease renewal, ownership rejection, telemetry, lease queries, and operator actions.
- Expanded Postgres integration tests for lease query and operator behavior.
- Added Postgres playground workloads for realistic concurrent writer and worker validation.
