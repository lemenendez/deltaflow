# v0.5.0

DeltaFlow v0.5.0 formalizes the latest-state MVP as a usable public Go API. The release exposes the worker runtime contract from `pkg/deltaflow`, documents projector and applier boundaries, and makes ghost deletion behavior part of the supported worker flow.

## Highlights

- Exposed `deltaflow.SyncWorker` with `RunOnce(ctx)` so applications can run the latest-state worker without importing `internal`.
- Kept `Projector.Project(ctx, identity)` as the latest-state projection boundary.
- Kept `ProjectionApplier.Apply(ctx, operation)` as the derived-system write boundary.
- Treats `ErrProjectionNotFound` as a Delta Ghost and applies a delete operation.
- Keeps transactional application writes documented through `postgres.DeltaStore.EnqueueInTx`.
- Uses the existing in-memory and Postgres-backed playgrounds as the canonical examples for wiring workers and fake/simple appliers.

## Public API

- `deltaflow.SyncWorker` dispatches pending deltas, claims one sync job, projects latest state, applies the resulting operation, and records the outcome.
- `deltaflow.SyncWorker.RunOnce(ctx)` processes at most one claimed job.
- `deltaflow.SyncWorkerConfig` validates the required runtime config: `sync_id`, `worker_id`, and positive `lock_for` duration.
- `deltaflow.Projector` remains:
  - `Project(ctx context.Context, identity ProjectionIdentity) (Projection, error)`
- `deltaflow.ProjectionApplier` remains:
  - `Apply(ctx context.Context, op ProjectionOperation) error`
- `deltaflow.ProjectorFunc` and `deltaflow.ProjectionApplierFunc` are available for lightweight wiring and tests.

## Worker Behavior

- Upsert path:
  - dispatch pending deltas through `DispatchStore`
  - claim a `SyncJob`
  - project latest state
  - apply `ProjectionOpUpsert`
  - mark the job synced
- Ghost delete path:
  - if the projector returns `ErrProjectionNotFound`, apply `ProjectionOpDelete`
  - mark the job synced with `GhostDetected=true`
- Failure path:
  - failed projection or apply moves the job to retrying while attempts remain
  - exhausted attempts move the job to dead
  - lease heartbeat failures are treated as worker failures and recorded through the same retry/dead path

## Documentation

- Refreshed `README.md` around the current latest-state flow.
- Refreshed `docs/DESIGN.md` to describe v0.5 as the current latest-state MVP.
- Updated `docs/ROADMAP.md` so follow-on work starts after the public worker API shape.
- Added `docs/PLAN_v0.5.0.md`.

## Acceptance Coverage

- Added named v0.5 acceptance tests for worker behavior:
  - `TestV05AcceptanceWorkerUpsertPath`
  - `TestV05AcceptanceWorkerGhostDeletePath`
  - `TestV05AcceptanceWorkerFailedApplyRetryAndDeadBehavior`
- Added Postgres transactional enqueue acceptance coverage:
  - `TestPostgresContainer_V05AcceptanceTransactionalEnqueueContract`

## Deferred

- Long-running public worker loop and shutdown policy.
- CLI/YAML worker runner.
- Concrete Redis/Postgres/OpenSearch or Elasticsearch applier packages.
- Worker throughput, batching, and configurable concurrency.
