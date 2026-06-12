# Plan v0.5.0

Goal: formalize the latest-state MVP as a usable public Go API.

## Scope

- [x] Expose `deltaflow.SyncWorker` with `RunOnce(ctx)`.
- [x] Keep `Projector.Project(ctx, identity)` as the latest-state projection boundary.
- [x] Keep `ProjectionApplier.Apply(ctx, operation)` as the derived-system write boundary.
- [x] Treat `ErrProjectionNotFound` as a Delta Ghost and apply a delete operation.
- [x] Keep transactional application writes documented through `postgres.DeltaStore.EnqueueInTx`.
- [x] Use existing playground fake/simple appliers as the canonical examples for now.
- [x] Refresh README and DESIGN language around the current v0.5 shape.
- [ ] Add v0.5 release notes after the docs and public worker changes settle.

## Acceptance Criteria

- Users can wire a worker without importing `internal`.
- The public worker path dispatches pending deltas, claims one job, calls the projector, calls the applier, and records synced/retry/dead state.
- Ghost deletes are visible through `SyncJob.GhostDetected`.
- The README points users to the current latest-state flow and playgrounds.
- The roadmap no longer implies that fake/simple appliers are missing runtime work.

## Deferred

- Long-running public worker loop and shutdown policy.
- CLI/YAML worker runner.
- Redis/Postgres/OpenSearch concrete applier packages.
- Batch claim and worker concurrency controls.
