# DeltaFlow

DeltaFlow is a reconciliation worker.

The current focus is the v0.5 latest-state MVP: public projector/applier
interfaces, a public `deltaflow.SyncWorker`, durable Postgres stores, ghost
delete handling, and transactional application outbox writes.

Core flow:

```text
Delta -> SyncJob -> SyncWorker -> Projector.Project -> ProjectionApplier.Apply
```

If `Projector.Project` returns `ErrProjectionNotFound`, the worker treats the
delta as a ghost and applies a delete operation.

## Playground

Standalone examples live under `playground/`.

- `playground/01-in-memory`: in-memory latest-state flow using the public DeltaFlow API.
- `playground/02-postgres`: Postgres-backed Contact delta flow using DeltaStore, DispatchStore, and JobStore via docker compose.
- `playground/03-postgres-e-commerce`: concurrent product-search workload using deterministic fake data, Postgres DeltaStore, two DeltaFlow workers, ghost deletion, retry, and dead-letter simulation.
- `playground/04-postgres-crm`: concurrent CRM read-model workload using deterministic fake data, Postgres DeltaStore, two DeltaFlow workers, Redis/OpenSearch fanout simulation, ghost deletion, retry, and dead-letter simulation.

The concrete Postgres delta store provides two clear write paths:

- `Enqueue(ctx, delta)` for standalone inserts (tests, backfills, CLI/admin tools).
- `EnqueueInTx(ctx, tx, delta)` when app writes and outbox inserts must share the same SQL transaction.

## Development Hooks

Set up the repo pre-commit hook:

```bash
./scripts/setup-git-hooks.sh
```

The hook runs:

- `gofmt -w` on staged Go files (and re-stages formatting changes)
- fails if a staged Go file also has unstaged edits (to protect partial staging)
- `go vet ./...`
- `golangci-lint run ./...` when `golangci-lint` is available on PATH
