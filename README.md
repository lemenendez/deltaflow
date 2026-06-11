# DeltaFlow

DeltaFlow is a reconciliation worker.

The core v0.2 focus is still the domain model and in-memory processing.
This branch also includes a Postgres durable-store playground that exercises the v0.3/v0.4 direction.

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
