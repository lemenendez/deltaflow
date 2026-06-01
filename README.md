# DeltaFlow

DeltaFlow is a reconciliation worker.

The core v0.2 focus is still the domain model and in-memory processing.
This branch also includes a Postgres durable-store playground that exercises the v0.3/v0.4 direction.

## Playground

Standalone examples live under `playground/`.

- `playground/01-in-memory`: in-memory latest-state flow using the public DeltaFlow API.
- `playground/02-postgres`: Postgres-backed Contact delta flow using DeltaStore, DispatchStore, and JobStore via docker compose.

The concrete Postgres delta store provides two clear write paths:
- `Enqueue(ctx, delta)` for standalone inserts (tests, backfills, CLI/admin tools).
- `EnqueueInTx(ctx, tx, delta)` when app writes and outbox inserts must share the same SQL transaction.
