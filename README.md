# DeltaFlow

DeltaFlow is a reconciliation worker.

The core v0.2 focus is still the domain model and in-memory processing.
This branch also includes a Postgres durable-store playground that exercises the v0.3/v0.4 direction.

When enqueuing deltas through `DeltaStore`, leave `Delta.ID` empty. Delta IDs are
assigned by the store, and `Enqueue` returns `ErrDeltaIDProvided` if callers
provide a non-empty ID.

When creating jobs through `JobStore`, leave `SyncJob.ID` empty. Job IDs are
assigned by the store, and `Create` returns `ErrJobIDProvided` if callers
provide a non-empty ID.

## Playground

Standalone examples live under `playground/`.

- `playground/01-in-memory`: in-memory latest-state flow using the public DeltaFlow API.
- `playground/02-postgres`: Postgres-backed Contact delta flow using DeltaStore, DispatchStore, and JobStore via docker compose.
