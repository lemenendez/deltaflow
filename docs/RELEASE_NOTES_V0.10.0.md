# v0.10.0

DeltaFlow v0.10.0 introduces SQLite durable-store support for local and embedded deployments.

This release prioritizes operational clarity over throughput: SQLite support is intentionally single-node and single-worker oriented.

## Highlights

- Added SQLite durable store path for Delta and SyncJob persistence.
- Added SQLite schema/migration path for DeltaFlow tables.
- Added SQLite-focused tests for contract compatibility in single-worker mode.
- Added docs and examples for local/embedded SQLite usage, including `docs/SQLITE.md` and `playground/05-sqlite`.

## Transactional Enqueue Clarification

- Delta enqueue remains explicit and application-driven.
- DeltaFlow does not automatically capture source-table writes.
- When source writes and DeltaFlow tables share one database, recommended pattern: source write + delta enqueue in one transaction so they commit or roll back together.
- When source writes are in a different database, use explicit cross-database enqueue orchestration (for example, app-owned outbox/relay or commit-then-enqueue with recovery).
- Worker and dispatcher operate on persisted deltas only.

## Concurrency and Worker Model

SQLite in v0.10.0 is intentionally constrained:

- One DeltaFlow worker process per SQLite database.
- One worker goroutine (`workers.concurrency=1`) for supported operation.
- No distributed lease coordination across multiple competing worker processes.

Rationale:

- Postgres worker parallelism relies on row-level locking and `FOR UPDATE SKIP LOCKED` claim behavior.
- SQLite uses a different locking model (single-writer behavior), so this release keeps the runtime model conservative and predictable.

## Postgres-to-SQLite Behavioral Notes

- SQL schema qualifiers and Postgres casts (`::uuid`, `::jsonb`) are removed in SQLite paths.
- Postgres claim/dispatch patterns using `FOR UPDATE SKIP LOCKED` are replaced with single-worker transactional claim/dispatch flow.
- Outbox idempotency remains enforced via uniqueness constraints and conflict-safe insert strategy.
- Lease ownership checks (`locked_by`, `locked_until`) remain part of finalization safety.

## Limitations

- Not intended for multiple hosts or distributed worker fleets.
- Not intended for high-throughput parallel claim workloads.
- Not a replacement for Postgres in production multi-worker deployments.

## Recommended Use Cases

- Local development.
- Embedded/single-binary applications.
- Demos and low-scale single-tenant deployments.

## Upgrade and Config Notes

- Use `store.type=sqlite` with a SQLite DSN/path.
- Keep `workers.concurrency=1`.
- Prefer WAL mode and a configured busy timeout for better local stability.
- Use `EnqueueInTx` when source writes and DeltaFlow tables share the same SQLite database transaction.

## Guardrail Messages

When SQLite is selected, DeltaFlow should fail fast with clear guidance for unsupported runtime shapes:

- `sqlite supports only workers.concurrency=1`
- `sqlite does not support multiple competing worker processes`
- `sqlite worker already running for this database; stop the other worker or use postgres for multi-worker deployments`

## Verification

Suggested checks:

- `go test ./...`
- SQLite contract and connector tests.
- End-to-end flow: app write -> delta enqueue -> dispatch -> claim -> apply -> synced.

## Deferred

- SQLite multi-goroutine worker support.
- SQLite multi-process worker coordination (no distributed worker mode).
- Production fleet guidance beyond single-node/local scenarios.
