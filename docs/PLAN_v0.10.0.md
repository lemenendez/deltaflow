# Plan v0.10.0

Goal: add a SQLite durable-store option for local and embedded usage, while keeping behavioral compatibility with core DeltaStore/JobStore contracts and explicitly limiting runtime concurrency.

## Product Positioning

- Single node only.
- Single DeltaFlow worker process only.
- Default and recommended runtime: one worker goroutine (`workers.concurrency=1`).
- Primary use cases: local development, embedded apps, demos, and small single-tenant deployments.

## Transactional Enqueue Contract

- Delta enqueue is application-managed, not auto-captured by DeltaFlow.
- User code must enqueue explicitly.
- When source tables and DeltaFlow tables are in the same database, enqueue should happen in the same transaction (for example, `EnqueueInTx`) so app writes and outbox writes commit/rollback together.
- When source tables live in a different database, same-transaction atomicity is not available; application code must use an explicit cross-database enqueue pattern (for example, app-owned outbox/relay or commit-then-enqueue with documented recovery semantics).
- DeltaFlow worker/dispatcher consumes already-enqueued deltas; it does not infer or scrape source-table mutations.

## Concurrency Decision (Goroutines)

Decision for v0.10.0:

- Do not support multi-goroutine worker execution for SQLite in this milestone.
- Enforce `workers.concurrency=1` when `store.type=sqlite`.
- Keep `workers.batch_size` support optional, but process claims in one goroutine only.

Why:

- SQLite has database-level write locking semantics (single writer), unlike Postgres row locking.
- Current Postgres claim paths depend on `FOR UPDATE SKIP LOCKED` for low-contention parallel claim.
- Supporting multiple goroutines safely in SQLite would require additional claim/lease contention design that is out of scope for this milestone.

Future reconsideration:

- Re-evaluate goroutine concurrency after measuring real workloads with WAL mode and busy timeout, and after proving no duplicate-claim regressions under concurrent claim attempts.

## Scope

- [x] Add `pkg/connectors/sqlite` DeltaStore implementation.
- [x] Add `pkg/connectors/sqlite` JobStore implementation.
- [x] Add SQLite migration path and schema.
- [x] Add optional SQLite DispatchStore implementation with same outbox semantics.
- [x] Add connector wiring/docs for `store.type=sqlite`.
- [x] Enforce runtime/config guardrails for single-worker operation.
- [x] Add contract/conformance tests for supported SQLite behavior.
- [ ] Add playground coverage for SQLite single-worker mode.
- [ ] Publish migration and operational docs for SQLite limits.
- [ ] Document and exemplify manual enqueue patterns from application code (same-DB transaction and cross-DB explicit enqueue).

## Postgres API Usage vs SQLite Implications

### DeltaStore implications

1. Postgres namespace and type casts
- Current SQL uses schema-qualified tables (`deltaflow.deltaflow_deltas`) and casts (`$1::uuid`, `$4::jsonb`).
- SQLite implication: remove schema qualification and Postgres casts.
- SQLite approach: store IDs as TEXT, JSON as TEXT (canonicalized before write).

2. Returning inserted/updated rows
- Current Postgres path uses `RETURNING` for create/update readback.
- SQLite implication: requires SQLite 3.35+ for `RETURNING`, or fallback to follow-up `SELECT` by id.
- SQLite approach: prefer `RETURNING` when available in CI/runtime baseline.

3. Dispatch pull locking
- Current dispatch path uses `FOR UPDATE SKIP LOCKED` to lock pending deltas safely.
- SQLite implication: no row-level `SKIP LOCKED` equivalent.
- SQLite approach for v0.10.0: single worker process removes inter-worker dispatch races; use `BEGIN IMMEDIATE` + deterministic selection/update in one transaction.

### JobStore implications

1. ClaimNext / ClaimNextBatch semantics
- Current Postgres claim uses CTE + `FOR UPDATE SKIP LOCKED` and supports parallel routines.
- SQLite implication: no row lock skipping model for high-concurrency claim.
- SQLite approach in v0.10.0: keep semantic correctness for one goroutine; claim via transactional `UPDATE ... WHERE id IN (SELECT ... LIMIT N)` (or looped single-claim), then read claimed rows.

2. Lease ownership transitions
- Current transitions (`MarkSynced`, `MarkRetrying`, `MarkDead`, `RequeueClaimed`, `RenewLease`) rely on conditional updates and rows-affected checks.
- SQLite implication: behavior maps well; ownership checks remain implementable.
- SQLite approach: preserve the same ownership predicates and error mapping (`ErrJobNotFound`, `ErrJobLeaseNotOwned`).

3. Outbox uniqueness and idempotent dispatch
- Current Postgres path uses partial unique index + `ON CONFLICT ... DO NOTHING` for outbox `delta_id`.
- SQLite implication: partial indexes are available, but UPSERT targeting may differ by version and SQL shape.
- SQLite approach: use a unique partial index where supported, or equivalent uniqueness strategy with `INSERT OR IGNORE` and explicit validation.

4. Lease operations queries
- Current Postgres lease ops use ordering and indexes including `NULLS FIRST` and partial indexes.
- SQLite implication: indexing and null ordering differ; query plans will differ.
- SQLite approach: keep query semantics, simplify index strategy for single-node use, and document performance envelope.

### Migration/runtime implications

1. Extension/function bootstrap
- Postgres migration bootstraps `pgcrypto` and `uuidv7()` compatibility.
- SQLite implication: no extension bootstrap path like Postgres.
- SQLite approach: generate IDs in application code, not in SQL defaults.

2. Timestamp behavior
- Postgres uses `TIMESTAMPTZ` and `NOW()`.
- SQLite implication: no native timestamptz type.
- SQLite approach: write UTC timestamps from application code in RFC3339 (or unix micros) consistently.

## Config and Guardrails

- [x] Allow `store.type=sqlite` in config validation.
- [ ] Reject/guard unsupported settings for SQLite:
  - [x] `workers.concurrency != 1`
  - [x] multiple DeltaFlow worker processes against the same DB
- [x] Add singleton worker startup guard for SQLite (DB-backed lock/lease) so a second worker fails fast on startup.
- [ ] Add fail-fast user-facing messages in `validate` and `run` paths:
  - [x] `sqlite supports only workers.concurrency=1`
  - [ ] `sqlite does not support multiple competing worker processes`
  - [x] `sqlite worker already running for this database; stop the other worker or use postgres for multi-worker deployments`
- [ ] Document recommended SQLite runtime settings:
  - [ ] WAL mode
  - [ ] busy timeout
  - [ ] single writer expectation

## Testing Plan

- [x] DeltaStore contract tests for enqueue/get/pull/mark-dispatched behavior.
- [x] JobStore contract tests for create/get/claim/renew/finalize behavior.
- [x] Dispatch tests verifying outbox mapping remains idempotent.
- [x] Concurrency negative test: SQLite config rejects `workers.concurrency > 1`.
- [x] Integration-like smoke test with application write + enqueue + worker cycle + final synced state.

## Acceptance Criteria

- [ ] SQLite stores pass core contract tests in single-worker mode.
- [ ] Worker run path is stable and deterministic with `workers.concurrency=1`.
- [ ] Config and docs clearly state unsupported multi-worker/multi-goroutine operation.
- [ ] Existing Postgres behavior remains unchanged.

## Out of Scope

- Multi-process worker coordination for SQLite (no distributed worker mode).
- High-throughput parallel claim optimization for SQLite.
- Declaring SQLite as a Postgres replacement for production fleets.
