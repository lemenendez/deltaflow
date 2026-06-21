# DeltaFlow

DeltaFlow is a reconciliation worker.

The current focus is the latest-state worker path: public projector/applier
interfaces, a public `deltaflow.SyncWorker`, durable Postgres and SQLite
stores, ghost delete handling, transactional application outbox writes, and
concrete Elasticsearch `ProjectionApplier`.

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
- `playground/03-postgres-e-commerce`: concurrent product-search workload using deterministic fake data, Postgres DeltaStore, two DeltaFlow workers, Elasticsearch, ghost deletion, retry, and dead-letter simulation.
- `playground/04-postgres-crm`: concurrent CRM read-model workload using deterministic fake data, Postgres DeltaStore, two DeltaFlow workers, simulated Redis views plus Elasticsearch search fanout, ghost deletion, retry, and dead-letter simulation.
- `playground/05-sqlite`: SQLite durable-store example for the supported single-node / single-worker model, with same-transaction source write + delta enqueue and a simple in-memory applier.

The concrete Postgres delta store provides two clear write paths:

- `Enqueue(ctx, delta)` for standalone inserts (tests, backfills, CLI/admin tools).
- `EnqueueInTx(ctx, tx, delta)` when app writes and outbox inserts must share the same SQL transaction.

The concrete SQLite delta store provides the same two write paths for local or embedded deployments:

- `Enqueue(ctx, delta)` for standalone inserts.
- `EnqueueInTx(ctx, tx, delta)` when the source write and DeltaFlow tables share one SQLite transaction.

SQLite usage notes live in `docs/SQLITE.md`.

The concrete Elasticsearch applier lives in `pkg/connectors/elasticsearch` and
maps `ProjectionOpUpsert`/`ProjectionOpDelete` to idempotent Elasticsearch
document writes.

The Elasticsearch playgrounds use that public applier directly. Their
`elasticsearch_target.go` files are demo glue around the applier: index
reset/seed, failure simulation, counters, snapshots, and local fallback
behavior. Application projectors and target-specific document shape remain
custom code owned by the application.

## CLI

The CLI validates one minimal YAML shape, applies embedded schema migrations,
and starts the v0.9 runtime wiring path with `run`:

```bash
go run ./cmd/deltaflow validate --config ./cmd/deltaflow/deltaflow.yaml
go run ./cmd/deltaflow migrate --config ./cmd/deltaflow/deltaflow.yaml
go run ./cmd/deltaflow run --config ./cmd/deltaflow/deltaflow.yaml
```

For SQLite, a sample config is available at `cmd/deltaflow/deltaflow.sqlite.yaml`.

Config loading expands environment variables with Go's `os.ExpandEnv`, so both
`${VAR}` and `$VAR` placeholders are supported. Expansion runs before YAML
parsing, so quoted YAML strings are expanded too; avoid literal `$NAME`
sequences in config values such as DSNs.

`run` currently executes one worker cycle per configured pipeline. Runtime
registrations are explicit and map-based. DeltaFlow does not infer application
projector wiring from YAML. Registration is done during startup before `run`,
and duplicate names fail fast.

Worker sizing notes for `run`:

- `workers.concurrency` controls how many goroutines process jobs per pipeline cycle.
- When `workers.concurrency > 1`, `Projector.Project` and `ProjectionApplier.Apply` can run concurrently and must be safe for concurrent use (or wrapped to be serialized).
- `workers.batch_size` controls how many jobs each goroutine can claim per cycle.
- `workers.lock_for` should be sized to exceed worst-case per-goroutine batch drain time. A practical bound is roughly `batch_size * max_job_time` per goroutine. If leases expire before a claimed job starts, ownership can be lost and the cycle may requeue leftovers.
- `workers.pull_size` is optional. When omitted, the worker derives dispatch pull size as `concurrency * batch_size`.
- Set `workers.pull_size` explicitly only when you need tighter or looser dispatch limits than the derived default.

SQLite-specific runtime notes:

- `store.type=sqlite` supports only `workers.concurrency=1`.
- SQLite does not support multiple competing worker processes against the same database.
- Prefer WAL mode and a configured busy timeout for local stability.

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
