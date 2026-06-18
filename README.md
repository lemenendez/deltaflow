# DeltaFlow

DeltaFlow is a reconciliation worker.

The current focus is the latest-state worker path: public projector/applier
interfaces, a public `deltaflow.SyncWorker`, durable Postgres stores, ghost
delete handling, transactional application outbox writes, and concrete
Elasticsearch `ProjectionApplier`.

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

The concrete Postgres delta store provides two clear write paths:

- `Enqueue(ctx, delta)` for standalone inserts (tests, backfills, CLI/admin tools).
- `EnqueueInTx(ctx, tx, delta)` when app writes and outbox inserts must share the same SQL transaction.

The concrete Elasticsearch applier lives in `pkg/connectors/elasticsearch` and
maps `ProjectionOpUpsert`/`ProjectionOpDelete` to idempotent Elasticsearch
document writes.

The Elasticsearch playgrounds use that public applier directly. Their
`elasticsearch_target.go` files are demo glue around the applier: index
reset/seed, failure simulation, counters, snapshots, and local fallback
behavior. Application projectors and target-specific document shape remain
custom code owned by the application.

## CLI

The CLI validates one minimal YAML shape, applies embedded Postgres schema
migrations, and starts the v0.8 runtime wiring path with `run`:

```bash
go run ./cmd/deltaflow validate --config ./cmd/deltaflow/deltaflow.yaml
go run ./cmd/deltaflow migrate --config ./cmd/deltaflow/deltaflow.yaml
go run ./cmd/deltaflow run --config ./cmd/deltaflow/deltaflow.yaml
```

Config loading expands environment variables with Go's `os.ExpandEnv`, so both
`${VAR}` and `$VAR` placeholders are supported. Expansion runs before YAML
parsing, so quoted YAML strings are expanded too; avoid literal `$NAME`
sequences in config values such as DSNs.

`run` currently executes one worker cycle per configured pipeline. Runtime
registrations are explicit and map-based. DeltaFlow does not infer application
projector wiring from YAML. Registration is done during startup before `run`,
and duplicate names fail fast.

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
