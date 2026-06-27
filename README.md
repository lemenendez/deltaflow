# DeltaFlow

DeltaFlow is a Go library and CLI for keeping a derived system synchronized with the latest state of an application projection.

Use it when your app owns the source of truth, but you need to keep another system up to date asynchronously and safely: search indexes, cache documents, read models, or denormalized views.

Core flow:

```text
Application write -> Delta enqueue -> Dispatch -> SyncJob -> SyncWorker -> Projector.Project -> ProjectionApplier.Apply
```

If `Projector.Project` returns `ErrProjectionNotFound`, DeltaFlow treats the projection as a ghost and applies a delete operation.

## What DeltaFlow Is

DeltaFlow is intentionally narrow.

- It is a latest-state synchronization worker.
- It gives you durable Delta and SyncJob stores.
- It gives you explicit projector and applier contracts.
- It handles dispatch, leases, retries, dead jobs, and ghost deletes.
- It supports Postgres for multi-worker durable deployments and SQLite for single-node / single-worker deployments.

DeltaFlow is not a CDC platform, connector marketplace, fan-out engine, or general-purpose stream processor.

## Start Here

Pick the path that matches what you want to learn next.

### I want to understand the project quickly

- Read the short design overview in [docs/DESIGN.md](docs/DESIGN.md).
- See the broader design notes in [docs/DESIGNFULL.md](docs/DESIGNFULL.md).
- Check the current scope and future milestones in [docs/ROADMAP.md](docs/ROADMAP.md).

### I want to know what is available right now

- Latest release notes: [docs/RELEASE_NOTES_V0.10.0.md](docs/RELEASE_NOTES_V0.10.0.md).
- Current SQLite guidance: [docs/SQLITE.md](docs/SQLITE.md).
- Connector notes: [docs/CONNECTORS.md](docs/CONNECTORS.md).

### I want to run something end to end

- Start with the smallest example: [playground/01-in-memory/README.md](playground/01-in-memory/README.md).
- Try a durable Postgres flow: [playground/02-postgres/README.md](playground/02-postgres/README.md).
- Try the SQLite single-node flow: [playground/05-sqlite/README.md](playground/05-sqlite/README.md).

### I want to wire DeltaFlow into an application

- Review the CLI config examples in [cmd/deltaflow/deltaflow.yaml](cmd/deltaflow/deltaflow.yaml) and [cmd/deltaflow/deltaflow.sqlite.yaml](cmd/deltaflow/deltaflow.sqlite.yaml).
- Read the runtime registry code in [pkg/runtime/registry.go](pkg/runtime/registry.go) and [pkg/runtime/runner.go](pkg/runtime/runner.go).
- Look at the host/runtime example in [pkg/examples/contactsruntime](pkg/examples/contactsruntime).

### I want to know what comes next

- Near-term and future milestones live in [docs/ROADMAP.md](docs/ROADMAP.md).
- Deferred ideas and open directions are tracked in [docs/FUTURE.md](docs/FUTURE.md).

## Current Status

The current public path is the latest-state worker model with:

- public `deltaflow.SyncWorker`, `Projector`, and `ProjectionApplier` contracts
- durable Postgres and SQLite Delta/SyncJob stores
- transactional delta enqueue support via `EnqueueInTx`
- dispatch and outbox-safe job creation
- lease ownership checks, retries, dead-letter behavior, and ghost delete handling
- a concrete Elasticsearch applier in [pkg/connectors/elasticsearch](pkg/connectors/elasticsearch)

The latest documented milestone is v0.10.0, which adds SQLite durable-store support for local and embedded use cases. See [docs/RELEASE_NOTES_V0.10.0.md](docs/RELEASE_NOTES_V0.10.0.md).

## Release History

- Latest release notes: [docs/RELEASE_NOTES_V0.10.0.md](docs/RELEASE_NOTES_V0.10.0.md)
- Previous milestones: [docs/RELEASE_NOTES_V0.9.0.md](docs/RELEASE_NOTES_V0.9.0.md), [docs/RELEASE_NOTES_V0.8.0.md](docs/RELEASE_NOTES_V0.8.0.md), [docs/RELEASE_NOTES_V0.7.0.md](docs/RELEASE_NOTES_V0.7.0.md), [docs/RELEASE_NOTES_V0.5.0.md](docs/RELEASE_NOTES_V0.5.0.md), [docs/RELEASE_NOTES_V0.4.0.md](docs/RELEASE_NOTES_V0.4.0.md), [docs/RELEASE_NOTES_v0.3.0.md](docs/RELEASE_NOTES_v0.3.0.md)

## Choose a Deployment Shape

### Postgres

Choose Postgres when you want the main durable multi-worker path.

- durable SQL-backed Delta and SyncJob stores
- better fit for concurrent workers
- recommended when you need production-style worker coordination
- examples: [playground/02-postgres](playground/02-postgres), [playground/03-postgres-e-commerce](playground/03-postgres-e-commerce), [playground/04-postgres-crm](playground/04-postgres-crm)

### SQLite

Choose SQLite when you want a local, embedded, or single-node deployment.

- supported model is one worker process and `workers.concurrency=1`
- intended for local development, demos, embedded apps, and low-scale single-node deployments
- not intended for competing worker processes or distributed worker fleets
- docs: [docs/SQLITE.md](docs/SQLITE.md)
- example: [playground/05-sqlite](playground/05-sqlite)

## Quick CLI Start

Validate config, apply migrations, and run the worker:

```bash
go run ./cmd/deltaflow validate --config ./cmd/deltaflow/deltaflow.yaml
go run ./cmd/deltaflow migrate --config ./cmd/deltaflow/deltaflow.yaml
go run ./cmd/deltaflow run --config ./cmd/deltaflow/deltaflow.yaml
```

SQLite sample config:

```bash
go run ./cmd/deltaflow validate --config ./cmd/deltaflow/deltaflow.sqlite.yaml
go run ./cmd/deltaflow migrate --config ./cmd/deltaflow/deltaflow.sqlite.yaml
go run ./cmd/deltaflow run --config ./cmd/deltaflow/deltaflow.sqlite.yaml
```

Config loading expands `${VAR}` and `$VAR` before YAML parsing.

## Playground Guide

Standalone examples live under [playground](playground).

- [playground/01-in-memory](playground/01-in-memory): minimal in-memory latest-state flow using the public API
- [playground/02-postgres](playground/02-postgres): durable Postgres-backed contact sync
- [playground/03-postgres-e-commerce](playground/03-postgres-e-commerce): concurrent product-search workload with Elasticsearch, retries, and dead-letter simulation
- [playground/04-postgres-crm](playground/04-postgres-crm): concurrent CRM read-model workload with simulated views and Elasticsearch fanout
- [playground/05-sqlite](playground/05-sqlite): SQLite durable-store example for the supported single-node / single-worker model

If you are new to the project, start with 01, then 02 or 05 depending on whether you want the Postgres or SQLite path.

## Developer Notes

### Transactional enqueue

Both the Postgres and SQLite Delta stores expose two enqueue paths:

- `Enqueue(ctx, delta)` for standalone inserts
- `EnqueueInTx(ctx, tx, delta)` when the application write and delta enqueue must commit in the same SQL transaction

This is the core adoption pattern when the source write and DeltaFlow tables share one database.

### Runtime wiring

Runtime registrations are explicit and map-based.

- DeltaFlow does not infer projector wiring from YAML.
- Registration happens during startup before `run`.
- Duplicate names fail fast.

### Worker sizing

- `workers.concurrency` controls how many goroutines process jobs per pipeline cycle
- `workers.batch_size` controls how many jobs each goroutine can claim per cycle
- `workers.pull_size` is optional; when omitted, dispatch pull size defaults to `concurrency * batch_size`
- `workers.lease_ttl` should be sized to exceed worst-case batch drain time

For SQLite, `store.type=sqlite` supports only `workers.concurrency=1`.

## Docs Index

- Project overview: [docs/DESIGN.md](docs/DESIGN.md)
- Connector notes: [docs/CONNECTORS.md](docs/CONNECTORS.md)
- SQLite guidance: [docs/SQLITE.md](docs/SQLITE.md)
- Roadmap: [docs/ROADMAP.md](docs/ROADMAP.md)
- Future ideas: [docs/FUTURE.md](docs/FUTURE.md)
- Latest release notes: [docs/RELEASE_NOTES_V0.10.0.md](docs/RELEASE_NOTES_V0.10.0.md)

## Development Hooks

Set up the repo pre-commit hook:

```bash
./scripts/setup-git-hooks.sh
```

The hook runs:

- `gofmt -w` on staged Go files and re-stages formatting changes
- a partial-staging safety check for staged Go files
- `go vet ./...`
- `golangci-lint run ./...` when `golangci-lint` is available on `PATH`
