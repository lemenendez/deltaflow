# DeltaFlow

DeltaFlow is a Go library and CLI for keeping a derived system synchronized with the latest state of an application projection.

Use it when your app owns the source of truth, but you need to keep another system up to date asynchronously and safely: search indexes, cache documents, read models, or denormalized views.

DeltaFlow is an embeddable projection synchronization engine focused on latest-state delivery today, with roadmap support for additional projection timing modes.

It helps applications record durable projection changes, dispatch sync jobs, apply projections to external targets, retry failures, inspect outcomes, and rebuild destinations through backfills.

Latest-state delivery means that, for a given Projection Identity, DeltaFlow is designed to converge the destination toward the newest desired projection state. With projection timing modes, this behavior can vary by mode: for example, Early Projection may intentionally apply multiple versions for the same Projection Identity in order.

Projection computation is application-owned. A projection may be captured early by the application, computed later by a Go Projector, or computed later through a configured SQL function/procedure.

DeltaFlow is not CDC, not event sourcing, not workflow automation, and not a universal integration platform. It focuses on application-owned projection semantics: the application decides what a business object means, when a projection is produced, and how that projection should appear in external targets.

Core flow (current latest-state path):

```text
Application write -> Delta enqueue -> Dispatch -> SyncJob -> SyncWorker -> Projector.Project -> ProjectionApplier.Apply
```

In the current latest-state path, if `Projector.Project` returns `ErrProjectionNotFound`, DeltaFlow treats the projection as a ghost and applies a delete operation.

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

- Read the design overview in [docs/DESIGN.md](docs/DESIGN.md).
- Check the current scope and future milestones in [docs/ROADMAP.md](docs/ROADMAP.md).

### I want to know what is available right now

- Latest release notes: [docs/RELEASE_NOTES_V0.11.0.md](docs/RELEASE_NOTES_V0.11.0.md).
- Current SQLite guidance: [docs/SQLITE.md](docs/SQLITE.md).
- Connector notes: [docs/CONNECTORS.md](docs/CONNECTORS.md).

### I want to run something end to end

- Start with the smallest example: [playground/01-in-memory/README.md](playground/01-in-memory/README.md).
- Additional playgrounds were moved out of this repository in v0.11.2. See [playground/README.md](playground/README.md) for the current in-repo scope and transition notes.

### I want to wire DeltaFlow into an application

- Review the CLI config examples in [cmd/deltaflow/deltaflow.yaml](cmd/deltaflow/deltaflow.yaml) and [cmd/deltaflow/deltaflow.sqlite.yaml](cmd/deltaflow/deltaflow.sqlite.yaml).
- Read the runtime registry code in [pkg/runtime/registry.go](pkg/runtime/registry.go) and [pkg/runtime/runner.go](pkg/runtime/runner.go).
- See the runtime package examples in [pkg/runtime](pkg/runtime).

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
- a concrete Redis applier with tested Valkey compatibility in [pkg/connectors/redis](pkg/connectors/redis)

The latest documented milestone is v0.11.0, which adds the Redis applier for cache-style projection targets. See [docs/RELEASE_NOTES_V0.11.0.md](docs/RELEASE_NOTES_V0.11.0.md).

## Release History

- Latest release notes: [docs/RELEASE_NOTES_V0.11.0.md](docs/RELEASE_NOTES_V0.11.0.md)
- Previous milestones: [docs/RELEASE_NOTES_V0.10.0.md](docs/RELEASE_NOTES_V0.10.0.md), [docs/RELEASE_NOTES_V0.9.0.md](docs/RELEASE_NOTES_V0.9.0.md), [docs/RELEASE_NOTES_V0.8.0.md](docs/RELEASE_NOTES_V0.8.0.md), [docs/RELEASE_NOTES_V0.7.0.md](docs/RELEASE_NOTES_V0.7.0.md), [docs/RELEASE_NOTES_V0.5.0.md](docs/RELEASE_NOTES_V0.5.0.md), [docs/RELEASE_NOTES_V0.4.0.md](docs/RELEASE_NOTES_V0.4.0.md), [docs/RELEASE_NOTES_v0.3.0.md](docs/RELEASE_NOTES_v0.3.0.md)

## Choose a Deployment Shape

### Postgres

Choose Postgres when you want the main durable multi-worker path.

- durable SQL-backed Delta and SyncJob stores
- better fit for concurrent workers
- recommended when you need production-style worker coordination
- v0.11.2 moved full Postgres playground applications out of this repository.

### SQLite

Choose SQLite when you want a local, embedded, or single-node deployment.

- supported model is one worker process and `workers.concurrency=1`
- intended for local development, demos, embedded apps, and low-scale single-node deployments
- not intended for competing worker processes or distributed worker fleets
- docs: [docs/SQLITE.md](docs/SQLITE.md)
- v0.11.2 moved the SQLite durable playground out of this repository.

## Quick CLI Start

Run the stock operator CLI:

```bash
export DELTAFLOW_STORE_DSN='postgres://deltaflow:deltaflow@localhost:5432/deltaflow?sslmode=disable'
go run ./cmd/deltaflow doctor --config ./cmd/deltaflow/deltaflow.yaml
go run ./cmd/deltaflow migrate --config ./cmd/deltaflow/deltaflow.yaml
```

The stock `cmd/deltaflow` binary is an operator tool: it supports `doctor` and `migrate`, but intentionally does not expose `run`. This is not the worker run command.

Worker execution must be implemented by your application.

Playground references:

- [playground/01-in-memory/README.md](playground/01-in-memory/README.md): smallest in-repo projector/applier flow
- [playground/README.md](playground/README.md): current playground catalog and transition notes
- [docs/RELEASE_NOTES_V0.11.2.md](docs/RELEASE_NOTES_V0.11.2.md): archived playground backup location and cleanup scope

The legacy `validate` subcommand name remains available as an alias for `doctor`.

SQLite sample config:

```bash
go run ./cmd/deltaflow doctor --config ./cmd/deltaflow/deltaflow.sqlite.yaml
go run ./cmd/deltaflow migrate --config ./cmd/deltaflow/deltaflow.sqlite.yaml
```

Config loading expands `${VAR}` and `$VAR` before YAML parsing.

## Playground Guide

Standalone examples live under [playground](playground).

- [playground/01-in-memory](playground/01-in-memory): minimal in-memory latest-state flow using the public API
- Full playground applications were removed from the core repo in v0.11.2 and prepared for externalization. See [playground/README.md](playground/README.md) for transition notes.

If you are new to the project, start with 01.

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
- Latest release notes: [docs/RELEASE_NOTES_V0.11.0.md](docs/RELEASE_NOTES_V0.11.0.md)

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
