# 02-postgres

This playground demonstrates DeltaFlow using durable Postgres-backed stores:

- DeltaStore: enqueue contact deltas
- DispatchStore: convert pending deltas into outbox jobs
- JobStore: claim/process jobs and mark synced
- public `deltaflow.SyncWorker`: orchestrates dispatch + claim + projector/applier + state transitions

Scenario:

- enqueue 3 Contact deltas
- dispatch them to jobs
- run `deltaflow.SyncWorker.RunOnce` in a fixed drain loop for the three demo jobs
- upsert 2 contacts and delete 1 ghost contact (`con-999`)

## Requirements

- Docker + Docker Compose
- Make

## Run

From this folder:

- `make run`

Useful commands:

- `make up` to start Postgres
- `make migrate` to apply schema
- `make reset` to drop/recreate the schema
- `make down` to stop and remove containers/volumes
