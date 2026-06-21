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

## Benchmark Mode (v0.9.0 throughput)

Run deterministic throughput benchmark (real Postgres stores + worker path):

- `make up`
- `make migrate`
- `DELTAFLOW_PG_DSN='postgres://deltaflow:deltaflow@localhost:5432/deltaflow?sslmode=disable' go run . -mode bench`

Explicit matrix example:

```bash
DELTAFLOW_PG_DSN='postgres://deltaflow:deltaflow@localhost:5432/deltaflow?sslmode=disable' \
	go run . \
	-mode bench \
	-seed 42 \
	-universe 1000 \
	-mutations 50000 \
	-ghost-every 10 \
	-concurrency 1,2,4,8 \
	-batch 1,8,16,32
```

Recommended tuning workflow:

- Keep `seed`, `universe`, and `mutations` fixed.
- Compare baseline (`concurrency=1`, `batch=1`) against `N x M` pairs.
- Watch speedup and correctness counters (`synced`, `dead`, `retrying`, `ghosts`).
- Record runs in `BENCHMARK_RESULTS.md`.
