# 03-postgres-e-commerce

Concurrent workload playground for product search synchronization.

The scenario simulates:

- 2 backend web servers changing product text, images, discounts, promotions, and checkout copy
- 2 business workers changing warehouse inventory and free-shipping availability
- 2 DeltaFlow workers claiming jobs from the same Postgres-backed sync
- deterministic fake product data from `gofakeit` with a fixed seed
- application writers update durable Postgres product tables and enqueue every change through `DeltaStore.EnqueueInTx`
- Elasticsearch product index updates through the concrete Elasticsearch applier
- ghost deletion for a stale product document
- transient retry and dead-letter behavior as target-side failures on normal product updates

The product source and DeltaFlow stores are durable Postgres tables. The docker compose run starts Elasticsearch and uses the concrete applier in `pkg/connectors/elasticsearch`. A direct local `go run .` without `DELTAFLOW_ES_ENDPOINT` still falls back to the simulator for quick development.

The REST/API consistency model is represented by the writer stage: every application-side product mutation and its Delta are committed in the same Postgres transaction through `DeltaStore.EnqueueInTx`. Elasticsearch is updated asynchronously by DeltaFlow workers after the transaction commits.

## Public API vs Playground Glue

This playground uses the public Elasticsearch applier from `pkg/connectors/elasticsearch` for the real target writes. `elasticsearch_target.go` wraps that applier only for demo concerns:

- create/reset the demo Elasticsearch index
- seed a stale ghost document
- map projection keys to product document IDs
- inject retry/dead-letter failures
- count applied operations and snapshot indexed documents for the report
- fall back to the in-memory simulator when `DELTAFLOW_ES_ENDPOINT` is unset

The application-owned work is still explicit: `domain.go` defines the source model and projector, `engine.go` wires `SyncWorker`, and the writer path enqueues Deltas transactionally with source writes.

## Run

From this folder:

- `make run`

Default simulation size:

- `PRODUCT_COUNT=14`
- `MUTATION_COUNT=56`
- `WRITER_COUNT=4`
- `WORKER_COUNT=2`
- `MAX_ATTEMPTS=3`
- `SIM_SEED=3003`

Custom size example:

- `make run PRODUCT_COUNT=100 MUTATION_COUNT=1000 WRITER_COUNT=8 WORKER_COUNT=4`

`PRODUCT_COUNT` changes the source universe. `MUTATION_COUNT` controls how many live writes/deltas/jobs are created. If you only increase `PRODUCT_COUNT`, the run has a larger product pool to choose from but the same number of mutations.

The workload always adds three special deltas on top of `MUTATION_COUNT`: one retry-once product, one dead-letter product, and one ghost delete.

Useful commands:

- `make up` to start Postgres and Elasticsearch
- `make migrate` to apply schema
- `make reset` to drop/recreate the schema
- `make down` to stop and remove containers/volumes
- `make counts` to show delta/job counts by state
- `make jobs-by-state` to show job state counts with ghost totals
- `make deltas` to show the first 50 deltas for this sync
- `make pending-deltas` to inspect un-dispatched deltas
- `make jobs` to show the first 50 sync jobs
- `make dead-jobs` to inspect dead-lettered jobs
- `make worker-log` to tail `logs/deltaflow-worker.log`
- `make beautify` to pretty-print the latest JSON worker log lines
- `make beautify LOG_LINES=20` to control how many log lines are formatted

The worker and Postgres lease logs are written to `logs/deltaflow-worker.log` by default. Override the file path with `DELTAFLOW_WORKER_LOG`.
