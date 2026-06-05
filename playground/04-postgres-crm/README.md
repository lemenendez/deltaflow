# 04-postgres-crm

Concurrent workload playground for CRM read models and search/queue fanout.

The scenario simulates:

- backend servers changing users, roles, customers, and orders
- CRM workers updating customer contact data and order statuses
- 2 DeltaFlow workers claiming jobs from the same Postgres-backed sync
- deterministic fake users/customers/orders from `gofakeit` with a fixed seed
- API/worker mutations update durable Postgres CRM tables and enqueue every change through `DeltaStore.EnqueueInTx`
- Redis-style latest read views in a simulated applier
- OpenSearch/Redis queue boundary in a simulated applier for order/search fanout
- ghost deletion for a stale customer view
- transient retry and dead-letter behavior without crashing a process

The CRM source and DeltaFlow stores are durable Postgres tables. The projector connector is not implemented yet, so this playground stops at the projection/applier boundary and simulates what would be sent to Redis/OpenSearch.

## Run

From this folder:

- `make run`

Default simulation size:

- `USER_COUNT=8`
- `CUSTOMER_COUNT=18`
- `ORDER_COUNT=22`
- `MUTATION_COUNT=64`
- `WRITER_COUNT=4`
- `WORKER_COUNT=2`
- `MAX_ATTEMPTS=3`
- `SIM_SEED=4004`

Custom size example:

- `make run USER_COUNT=50 CUSTOMER_COUNT=500 ORDER_COUNT=1000 MUTATION_COUNT=5000 WRITER_COUNT=8 WORKER_COUNT=4`

`USER_COUNT`, `CUSTOMER_COUNT`, and `ORDER_COUNT` change the source universe. `MUTATION_COUNT` controls how many live writes/deltas/jobs are created. If you only increase `CUSTOMER_COUNT` or `ORDER_COUNT`, the run has a larger pool to choose from but the same number of mutations.

The workload always adds three special deltas on top of `MUTATION_COUNT`: one retry-once customer, one dead-letter order, and one ghost delete.

Useful commands:

- `make up` to start Postgres
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
