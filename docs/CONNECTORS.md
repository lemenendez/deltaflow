# Connectors

In early Deltaflow versions, connectors should live under `internal/connectors`.
This keeps experimental connectors out of the public API while they are still
changing.

Once a connector is stable, it can graduate to a user-facing package under
`pkg/connectors` or be distributed from a separate repository.

## Runtime Worker Sizing

When connector-backed pipelines run through the CLI/runtime path, worker sizing
is controlled by `workers` config values:

- `workers.concurrency`: number of worker goroutines per pipeline cycle.
- `workers.batch_size`: max claims per goroutine in each cycle.
- `workers.pull_size`: optional dispatch pull limit.

Operational note:

- `workers.lock_for` must exceed worst-case time for one goroutine to drain its claimed batch. If leases expire before later claimed jobs start processing, ownership can be lost and the cycle may requeue leftovers.

If `workers.pull_size` is omitted, runtime derives dispatch pull size as
`workers.concurrency * workers.batch_size`. Set `workers.pull_size` explicitly
only when you need an override for operational tuning.
