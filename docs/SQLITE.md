# SQLite

SQLite support in DeltaFlow v0.10.0 is intentionally conservative, but it is production-capable for the supported single-node / single-worker shape.

Use it for:

- local development
- embedded applications
- demos
- single-node, single-tenant disk-backed usage
- production deployments that fit the supported single-node / single-worker model

Do not use it for:

- multiple competing DeltaFlow worker processes on the same database
- multiple hosts
- high-throughput distributed worker fleets
- production shapes that need horizontal worker scaling

## Runtime Model

Supported runtime shape:

- `store.type=sqlite`
- exactly one DeltaFlow worker process per database
- `workers.concurrency=1`

Guardrails:

- `doctor` warns that SQLite does not support multiple competing worker processes
- `run` rejects `workers.concurrency != 1`
- `run` acquires a singleton DB-backed worker lock and fails fast if another worker is already active
- the singleton lock is stored in `deltaflow_worker_locks` and is released on normal exit

## Recommended SQLite Settings

Recommended operational settings for SQLite mode:

- WAL mode
- busy timeout
- single writer expectation

Examples:

```sql
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
```

Notes:

- WAL mode improves local read/write behavior but does not change the single-worker runtime contract.
- Busy timeout reduces immediate lock failures under short local write contention.
- DeltaFlow still assumes one active worker process and one worker goroutine.

## Migrations

CLI migration works with SQLite as well:

```bash
go run ./cmd/deltaflow migrate --config ./path/to/deltaflow.yaml
```

Minimal config shape:

```yaml
store:
  type: sqlite
  dsn: file:deltaflow.sqlite

workers:
  concurrency: 1
  lease_ttl: 30s

pipelines:
  - name: contacts-to-elasticsearch
    sync_id: contacts-to-elasticsearch
    source:
      type: postgres-outbox
      projection_type: contact
    projector:
      name: contact-projector
    target:
      type: elasticsearch
      index: contacts
    applier:
      mode: upsert
```

Note:

- `source.type` is currently a required fixed value for the dispatch/outbox source shape.
- `source.type: postgres-outbox` does not mean Postgres is the durable store for this config.
- In this example, the durable store is still SQLite because `store.type=sqlite`.
- DeltaFlow still does not automatically capture source-table writes; applications enqueue deltas explicitly.

## Manual Enqueue Patterns

DeltaFlow does not scrape source-table writes automatically. Applications enqueue deltas explicitly.

### Same database transaction

When source tables and DeltaFlow tables live in the same SQLite database, write the source row and enqueue the delta in the same transaction.

```go
ctx := context.Background()
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()

if _, err := tx.ExecContext(ctx,
    `INSERT INTO contacts (id, full_name) VALUES (?, ?)`,
    "c-1", "Alice",
); err != nil {
    return err
}

_, err = deltaStore.EnqueueInTx(ctx, tx, deltaflow.Delta{
    SyncID:         "contacts-sync",
    Origin:         deltaflow.OriginOperationUpdated,
    ProjectionType: "contact",
    ProjectionKey: deltaflow.ProjectionKey{
        "contact_id": json.RawMessage(`"c-1"`),
    },
})
if err != nil {
    return err
}

return tx.Commit()
```

That gives one commit/rollback boundary for both the application write and the DeltaFlow outbox write.

### Cross-database explicit enqueue

When the source write and DeltaFlow SQLite database do not share the same transaction boundary, use an explicit orchestration pattern instead:

- app-owned outbox plus relay into DeltaFlow
- commit source write, then enqueue, with recovery on enqueue failure
- another application-specific compensating/retry workflow

DeltaFlow does not provide cross-database atomicity.

## Playground

The v0.11.2 cleanup moved the SQLite durable playground out of the core repository.
Use [playground/01-in-memory](../playground/01-in-memory/README.md) for a local in-repo sanity flow, and use the external SQLite playground/module once published.
