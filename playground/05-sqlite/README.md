# 05-sqlite

This playground demonstrates DeltaFlow with the SQLite durable-store path in its intended v0.10.0 shape:

- local single-process execution
- one worker goroutine
- source-table write and delta enqueue in the same SQLite transaction via `EnqueueInTx`
- public `deltaflow.SyncWorker` using SQLite DeltaStore, DispatchStore, and JobStore
- simple in-memory target applier

## What it shows

- application source row write to `contacts`
- transactional `EnqueueInTx` into DeltaFlow tables in the same database
- SQLite migrations and recommended runtime pragmas
- the singleton worker lock that keeps only one DeltaFlow process active for the database
- one worker cycle that projects the latest row and applies an upsert

## Run

```bash
cd playground/05-sqlite
go run .
```

Optional environment variable:

- `DELTAFLOW_SQLITE_DSN`: override the default SQLite DSN/path

Default behavior uses a local file database and applies:

- `PRAGMA journal_mode=WAL`
- `PRAGMA busy_timeout=5000`

## Notes

- This playground is intentionally single-worker only.
- Do not run multiple copies against the same database.
- For multi-worker or multi-host deployments, use Postgres instead.
