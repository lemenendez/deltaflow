# 06-postgres-redis

This playground demonstrates a durable Postgres source/outbox synchronized asynchronously into a Redis cache through the concrete DeltaFlow Redis applier.

The scenario performs:

- a Postgres contact write and `DeltaStore.EnqueueInTx` in the same transaction
- durable dispatch and worker processing through the Postgres DeltaFlow stores
- application-owned key mapping from `ContactCache` identity to `contacts:<contact_id>`
- an opaque JSON payload stored through binary-safe Redis `SET`
- configured replace-and-refresh TTL behavior
- one simulated temporary Redis timeout followed by worker re-projection and retry
- ghost detection followed by idempotent Redis `DEL`

Redis is the default service. The same scenario can run against Valkey as the explicitly tested compatibility target.

## Requirements

- Docker with Docker Compose
- Make

## Run with Redis

```bash
make run
```

## Run with Valkey

```bash
make down
make run-valkey
```

The Valkey command changes only the server image. The connector and application code remain unchanged.

## Inspect

```bash
make redis-value
make redis-ttl
make jobs
```

Override the cache TTL when running the scenario:

```bash
make run DELTAFLOW_REDIS_TTL=30s
```

`DELTAFLOW_REDIS_TTL=0s` creates persistent keys. A positive TTL is replaced and refreshed on each successful upsert; retries may therefore extend cache retention.

## Cleanup

```bash
make down
```
