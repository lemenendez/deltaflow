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

## SQLite Notes

SQLite support is intentionally single-worker in v0.10.0.

- Use `store.type=sqlite` only with `workers.concurrency=1`.
- Do not run multiple DeltaFlow worker processes against the same SQLite database.
- Prefer WAL mode and a configured busy timeout for local deployments.
- Use `EnqueueInTx` when source writes and DeltaFlow tables share the same SQLite transaction.

See `docs/SQLITE.md` for full operational and enqueue-pattern guidance.

## Redis Projection Applier

`pkg/connectors/redis` applies latest-state projections to Redis-compatible string keys. Redis is a derived cache target; it is not a DeltaStore or JobStore.

The application constructs and owns the go-redis client and must provide a deterministic `KeyFunc`:

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

applier, err := redisconnector.NewApplier(redisconnector.ApplierConfig{
    Client: client,
    KeyFunc: func(identity deltaflow.ProjectionIdentity) (string, error) {
        id, err := contactID(identity.Key)
        if err != nil {
            return "", err
        }
        return "contacts:" + id, nil
    },
    TTL: 15 * time.Minute,
})
```

Client ownership:

- The applier accepts the narrow `CommandClient` interface implemented by standard go-redis standalone, Sentinel, Cluster, and Ring clients.
- The applier does not create, ping, or close the client.
- Connection pools, authentication, TLS, timeouts, topology, and go-redis transport retries remain application configuration.

Key and payload contract:

- `KeyFunc` is mandatory; there is no connector-owned key format.
- The same Projection Identity must always resolve to the same non-empty key.
- Applications own namespacing and collision avoidance.
- `Projection.Payload` is stored unchanged as a binary-safe Redis string value.
- JSON is supported as ordinary bytes but is neither required nor validated.
- RedisJSON and other module-specific commands are not used.

Operation contract:

- Upsert executes `SET`, replacing the complete value.
- Delete executes `DEL`; an already-missing key is success.
- Additive commands such as `INCR`, append, list mutation, and stream publication are intentionally unavailable because the current `ProjectionApplier` contract requires state-idempotent behavior.
- The connector adds no retry loop. Worker backoff and `MaxAttempts` remain the only DeltaFlow retry policy.

TTL uses replace-and-refresh semantics:

- `TTL == 0` writes a persistent key and clears an existing expiration.
- `TTL > 0` sets a new expiration relative to every successful upsert.
- Negative TTL is rejected during construction.
- Retries may refresh expiration again.
- Reads do not extend TTL.

Redis is the primary documented target. Valkey runs the same connector integration contract. Other Redis-compatible servers remain best-effort.

The v0.11.2 cleanup moved the larger playground applications out of the core repository. Use the external Postgres-to-Redis playground/module once published.
