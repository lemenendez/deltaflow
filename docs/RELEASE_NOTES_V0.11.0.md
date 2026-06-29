# v0.11.0

DeltaFlow v0.11.0 adds a concrete Redis projection applier for asynchronously maintained cache views, with application-owned key naming, configurable expiration, opaque payload storage, and tested Valkey compatibility.

> Status: draft. The items below describe the intended release and must be confirmed against the final implementation before publication.

## Highlights

- Adds `pkg/connectors/redis` as a public `ProjectionApplier` package.
- Supports latest-state upsert through Redis `SET`.
- Supports idempotent deletion through Redis `DEL`.
- Accepts an explicitly constructed `go-redis/v9` client from application code.
- Requires an application-provided `KeyFunc`; DeltaFlow does not impose a Redis key schema.
- Stores `Projection.Payload` as unchanged, binary-safe bytes rather than requiring JSON.
- Supports persistent keys and configurable TTL expiration.
- Uses Redis as the primary documented server and tests Valkey compatibility.

## Proposed Go Configuration

```go
client := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

applier, err := redisconnector.NewApplier(redisconnector.ApplierConfig{
    Client: client,
    KeyFunc: func(identity deltaflow.ProjectionIdentity) (string, error) {
        return applicationCacheKey(identity)
    },
    TTL: 15 * time.Minute,
})
```

The application owns the client lifecycle and closes it when the application shuts down.

## Key Ownership

Redis keys are part of the consuming application's cache contract. This release intentionally provides no default key generator and does not automatically include `SyncID`, projection type, or projection-key hashes.

Applications must provide a deterministic, non-empty key from `ProjectionIdentity` and are responsible for namespace and collision behavior.

## Payload and TTL Semantics

- Projection payloads are written unchanged as binary-safe Redis string values.
- JSON is supported as ordinary bytes but is not required or validated.
- `Projection.MediaType` does not change Redis storage behavior.
- `TTL == 0` creates persistent keys.
- `TTL > 0` applies expiration and refreshes that expiration on every successful upsert.
- Negative TTL configuration is rejected.
- Delete removes the key regardless of its TTL and succeeds when the key is already missing.

The TTL policy is **replace-and-refresh**:

- The last `SET` processed by Redis determines both the stored value and its expiration.
- A zero-TTL upsert removes any previous expiration and leaves the key persistent.
- A positive-TTL upsert replaces the previous deadline with a new deadline relative to that write.
- Retries can refresh the deadline again; TTL represents cache retention after synchronization, not an absolute business expiry.
- TTL values are not sorted, merged, minimized, maximized, or preserved.
- Reads do not extend TTL.

## State Idempotency

The connector is state-idempotent but does not promise exactly-once execution:

- Repeating an upsert converges on the same key/value state, apart from the documented TTL refresh.
- Repeating a delete leaves the key absent.
- The connector does not expose non-idempotent operations such as increment, append, or stream publication.
- Duplicate Redis writes, replication/AOF activity, metrics, or keyspace notifications may still occur and must not be consumed as exactly-once events.
- `KeyFunc` must be deterministic. Applications that deliberately map multiple projection identities or writers to one key accept last-command-wins coordination.

## Retry Behavior

The Redis connector adds no internal retry policy and does not introduce retryable/permanent error classification.

Redis apply failures continue through the existing worker policy:

- The job is retried according to worker backoff and `MaxAttempts`.
- Each attempt projects the latest source state again before applying it.
- The job is marked dead only when the configured attempt limit is reached.
- Upsert and delete failures follow the same policy.

## Redis and Valkey Compatibility

The connector uses standard `SET` and `DEL` commands without modules, Lua scripts, or vendor-specific extensions.

- Redis is the default server in documentation and the playground.
- Valkey runs the same integration contract as an explicitly supported compatible target.
- Other Redis-compatible servers remain best-effort for this release.

## Playground

`playground/06-postgres-redis` demonstrates:

- A Postgres source write and Delta enqueue in one transaction.
- Durable Postgres dispatch and worker processing.
- Asynchronous application of the latest projection to Redis.
- Application-owned cache keys.
- Cache upsert, worker retry, and ghost deletion behavior.
- Redis by default, with documented Valkey compatibility execution.

## Validation and Errors

- A client and `KeyFunc` are required.
- Negative TTL and empty generated keys are rejected.
- Upsert requires a projection.
- Unsupported operation types return an error.
- Client and key-generation errors preserve their original cause through Go error wrapping.
- No new core DeltaFlow sentinel errors are introduced.

## Compatibility and Upgrade Notes

- This release does not change `Projection`, `ProjectionOperation`, `ProjectionApplier`, `SyncID`, or worker retry contracts.
- Existing Postgres, SQLite, and Elasticsearch connectors are unaffected.
- Redis client construction remains explicit Go code; no Redis YAML/runtime registry configuration is introduced.

## Deferred

- Default or connector-owned Redis key formats.
- RedisJSON and module-specific operations.
- Dynamic/per-projection TTL.
- Connector-level retries or permanent-failure signaling.
- Runtime/YAML Redis client construction.
- Batch/pipelined apply APIs.
- Payload checksums, duplicate suppression, and no-op detection.
- Formal support for additional Redis-compatible servers beyond Valkey.

## Verification

Before release, verify:

- Root unit tests pass with `go test ./...`.
- Shared connector integration tests pass against Redis and Valkey.
- Exact binary payload round trips succeed.
- TTL and persistent-key behavior match the documented contract.
- The Redis-default playground completes source write through asynchronous cache synchronization.
