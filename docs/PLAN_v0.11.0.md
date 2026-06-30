# Plan v0.11.0

Goal: add a concrete Redis `ProjectionApplier` for cache-style latest-state projections, with application-owned key naming, optional expiration, and tested Valkey compatibility.

## Product Positioning

- Redis is a derived cache projection target, not a DeltaFlow durable store.
- The application remains responsible for constructing and configuring the Redis client.
- The application owns Redis key naming through a mandatory `KeyFunc`.
- Redis is the primary documented target; Valkey is an explicitly tested compatible target.
- Other Redis-compatible servers are best-effort and are not part of the v0.11.0 compatibility guarantee.

## Connector Package and Client

- Add the public package `pkg/connectors/redis`.
- Use `github.com/redis/go-redis/v9`.
- Accept a caller-owned client through a narrow `CommandClient` interface containing the go-redis `Set` and `Del` methods. Standard standalone, Sentinel, Cluster, and Ring clients satisfy it.
- Do not create a client from an endpoint inside the applier.
- Do not close the caller-owned client.
- Do not add connector registration or YAML configuration in this milestone; construction remains explicit Go code.

Proposed configuration:

```go
type KeyFunc func(deltaflow.ProjectionIdentity) (string, error)

type CommandClient interface {
    Set(context.Context, string, interface{}, time.Duration) *redis.StatusCmd
    Del(context.Context, ...string) *redis.IntCmd
}

type ApplierConfig struct {
    Client  CommandClient
    KeyFunc KeyFunc
    TTL     time.Duration
}
```

Validation:

- `Client` is required.
- `KeyFunc` is required; there is no default key generator.
- `TTL == 0` stores persistent keys.
- `TTL > 0` applies expiration on every upsert.
- `TTL < 0` is rejected by `NewApplier`.
- `KeyFunc` must return a non-empty key.
- Invalid constructor configuration returns an error; it does not panic.

## Sync and Key Ownership

`SyncID` identifies a synchronization pipeline, for example `contacts-to-elasticsearch`. It groups and selects deltas/jobs and is distinct from `WorkerID`, which identifies a worker lease owner.

The Redis applier does not require `SyncID` in its configuration or impose it on destination keys. Redis key layout is part of the application/client realm. Applications may include sync identity, projection type, tenant, version, or any other namespace in their `KeyFunc` when appropriate.

Example application-owned key:

```text
contacts-111-xxx-yy
```

This decision avoids connector-created namespaces and permits the Redis cache schema to match application read paths directly.

## Payload Contract

- Store `Projection.Payload` unchanged as a Redis string value.
- Redis string values are binary-safe, so payloads are opaque bytes rather than JSON-only documents.
- Do not validate, canonicalize, or rewrite payload bytes.
- Do not interpret or restrict `Projection.MediaType`.
- RedisJSON/module commands are out of scope because they are not required for blob storage and would narrow server compatibility.
- Payload size and serialization choices remain application responsibilities.

## Operation Mapping

### Upsert

1. Require a non-nil `Projection`.
2. Resolve the destination key using `KeyFunc(op.Identity)`.
3. Execute `SET key payload TTL` through the configured client.
4. Repeated upserts replace the same key and are idempotent for latest-state behavior.

### Delete

1. Resolve the destination key using `KeyFunc(op.Identity)`.
2. Execute `DEL key`.
3. Treat deletion of a missing key as success, matching the idempotent applier contract.

Unsupported operation types return an error.

## State Idempotency and TTL Policy

The Redis connector is state-idempotent, not exactly-once:

- `KeyFunc` must deterministically map the same `ProjectionIdentity` to the same Redis key.
- Upsert uses replacement (`SET`), never additive commands such as `INCR`, list append, or stream publication.
- Delete uses `DEL` and remains successful when the key is already absent.
- A repeated command may still produce repeated Redis writes, replication/AOF activity, metrics, or keyspace notifications. Consumers must not treat those secondary effects as exactly-once events.

TTL follows a **replace-and-refresh** policy:

- TTL is fixed per applier configuration; it is not supplied by each projection.
- `TTL == 0` sends `SET` without expiration. This makes the key persistent and replaces any expiration already attached to that key.
- `TTL > 0` sends `SET` with that expiration. Each successful upsert establishes a new expiration deadline relative to when Redis processes that `SET`.
- A retry after an uncertain or failed response can therefore refresh the expiration again. This is intentional cache-retention behavior.
- The last `SET` processed by Redis for a key determines both its value and expiration. There is no minimum/maximum TTL selection, sorting, merging, or preservation of an earlier TTL.
- `DEL` removes both the value and its expiration metadata.
- Reads do not extend expiration; sliding TTL is out of scope.

TTL represents retention after synchronization, not source-record recency or an absolute business expiry. Strictly repeatable expiration would require an absolute expiry timestamp or version metadata in the operation contract and is outside v0.11.0.

DeltaFlow does not provide per-key fencing or compare-and-set in this milestone. If multiple projection identities, appliers, or external writers intentionally share one Redis key, Redis command order determines the final state and the application owns that coordination risk.

## Retry and Error Policy

- Do not implement a retry loop inside the Redis applier.
- Return key-generation and Redis client errors to the caller.
- Wrap errors with operation context using `%w` so `errors.Is`/`errors.As`, context errors, and go-redis errors remain inspectable.
- Do not add Redis-specific sentinel errors or a retryable/permanent error taxonomy in this milestone.
- Constructor validation uses ordinary descriptive errors, consistent with the existing Elasticsearch applier.
- The worker retains the existing uniform policy: a projection or apply failure marks the job retrying until `MaxAttempts`, then marks it dead.
- Each worker attempt runs the projector again before applying, preserving latest-state semantics instead of retrying a stale captured payload.
- The same worker policy applies to both upserts and deletes.

The broader applier result/permanent-failure contract remains deferred to the v1.1.0 roadmap.

## Redis and Valkey Compatibility

The connector uses only standard, long-established commands:

- `SET` with optional expiration.
- `DEL` for idempotent deletion.

No Lua scripts, modules, RedisJSON commands, or server-specific extensions are required.

Compatibility commitment for v0.11.0:

- Redis is the primary supported and documented server.
- Valkey is a tested compatible server.
- CI/integration coverage must run the same connector behavior against both Redis and Valkey.
- KeyDB, Dragonfly, and managed Redis-compatible services may work through protocol compatibility but remain best-effort unless added to the test matrix later.

## Implementation Steps

1. Add the go-redis dependency.
2. Add `pkg/connectors/redis/applier.go` with configuration validation and `ProjectionApplier` implementation.
3. Add focused unit tests for configuration, key resolution, command mapping, TTL, idempotency, opaque bytes, and wrapped errors.
4. Add container-backed connector integration tests shared by Redis and Valkey.
5. Add `playground/06-postgres-redis` using Redis by default.
6. Extend connector documentation and the playground catalog.
7. Update CI path detection and integration execution for the Redis/Valkey matrix.
8. Complete release notes and mark roadmap items only after verification.

## Testing Plan

### Unit tests

- Reject nil client.
- Reject nil `KeyFunc`.
- Reject negative TTL.
- Reject an empty key returned by `KeyFunc`.
- Reject upsert with a nil projection.
- Reject unsupported operation types.
- Map upsert to `SET` with exact payload bytes and configured TTL.
- Map zero TTL to persistent storage.
- Verify zero-TTL upsert clears an expiration already present on the key.
- Verify positive-TTL upsert refreshes expiration rather than preserving the previous deadline.
- Map delete to `DEL`.
- Verify repeated upsert and delete calls remain safe.
- Preserve arbitrary non-JSON/binary payload bytes.
- Preserve wrapped key-function, context, and client errors for `errors.Is`/`errors.As`.

### Integration tests

Run the same contract suite against Redis and Valkey:

- Persistent upsert/readback.
- Expiring upsert/readback and eventual expiration.
- Replacement of an existing value.
- Binary payload round trip.
- Existing-key delete.
- Missing-key delete.
- Context cancellation or unavailable-server error propagation where deterministic.

### Regression tests

- Run `go test ./...` from the root module.
- Verify existing Postgres, SQLite, Elasticsearch, worker retry, and playground behavior remains unchanged.

## Playground

Add `playground/06-postgres-redis` with:

- Postgres application/source tables.
- Postgres DeltaFlow durable stores.
- A source write and Delta enqueue committed in the same transaction.
- Dispatcher plus asynchronous worker execution.
- A concrete Redis applier configured from Go.
- An application-owned `KeyFunc` matching the cache read path.
- Redis as the default Docker Compose service.
- Observable cache upsert, retry, and ghost deletion behavior.
- A documented way to run the same scenario against Valkey for compatibility verification.

## Documentation

- Explain that Redis is a cache projection, not the source of truth or durable DeltaFlow store.
- Document client ownership and lifecycle.
- Document mandatory `KeyFunc` ownership and collision responsibility.
- Document opaque/binary-safe payload semantics.
- Document TTL behavior and refresh-on-upsert semantics.
- Document state idempotency versus exactly-once execution and duplicate notifications.
- Document idempotent upsert/delete expectations.
- Document the existing worker retry/max-attempts behavior.
- State the Redis-primary and Valkey-tested compatibility policy precisely.

## Acceptance Criteria

- `pkg/connectors/redis.Applier` implements `deltaflow.ProjectionApplier`.
- Application code must provide both the Redis client and `KeyFunc`.
- Upsert writes exact payload bytes with configured TTL semantics.
- Delete is idempotent when the key does not exist.
- The applier adds no retry loop and does not change worker retry policy.
- No new core sentinel errors are introduced.
- Shared integration behavior passes against Redis and Valkey.
- The Redis-default playground demonstrates transactional enqueue through asynchronous cache synchronization.
- Root tests and existing connector tests remain green.

## Out of Scope

- Connector-owned/default Redis key formats.
- Adding `SyncID` to `ProjectionOperation` or Redis applier configuration.
- Redis as a DeltaStore or JobStore.
- RedisJSON or other module-specific storage.
- Payload validation or serialization.
- Per-projection or dynamic TTL callbacks.
- Sliding expiration caused by reads.
- Connector-level retries or retryable/permanent error classification.
- YAML/runtime registry construction for Redis.
- Pipelined or multi-operation apply APIs.
- Transactions, Lua scripts, compare-and-set, or checksum-based no-op detection.
- Formal compatibility guarantees for Redis-compatible servers other than Valkey.
