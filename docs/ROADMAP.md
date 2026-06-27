# Roadmap

## v0.1.0 - Skeleton + vocabulary + DESIGN.md
- [ ] Skeleton
- [ ] Vocabulary
- [ ] Initial [DESIGN.md](DESIGN.md)

## v0.2.0 - Core domain model + in-memory stores
- [ ] Intentionally non-durable milestone
- [ ] Projection
- [ ] ProjectionType
- [ ] ProjectionKey
- [ ] ProjectionIdentity
- [ ] Delta
- [ ] SyncJob
- [ ] Projector
- [ ] ProjectionApplier
- [ ] ProjectionOperation
- [ ] In-memory DeltaStore + JobStore + DispatchStore
- [ ] Delta -> SyncJob dispatch + worker FSM tests

## v0.3.0 - SQL schema + durable store implementation
- [ ] deltaflow_deltas + deltaflow_sync_jobs
- [ ] Durable DeltaStore + JobStore contracts
- [ ] Outbox/jobs split persists in SQL
- [ ] Canonical projection_key hashing
- [ ] Baseline leases: claim with lock_for + timeout reclaim
- [ ] Integration tests (container-backed Postgres)

## v0.4.0 - Postgres lease hardening + transactional app patterns
- [ ] Insert Deltas transactionally with application writes
- [ ] Lease renewal/heartbeat semantics
- [ ] Lease ownership checks on job state transitions
- [ ] Advanced lease observability and operations (slog-first + Prometheus-compatible telemetry)
- [ ] Playground for concurrent workload simulation (writers + workers)

## v0.5.0 - latest_state MVP
- [ ] Public SyncWorker.RunOnce API
- [ ] Documented Projector.Project() + ProjectionApplier.Apply() runtime contract
- [ ] Documented Delta ghost handling with ErrProjectionNotFound -> delete
- [ ] Documented app transaction example using EnqueueInTx
- [ ] Point users at existing fake/simple applier playgrounds
- [ ] README and DESIGN refresh for current latest-state MVP status
- [ ] Release notes

## v0.6.0 - CLI + minimal YAML
- [ ] Validate config
- [ ] Migrate Postgres schema
- [ ] Single minimal YAML shape
- [ ] No version field yet
- [ ] sync_id
- [ ] workers.lease_ttl / workers.max_attempts config
- [ ] Defer run worker until runtime wiring is designed
- [ ] No connector registry

## v0.7.0 - Elasticsearch applier + real search playground
- [ ] Concrete Elasticsearch ProjectionApplier package
- [ ] Upsert/delete support
- [ ] Explicit index/client config from Go code
- [ ] Retryable vs permanent error classification where practical
- [ ] Focused tests around operation mapping and error handling
- [ ] Update current search-oriented playgrounds to use Elasticsearch
- [ ] Document REST write -> transactional Delta enqueue -> async Elasticsearch sync

## v0.8.0 - CLI run + runtime wiring model
- [ ] Decide explicit registry/plugin story
- [ ] Run worker from YAML using registered projectors/appliers
- [ ] Keep runtime wiring explicit; do not infer app projectors by name
- [ ] Support Postgres store wiring from config
- [ ] Document how applications embed or wrap the runner

## v0.9.0 - Worker throughput + batching
- [ ] Configurable worker concurrency: N goroutines per sync/worker process
- [ ] Configurable batch size: each routine fetches up to M jobs per pull/drain cycle
- [ ] Preserve lease ownership semantics for every claimed job
- [ ] Decide API shape for batch claim (for example ClaimNextBatch(sync_id, worker_id, limit, lock_for))
- [ ] Keep per-job retry/dead/ghost handling observable inside the batch
- [ ] Benchmark against playground baseline: one-job-per-RunOnce drain time vs N routines x M batch size
- [ ] Expose config in CLI/YAML: workers.concurrency=N and workers.batch_size=M
- [ ] Validate with large playground runs using fixed seed/source universe/mutation count
- [ ] Plan doc: [PLAN_v0.9.0.md](PLAN_v0.9.0.md)
- [ ] Release notes draft: [RELEASE_NOTES_V0.9.0.md](RELEASE_NOTES_V0.9.0.md)

## v0.10.0 - SQLite single-node durable stores
- [x] SQLite DeltaStore implementation
- [x] SQLite JobStore implementation
- [x] SQLite schema: deltaflow_deltas + deltaflow_sync_jobs
- [x] Intentionally single-node / single-worker support only
- [x] No distributed worker coordination
- [x] No multiple competing worker processes
- [x] Enforce and clearly message unsupported settings: workers.concurrency > 1, multiple worker processes, distributed lease ownership
- [x] Support manual application enqueue API: same-transaction enqueue when source writes share the DB, and documented cross-database enqueue orchestration when they do not
- [x] Document intended use cases: local development, embedded apps, demos, local/single-tenant disk mode
- [x] Document non-goals: not for production worker fleets, not for multiple hosts, not for high-throughput distributed sync, not a replacement for Postgres durable stores
- [x] Add SQLite playground: local app write -> SQLite Delta enqueue -> single worker -> simple applier
- [x] Add SQLite store conformance tests for supported single-worker behavior
- [x] Plan doc: [PLAN_v0.10.0.md](PLAN_v0.10.0.md)
- [x] Release notes draft: [RELEASE_NOTES_V0.10.0.md](RELEASE_NOTES_V0.10.0.md)

## v0.11.0 - Redis applier
- [ ] Concrete Redis ProjectionApplier package
- [ ] Upsert/delete support for cache-style projections
- [ ] Explicit Redis client/config from Go code
- [ ] Key naming strategy: sync_id + projection_type + projection_key_hash
- [ ] JSON payload storage for latest projected state
- [ ] Optional TTL explicitly deferred unless needed by a playground
- [ ] Retryable vs permanent error classification where practical
- [ ] Focused tests around operation mapping and error handling
- [ ] Redis playground: source write -> transactional Delta enqueue -> async Redis cache sync
- [ ] Document Redis as a cache projection target
- [ ] Plan doc: [PLAN_v0.11.0.md](PLAN_v0.11.0.md)
- [ ] Release notes draft: [RELEASE_NOTES_V0.11.0.md](RELEASE_NOTES_V0.11.0.md)

## v0.12.0 - Backfills
- [ ] Define backfill API and CLI shape
- [ ] Enqueue deltas for an existing source universe
- [ ] Support deterministic paging over source records
- [ ] Support dry-run mode where practical
- [ ] Support resumable or restart-safe backfill behavior
- [ ] Avoid duplicate chaos by relying on latest_state collapse semantics
- [ ] Document backfill responsibilities: source enumeration belongs to the application/projector side, while DeltaFlow owns durable enqueue, dispatch, retry, and application
- [ ] Add backfill playground: seed source records -> run backfill -> observe destination catch up
- [ ] Include large-source playground scenario with fixed seed/count
- [ ] Plan doc: [PLAN_v0.12.0.md](PLAN_v0.12.0.md)
- [ ] Release notes draft: [RELEASE_NOTES_V0.12.0.md](RELEASE_NOTES_V0.12.0.md)

## v0.13.0 - Metrics + logs + operational safety
- [ ] Applier-level telemetry/logging for concrete target appliers: operation, result, retryable, status_code, and latency
- [ ] Prometheus-compatible examples and Grafana dashboard guidance
- [ ] Entity trace/debug command: given sync_id + projection_type + projection_key, explain what happened by fetching matching deltas, mapped jobs, attempts, final state, last error, ghost flag, lease ownership history, and related worker/lease logs
- [ ] Support trace for playground/operator usage first (for example: make trace TYPE=ProductSearchDocument KEY='{"product_id":"sku-004"}')
- [ ] Later promote trace to CLI: deltaflow trace --sync <sync_id> --type <projection_type> --key '<json>'
- [ ] Consider persisting/logging projection_key_hash and job_id in structured logs so traces can join SQL state and slog output reliably

## v0.14.0 - API polish + docs + examples
- [ ] Stabilize public interfaces before v1.0: Projection, ProjectionType, ProjectionKey, ProjectionIdentity, Delta, SyncJob, Projector, ProjectionApplier, ProjectionOperation, DeltaStore, JobStore, SyncWorker
- [ ] Review naming consistency across packages
- [ ] Review error names and exported sentinel errors
- [ ] Review config/YAML naming
- [ ] Review CLI command names and flags
- [ ] Add minimal operational safety documentation: retries, dead jobs, lease behavior, ghost handling, transactional enqueue, worker shutdown, SQLite single-worker limits, Postgres production recommendations
- [ ] Add example catalog: Postgres durable store + Elasticsearch, SQLite durable store + fake/simple applier, Postgres durable store + Redis, backfill example
- [ ] Refresh README
- [ ] Refresh [DESIGN.md](DESIGN.md)
- [ ] Add migration/upgrade notes from pre-v1 versions
- [ ] Mark unstable or experimental APIs clearly
- [ ] Prepare v1.0 release checklist
- [ ] Release notes draft: [RELEASE_NOTES_V0.14.0.md](RELEASE_NOTES_V0.14.0.md)

## v1.0.0 - Stable latest_state OSS release
- [ ] Stable latest_state synchronization model
- [ ] Stable public runtime contracts
- [ ] Stable durable store contracts
- [ ] Stable Postgres durable store
- [ ] Stable SQLite single-node durable store
- [ ] Stable Elasticsearch applier
- [ ] Stable Redis applier
- [ ] Stable CLI basics: validate, migrate, run
- [ ] Documented transactional enqueue patterns
- [ ] Documented worker behavior: leases, retries, dead jobs, ghost handling, backfills
- [ ] Documented adoption ladder: in-memory -> SQLite single-node -> Postgres production
- [ ] Documented examples and playgrounds

## v1.1.0 - Idempotency + duplicate detection foundation
- [ ] Define projection payload canonicalization rules
- [ ] Add projection_payload_hash / projection_fingerprint concept
- [ ] Define operation idempotency key: sync_id + projection_type + projection_key_hash + operation_type + payload_hash
- [ ] Extend ProjectionOperation metadata with projection_key_hash, payload_hash, idempotency_key
- [ ] Define applier result semantics: applied, skipped_no_change, skipped_duplicate, retryable_failure, permanent_failure
- [ ] Optional apply ledger / operation receipt store
- [ ] Allow appliers to detect no-op updates before writing
- [ ] Document differences among identity deduplication, content deduplication, and operation deduplication
- [ ] Add playground scenario: duplicate deltas -> same projection hash -> one effective target write
- [ ] Add failure scenario: worker retries after uncertain result -> idempotency prevents duplicate apply
