# Roadmap

## v0.1.0 - Skeleton + vocabulary + DESIGN.md
- [x] Skeleton
- [x] Vocabulary
- [x] Initial [DESIGN.md](DESIGN.md)

## v0.2.0 - Core domain model + in-memory stores
- [x] Intentionally non-durable milestone
- [x] Projection
- [x] ProjectionType
- [x] ProjectionKey
- [x] ProjectionIdentity
- [x] Delta
- [x] SyncJob
- [x] Projector
- [x] ProjectionApplier
- [x] ProjectionOperation
- [x] In-memory DeltaStore + JobStore + DispatchStore
- [x] Delta -> SyncJob dispatch + worker FSM tests

## v0.3.0 - SQL schema + durable store implementation
- [x] deltaflow_deltas + deltaflow_sync_jobs
- [x] Durable DeltaStore + JobStore contracts
- [x] Outbox/jobs split persists in SQL
- [x] Canonical projection_key hashing
- [x] Baseline leases: claim with lock_for + timeout reclaim
- [x] Integration tests (container-backed Postgres)

## v0.4.0 - Postgres lease hardening + transactional app patterns
- [x] Insert Deltas transactionally with application writes
- [x] Lease renewal/heartbeat semantics
- [x] Lease ownership checks on job state transitions
- [x] Advanced lease observability and operations (slog-first + Prometheus-compatible telemetry)
- [x] Playground for concurrent workload simulation (writers + workers)

## v0.5.0 - latest_state MVP
- [x] Public SyncWorker.RunOnce API
- [x] Documented Projector.Project() + ProjectionApplier.Apply() runtime contract
- [x] Documented Delta ghost handling with ErrProjectionNotFound -> delete
- [x] Documented app transaction example using EnqueueInTx
- [x] Point users at existing fake/simple applier playgrounds
- [x] README and DESIGN refresh for current latest-state MVP status
- [x] Release notes

## v0.6.0 - CLI + minimal YAML
- [x] Validate config
- [x] Migrate Postgres schema
- [x] Single minimal YAML shape
- [x] No version field yet
- [x] sync_id
- [x] workers.lease_ttl / workers.max_attempts config
- [x] Defer run worker until runtime wiring is designed
- [x] No connector registry

## v0.7.0 - Elasticsearch applier + real search playground
- [x] Concrete Elasticsearch ProjectionApplier package
- [x] Upsert/delete support
- [x] Explicit index/client config from Go code
- [x] Retryable vs permanent error classification where practical
- [x] Focused tests around operation mapping and error handling
- [x] Update current search-oriented playgrounds to use Elasticsearch
- [x] Document REST write -> transactional Delta enqueue -> async Elasticsearch sync

## v0.8.0 - CLI run + runtime wiring model
- [x] Decide explicit registry/plugin story
- [x] Run worker from YAML using registered projectors/appliers
- [x] Keep runtime wiring explicit; do not infer app projectors by name
- [x] Support Postgres store wiring from config
- [x] Document how applications embed or wrap the runner

## v0.9.0 - Worker throughput + batching
- [x] Configurable worker concurrency: N goroutines per sync/worker process
- [x] Configurable batch size: each routine fetches up to M jobs per pull/drain cycle
- [x] Preserve lease ownership semantics for every claimed job
- [x] Decide API shape for batch claim (for example ClaimNextBatch(sync_id, worker_id, limit, lock_for))
- [x] Keep per-job retry/dead/ghost handling observable inside the batch
- [x] Benchmark against playground baseline: one-job-per-RunOnce drain time vs N routines x M batch size
- [x] Expose config in CLI/YAML: workers.concurrency=N and workers.batch_size=M
- [x] Validate with large playground runs using fixed seed/source universe/mutation count
- [x] Plan doc: [PLAN_v0.9.0.md](PLAN_v0.9.0.md)
- [x] Release notes draft: [RELEASE_NOTES_V0.9.0.md](RELEASE_NOTES_V0.9.0.md)

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
- [x] Concrete Redis ProjectionApplier package
- [x] Upsert/delete support for cache-style projections
- [x] Explicit Redis client/config from Go code
- [x] Mandatory application-provided KeyFunc; no connector-owned default key format
- [x] Binary-safe storage of opaque Projection.Payload bytes
- [x] Configurable TTL: zero persists, positive expires, negative is invalid
- [x] Preserve the existing worker retry/max-attempts policy without connector-level retries
- [x] Focused tests around operation mapping and error handling
- [x] Redis playground: source write -> transactional Delta enqueue -> async Redis cache sync
- [x] Document Redis as a cache projection target with Redis/Valkey compatibility contract tests
- [x] Plan doc: [PLAN_v0.11.0.md](PLAN_v0.11.0.md)
- [x] Release notes draft: [RELEASE_NOTES_V0.11.0.md](RELEASE_NOTES_V0.11.0.md)

## v0.12.0 - Backfills
- [ ] Define backfill API and CLI shape
- [ ] Enqueue deltas for an existing source universe
- [ ] Support multiple workers for Postgres-backed backfills using existing lease/batch semantics
- [ ] Keep SQLite backfills single-worker only
- [ ] Support restart-safe backfills through application-provided stable ordering and optional checkpoint token
- [ ] Support dry-run mode where practical
- [ ] Document that DeltaFlow does not own source enumeration state unless the application provides a checkpoint strategy
- [ ] Define compatibility of each timing mode with backfills
- [ ] Add backfill playground: seed source records -> run backfill -> observe destination catch up
- [ ] Include large-source playground scenario with fixed seed/count
- [ ] Plan doc: [PLAN_v0.12.0.md](PLAN_v0.12.0.md)
- [ ] Release notes draft: [RELEASE_NOTES_V0.12.0.md](RELEASE_NOTES_V0.12.0.md)

## v0.13.0 - Connector module split
- [ ] Keep core module free of concrete connector client/driver dependencies
- [ ] PostgreSQL storage connector module
- [ ] SQLite storage connector module
- [ ] Elasticsearch applier connector module
- [ ] Redis/Valkey applier connector module
- [ ] Move bundled CLI into its own Go module
- [ ] Ship CLI as composition/distribution module
- [ ] Document explicit third-party connector registration model
- [ ] Avoid runtime `go get`, arbitrary auto-discovery, or Go plugin loading
- [ ] Add core-only consumer test proving no connector dependencies leak into core
- [ ] Require future connectors to start as separate Go modules
- [ ] Move full playground applications out of the core repository into separate playground repositories
- [ ] Keep only minimal examples, contract-test fixtures, and documentation snippets inside the core repository
- [ ] Document the difference between examples, playgrounds, connector modules, and custom application hosts
- [ ] Add a playground catalog in the README linking to external playground repositories
- [ ] Ensure external playgrounds pin DeltaFlow and connector module versions explicitly
- [ ] Release notes draft: [RELEASE_NOTES_V0.13.0.md](RELEASE_NOTES_V0.13.0.md)

## v0.13.5 - Connector management readiness
- [ ] Define connector capability manifest (storage/applier features, timing-mode compatibility, backfill support)
- [ ] Define connector lifecycle policy: versioning, deprecation, and support windows
- [ ] Add connector conformance/contract test matrix and minimum passing criteria
- [ ] Add connector operator guide: configuration validation, rollout strategy, and rollback expectations
- [ ] Define compatibility labels for production readiness (experimental, preview, stable)

## v0.14.0 - Projection timing modes
- [ ] Define Early Projection mode: application computes projection before enqueue
- [ ] Define Late Go Projection mode: custom worker binary registers Go Projectors and computes projection during worker execution
- [ ] Define Late SQL Projection mode: worker calls configured SQL function/procedure before apply
- [ ] Define what is persisted per mode: delta only, projection snapshot, payload hash, operation metadata
- [ ] Define required capabilities per mode: DB connection, projector registry, function/procedure name, timeout, error semantics
- [ ] Document tradeoffs: ordering, transaction boundaries, replay behavior, backfill behavior
- [ ] Add playground for Early Projection
- [ ] Add playground for Late Go Projection
- [ ] Add design note for Late SQL Projection, even if implementation is deferred

## v0.15.0 - SQL Server durable stores
- [ ] SQL Server durable store must honor current Postgres operational capabilities
- [ ] SQL Server schema for deltaflow_deltas + deltaflow_sync_jobs
- [ ] SQL Server DeltaStore + JobStore + DispatchStore
- [ ] Transactional enqueue using existing app transaction
- [ ] Lease claim semantics using SQL Server locking behavior
- [ ] Timeout reclaim behavior
- [ ] Migration command support
- [ ] Integration tests with container-backed SQL Server
- [ ] C# app example: business write + DeltaFlow enqueue in same transaction
- [ ] Document SQL Server production recommendations
- [ ] Document unsupported SQL Server behaviors or differences explicitly

## v0.16.0 - Producer SDKs for C# and TypeScript/JavaScript
- [ ] Define language-neutral enqueue contract for latest_state mode
- [ ] Define canonical projection key JSON rules shared across Go, C#, and TypeScript
- [ ] Define projection_key_hash compatibility test vectors
- [ ] Add .NET/C# producer SDK
- [ ] Add TypeScript/JavaScript producer SDK
- [ ] Support EnqueueLatestState for supported durable stores
- [ ] Support same-transaction enqueue where the app and DeltaFlow tables share the same database
- [ ] Support explicit cross-database enqueue orchestration guidance where they do not
- [ ] Provide examples:
  - C# + SQL Server app write + Delta enqueue
  - Node/TypeScript + Postgres app write + Delta enqueue
- [ ] Add compatibility tests proving C# and TS generate the same projection_key_hash as Go
- [ ] Document SDK non-goals: no worker runtime, no appliers, no leases, no retries, no connector registry
- [ ] Release notes draft: RELEASE_NOTES_V0.16.0.md

## v0.16.5 - Adoption enablement (SDK + memory)
- [ ] Publish adoption playbooks: first sync in one hour for Go, C#, and TypeScript producers
- [ ] Define producer-side payload/memory guidance (size limits, serialization strategy, retention expectations)
- [ ] Add SDK diagnostics for common enqueue failures and mismatch detection
- [ ] Add cross-language examples that align with timing modes and backfill strategy
- [ ] Document migration path from app-specific enqueue code to official SDKs

## v0.17.0 - Observability foundation
- [ ] Stable correlation fields: sync_id, projection_type, projection_key_hash, delta_id, job_id, worker_id, operation, result
- [ ] Worker-level telemetry/logging for claim, dispatch, retry, dead, ghost, and sync outcomes
- [ ] Applier-level telemetry/logging for operation, result, retryable, status_code, and latency
- [ ] Prometheus-compatible examples
- [ ] Grafana dashboard guidance
- [ ] Operational safety guide: retries, dead jobs, lease behavior, worker shutdown, and SQLite limits

## v0.18.0 - WATTA Lite: What happened to this projection?
- [ ] Add `deltaflow watta projection --sync <sync_id> --type <projection_type> --key '<json>'`
- [ ] Explain one Projection Identity from durable state only
- [ ] Show matching deltas
- [ ] Show dispatched jobs
- [ ] Show attempts, retries, leases, ghost detection, apply outcomes, final state, and last error
- [ ] Clearly distinguish retained facts from missing history
- [ ] Support human-readable output
- [ ] Support JSON output
- [ ] Redact projection payloads, credentials, and connector secrets by default
- [ ] Add playground/operator examples

## v0.18.5 - WATTA with logs
- [ ] Correlate WATTA output with structured worker/applier logs
- [ ] Support configured log sources
- [ ] Merge durable state and logs into one chronological explanation
- [ ] Clearly mark whether each fact came from DB state or logs

## v0.18.7 - Answers and operational memory
- [ ] Define durable fact model for explainability outputs (what is retained vs derived)
- [ ] Persist operator-facing answer artifacts for repeated investigation workflows
- [ ] Add "why not synced yet" and "what changed since last success" answer modes
- [ ] Add redaction policy for stored answer context and payload-adjacent metadata
- [ ] Document cost/retention tradeoffs for answer-memory storage

## v0.19.0 - API polish + docs + examples
- [ ] Stabilize public interfaces before v1.0: Projection, ProjectionType, ProjectionKey, ProjectionIdentity, Delta, SyncJob, Projector, ProjectionApplier, ProjectionOperation, DeltaStore, JobStore, SyncWorker
- [ ] Review naming consistency across packages
- [ ] Review error names and exported sentinel errors
- [ ] Review config/YAML naming
- [ ] Review CLI command names and flags
- [ ] Add minimal operational safety documentation
- [ ] Add example catalog
- [ ] Refresh README
- [ ] Refresh DESIGN.md
- [ ] Add migration/upgrade notes from pre-v1 versions
- [ ] Mark unstable or experimental APIs clearly
- [ ] Prepare v1.0 release checklist

## v1.0.0 - Stable latest_state OSS release
- [ ] Stable timing modes
- [ ] Stable public runtime contracts
- [ ] Stable durable store contracts
- [ ] Stable Postgres durable store
- [ ] Stable SQL Server durable store
- [ ] Stable SQLite single-node durable store
- [ ] Stable Elasticsearch applier
- [ ] Stable Redis applier
- [ ] Stable CLI basics: validate, migrate, run
- [ ] Documented transactional enqueue patterns
- [ ] Documented worker behavior: leases, retries, dead jobs, ghost handling, backfills
- [ ] Documented adoption ladder: in-memory -> SQLite single-node -> Postgres/SQL Server production
- [ ] Documented examples and playgrounds

## v1.1.0 - Non-idempotent delivery safety + duplicate detection
- [ ] Define projection payload canonicalization rules
- [ ] Add projection_payload_hash / projection_fingerprint concept
- [ ] Define a stable operation key: sync_id + projection_type + projection_key_hash + operation_type + payload_hash
- [ ] Extend ProjectionOperation metadata with projection_key_hash, payload_hash, and operation_key
- [ ] Add a delivery receipt ledger with started, succeeded, failed, duplicate, and outcome_unknown states
- [ ] Define applier result semantics: applied, skipped_no_change, skipped_duplicate, retryable_failure, permanent_failure, and outcome_unknown
- [ ] Define connector capabilities: native idempotency, reconcilable non-idempotent operation, and unsafe non-idempotent operation
- [ ] Use target-native idempotency/request keys when supported
- [ ] Support reconciliation by stable external reference when the target can be queried after an ambiguous result
- [ ] Do not blindly retry an outcome_unknown operation when duplicate effects cannot be prevented or detected
- [ ] Provide an operator resolution path for genuinely unverifiable outcomes
- [ ] Document differences among identity deduplication, content deduplication, operation deduplication, and exactly-once claims
- [ ] Add playground scenario: duplicate deltas -> same projection hash -> one effective target write
- [ ] Add failure scenario: target applies a non-idempotent operation but the response is lost -> reconcile or mark outcome_unknown without creating a duplicate

## v1.2.0 - Durable multi-target fan-out
- [ ] Allow one logical Delta to produce an independent delivery for each configured destination
- [ ] Give every destination delivery independent lease, retry, attempt, dead, replay, and observability state
- [ ] Define stable destination identity and delivery uniqueness
- [ ] Prevent one failed destination from replaying successful destinations
- [ ] Support attaching and backfilling a new destination without disturbing existing destinations
- [ ] Add playground scenario: one source Projection -> multiple appliers with independent failure and recovery

## v1.3.0 - Sync-Tree
- [ ] Introduce Sync-Tree as a DeltaFlow feature; DeltaFlow remains the project name
- [ ] Define nodes as projections/resources and edges as dependency or relationship requirements
- [ ] Support dependency-aware execution, such as Customer before Invoice
- [ ] Support external identity mapping from local Projection Identity to destination-native object ID
- [ ] Support relationship synchronization, such as contacts, organizations, deals, and their associations
- [ ] Represent execution dependencies as a DAG while allowing the synchronized business model to contain shared relationships
- [ ] Block dependent deliveries until prerequisites succeed, without consuming retry attempts
- [ ] Define deletion and unlink ordering, including dependent-first cleanup where required
- [ ] Support partial failure recovery, replay, reconciliation, and backfill of a subtree or selected nodes
- [ ] Add QuickBooks-style Customer -> Invoice and CRM-style Organization/Contact/Deal playground scenarios
