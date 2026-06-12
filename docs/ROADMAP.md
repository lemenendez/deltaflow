# Roadmap

v0.1.0   Skeleton + vocabulary + DESIGN.md

v0.2.0   Core domain model + in-memory stores
         - intentionally non-durable in this milestone
         - Projection
         - ProjectionType
         - ProjectionKey
         - ProjectionIdentity
         - Delta
         - SyncJob
         - Projector
         - ProjectionApplier
         - ProjectionOperation
         - in-memory DeltaStore + JobStore + DispatchStore
         - Delta->SyncJob dispatch + worker FSM tests

v0.3.0   SQL schema + durable store implementation
         - deltaflow_deltas + deltaflow_sync_jobs
         - durable DeltaStore + JobStore contracts
         - outbox/jobs split persists in SQL
         - canonical projection_key hashing
         - baseline leases: claim with lock_for + timeout reclaim
         - integration tests (container-backed postgres)

v0.4.0   Postgres lease hardening + transactional app patterns
         - insert Deltas transactionally with application writes
         - lease renewal/heartbeat semantics
         - lease ownership checks on job state transitions
         - advanced lease observability and operations (slog-first + Prometheus-compatible telemetry)
         - playground for concurrent workload simulation (writers + workers)

v0.5.0   latest_state MVP
         - public SyncWorker.RunOnce API
         - documented Projector.Project() + ProjectionApplier.Apply() runtime contract
         - documented Delta Ghost handling with ErrProjectionNotFound -> delete
         - documented app transaction example using EnqueueInTx
         - point users at existing fake/simple applier playgrounds
         - README/DESIGN refresh for current latest-state MVP status

v0.6.0   CLI + minimal YAML
         - run worker
         - config loading
         - sync_id
         - retry config
         - no connector registry

v0.7.0   Worker throughput + batching
         - configurable worker concurrency:
           N goroutines per sync/worker process
         - configurable batch size:
           each routine fetches up to M jobs per pull/drain cycle
         - preserve lease ownership semantics for every claimed job
         - decide API shape for batch claim, for example ClaimNextBatch(sync_id, worker_id, limit, lock_for)
         - keep per-job retry/dead/ghost handling observable inside the batch
         - benchmark against playground baseline:
           current one-job-per-RunOnce drain time vs N routines x M batch size
         - expose config in CLI/YAML:
           worker.concurrency=N
           worker.batch_size=M
         - validate with large playground runs using fixed seed/source universe/mutation count

v0.8.0   Redis/Postgres appliers

v0.9.0   Elasticsearch applier + search example

v0.10.0  Metrics + logs + operational safety
         - entity trace/debug command:
           given sync_id + projection_type + projection_key, explain "what happened"
           by fetching matching deltas, mapped jobs, attempts, final state, last error,
           ghost flag, lease ownership history, and related worker/lease logs
         - trace should support playground/operator usage first, for example:
           make trace TYPE=ProductSearchDocument KEY='{"product_id":"sku-004"}'
         - later promote to CLI:
           deltaflow trace --sync <sync_id> --type <projection_type> --key '<json>'
         - consider persisting/logging projection_key_hash and job_id in structured logs
           so traces can join SQL state and slog output reliably

v0.11.0  Backfills

v0.12.0  API polish + docs + examples

v1.0.0   Stable latest_state OSS release
