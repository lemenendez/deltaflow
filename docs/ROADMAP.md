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
         - advanced lease observability and operations
         - layground for concurrent workload simulation (writers + workers) 

v0.5.0   latest_state MVP
         - Projector.Project()
         - ProjectionApplier.Apply()
         - Delta Ghost handling
         - app transaction example
         - fake/simple applier example

v0.6.0   CLI + minimal YAML
         - run worker
         - config loading
         - sync_id
         - retry config
         - no connector registry

v0.7.0   Redis/Postgres appliers

v0.8.0   Elasticsearch applier + search example

v0.9.0   Metrics + logs + operational safety

v0.10.0  Backfills

v0.11.0  API polish + docs + examples

v1.0.0   Stable latest_state OSS release
