v0.1.0   Skeleton + vocabulary + DESIGN.md

v0.2.0   Core domain model + in-memory DeltaStore
         - Projection
         - ProjectionType
         - ProjectionKey
         - ProjectionIdentity
         - Delta
         - Projector
         - ProjectionApplier
         - ProjectionOperation
         - in-memory DeltaStore
         - in-memory dispatcher
         - in-memory worker FSM tests

v0.3.0   SQL schema + Store contracts
         - deltaflow_deltas
         - DeltaStore interface
         - one table acts as both transactional outbox and worker job queue
         - canonical projection_key hashing

v0.4.0   Postgres DeltaStore + leases
         - insert Deltas transactionally with application writes
         - claim Deltas with FOR UPDATE SKIP LOCKED
         - retry scheduling
         - dead Deltas
         - integration tests

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
