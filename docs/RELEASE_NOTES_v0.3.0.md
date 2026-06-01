# v0.3.0

- Added durable SQL schema for deltas and sync jobs.
- Implemented durable Postgres DeltaStore, JobStore, and DispatchStore.
- Added canonical projection_key hashing and sync_id-scoped pull/claim behavior.
- Added baseline lease semantics (claim, timeout, reclaim) for SyncJobs.
- Added container-backed Postgres integration test module and suite.
