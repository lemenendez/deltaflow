# v0.8.0

DeltaFlow v0.8.0 starts the CLI runtime wiring milestone with an explicit
registry model for projector/applier construction and an initial
configuration-driven `deltaflow run` path.

This release intentionally favors explicit host wiring over magic runtime
inference, aligning with the roadmap guardrail.

Runtime registration is intentionally simple: exact-name map lookups with
explicit startup registration and no reflection-based discovery.

## Highlights

- Added `deltaflow run` command to execute one worker cycle per configured pipeline.
- Added `pkg/runtime` registry with explicit projector and applier factory registration.
- Kept runtime registration explicit with exact-name map wiring.
- Added fast-fail runtime wiring checks before DB connection:
  - missing projector registration
  - missing applier registration
- Wired Postgres store setup from config into `run` (`DeltaStore`, `JobStore`, `DispatchStore`).
- Added tests for runtime registry resolution and config-to-worker runtime execution.
- Kept Cobra error output behavior unchanged (`SilenceUsage` / `SilenceErrors` still respected).

## Runtime Model

`run` expects startup registration to provide factories:

- `pipelines[].projector.name` -> projector factory
- `pipelines[].target.type` -> applier factory

DeltaFlow does not infer application projectors by name convention and does not
construct application-specific projectors from YAML alone.

## Verification

- `go test ./...`

## Deferred

- Long-running worker process mode for `deltaflow run` (current behavior is one cycle per pipeline).
- End-to-end integration test for CLI `run` with containerized Postgres + Elasticsearch.
- Worker batching and throughput improvements remain v0.9.0.
