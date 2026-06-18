# Plan v0.8.0

Goal: deliver the first CLI runtime wiring model so applications can run worker
pipelines from YAML while keeping projector/applier binding explicit in startup
registration code.

## Scope

- [x] Add `deltaflow run` command to execute worker runtime from YAML config.
- [x] Introduce an explicit runtime registry for projector/applier resolution.
- [x] Keep runtime wiring explicit: no projector inference by config name beyond exact registered keys.
- [x] Wire Postgres stores from config (`store.type=postgres`, `store.dsn`) into worker execution.
- [x] Run one worker cycle per configured pipeline (`RunOnce`) as the initial v0.8 implementation slice.
- [ ] Add end-to-end integration coverage for `deltaflow run` with real Postgres + Elasticsearch.
- [ ] Expand `run` from one-shot `RunOnce` to long-running worker loop with clean shutdown semantics.

## Runtime Wiring Model

New runtime package:

```text
pkg/runtime
```

Runtime decisions in v0.8:

- Startup code explicitly registers projector factories by `pipelines[].projector.name`.
- Startup code explicitly registers applier factories by `pipelines[].target.type`.
- Registry uses exact-name map lookups with no reflection.
- Duplicate registrations panic during startup.
- CLI `run` validates registration presence before opening the store connection.
- Missing registrations fail fast with clear errors:
  - `runtime projector not registered`
  - `runtime applier not registered`

This preserves the design guardrail: DeltaFlow does not infer application
projectors by convention.

## Initial CLI Run Behavior

Command:

```bash
go run ./cmd/deltaflow run --config ./cmd/deltaflow/deltaflow.yaml
```

Current behavior for this first v0.8 slice:

- Loads and validates YAML config.
- Requires explicit runtime registry wiring during startup.
- Connects to Postgres using `store.dsn`.
- Builds `DeltaStore`, `JobStore`, and `DispatchStore` from Postgres connector packages.
- Resolves each configured pipeline from the runtime registry.
- Executes one `SyncWorker.RunOnce` per pipeline.

## Acceptance Criteria

- [x] `deltaflow run` is available in CLI root command.
- [x] Runtime registry supports explicit projector/applier registration and resolution.
- [x] CLI fails fast when runtime registrations are missing.
- [x] CLI run path compiles and is covered by unit tests.
- [x] Existing repository test suite remains green.
- [ ] Integration coverage demonstrates a full config-driven run against Postgres + Elasticsearch.

## Out of Scope

- Dynamic plugin loading.
- Name-based projector inference beyond explicit registration.
- Worker batching and throughput optimizations (v0.9.0).
- Additional target appliers beyond current Elasticsearch path.
- Full operational telemetry/trace CLI features (v0.11.0).
