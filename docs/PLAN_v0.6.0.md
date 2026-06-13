# Plan v0.6.0

Goal: add a small operator-facing CLI and one minimal YAML configuration shape for validation and Postgres schema migration.

## Scope

- [x] Add `cmd/deltaflow/main.go`.
- [x] Add `internal/cli/root.go`.
- [x] Add `internal/cli/validate.go`.
- [x] Add `internal/cli/migrate.go`.
- [x] Defer `internal/cli/run.go` until the runtime wiring model is designed.
- [x] Add config loading from a single YAML shape.
- [x] Support environment interpolation for string values such as `${DELTAFLOW_STORE_DSN}`.
- [x] Add config validation for store, workers, and pipelines.
- [x] Add Postgres migration command using existing `pkg/connectors/postgres/migrations`.
- [x] Update README and ROADMAP references for the v0.6 CLI shape.

## YAML Shape

Use one shape only. Do not include a `version` field yet.

```yaml
store:
  type: postgres
  dsn: ${DELTAFLOW_STORE_DSN}

workers:
  concurrency: 8
  lease_ttl: 30s
  pull_size: 1
  max_attempts: 5

pipelines:
  - name: contacts-to-elasticsearch
    sync_id: contacts-to-elasticsearch
    source:
      type: postgres-outbox
      projection_type: contact
    projector:
      name: contact-projector
    target:
      type: elasticsearch
      index: contacts
    applier:
      mode: upsert
```

## Commands

- `deltaflow validate --config ./cmd/deltaflow/deltaflow.yaml`
- `deltaflow migrate --config ./cmd/deltaflow/deltaflow.yaml`

`deltaflow run --config ./cmd/deltaflow/deltaflow.yaml` is deferred. A real run command needs a clear runtime wiring model for application projectors and appliers; v0.6 should not introduce a connector registry or pretend YAML names can instantiate application code.

## Config Rules

- `store.type` is required and only supports `postgres` in v0.6.
- `store.dsn` is required for Postgres.
- `workers.concurrency` must be positive.
- `workers.lease_ttl` must parse as a positive Go duration.
- `workers.pull_size` must be positive when provided.
- `workers.max_attempts` must be positive when provided.
- `pipelines` must contain at least one pipeline.
- `pipeline.name` and `pipeline.sync_id` are required.
- `source.type` is required and only supports `postgres-outbox` in v0.6.
- `source.projection_type` is required.
- `projector.name` is required but is metadata only in v0.6.
- `target.type` and `target.index` are validated as config, but concrete target appliers are not created in v0.6.
- `applier.mode` is required and should initially support `upsert`.

## Acceptance Criteria

- `deltaflow validate --config deltaflow.yaml` exits successfully for the canonical YAML shape.
- `deltaflow validate --config deltaflow.yaml` reports actionable errors for missing DSN, malformed durations, empty pipeline lists, missing `sync_id`, and unsupported store/source types.
- `${DELTAFLOW_STORE_DSN}` resolves from the environment before validation.
- `deltaflow migrate --config deltaflow.yaml` applies the existing Postgres migrations against `store.dsn`.
- The CLI does not expose a working `run` command in v0.6.
- The CLI does not include a connector registry in v0.6.

## Deferred

- Worker run loop and shutdown policy.
- Runtime wiring model for projectors and appliers.
- Connector registry or plugin model.
- Elasticsearch applier implementation.
- Backfill command.
- Worker batching and throughput controls beyond config validation.
