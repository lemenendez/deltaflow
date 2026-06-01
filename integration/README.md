# DeltaFlow Container Integration Tests

This module contains real database integration tests that run against disposable containers.

Why this lives in its own module:

- Keep heavy test-only dependencies (testcontainers, container drivers) out of the core library module.
- Let integration test dependencies/versioning evolve independently.
- Provide a clean expansion point for multi-database providers (Postgres now, MySQL/MSSQL later).

## Current support

- `postgres` provider (default): boots a `postgres:17` container, applies compatibility SQL (`uuidv7()` shim), and executes all SQL migrations from `pkg/connectors/postgres/migrations`.

## Run

From this directory:

- `DELTAFLOW_IT_ENABLE=1 go test -tags=integration ./...`

Optional provider selector:

- `DELTAFLOW_IT_DB=postgres` (default)

If `DELTAFLOW_IT_ENABLE` is not set to `1`, tests are skipped.

## Add a new database provider

1. Add a provider implementation in `testenv/` that satisfies `Provider`.
2. Update `testenv.NewFromEnv` to register the new provider key.
3. Reuse the same suite tests to verify store behavior across engines.
