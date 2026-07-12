# DeltaFlow v0.12.0 Release Notes

DeltaFlow v0.12.0 is the first backfill-focused release. It turns the earlier backfill design into a practical adoption story centered on application-owned source scanning and DeltaFlow-owned durable enqueue guardrails.

> Status: draft. The notes below describe the intended v0.12.0 scope and should be confirmed against the final repository state before publication.

## What Changed

- Adds a dedicated [BACKFILL.md](BACKFILL.md) guide that defines the backfill ownership boundary and operator workflow.
- Adds `NewBackfillDelta` as a public helper for constructing backfill-oriented deltas correctly.
- Documents restart-safe backfill patterns using stable ordering, dedup windows, caller-owned checkpoints, and optional checkpoint tokens.
- Adds concrete examples for SQL seek pagination, high-watermark scans, and caller-owned cursor recovery.
- Improves Postgres `EnqueueBatch` throughput by using a multi-row insert path while preserving the existing batch API and duplicate-suppression semantics.
- Keeps the core repo focused on reusable APIs and documentation instead of reintroducing bundled backfill playground applications.
- Defines [lemenendez/deltaflow-playground-crm](https://github.com/lemenendez/deltaflow-playground-crm/) as the external playground home for the executable CRM backfill scenario.

## What Stayed the Same

- DeltaFlow still does not own source enumeration, checkpoint persistence, scheduling, or pause/resume orchestration.
- Backfill remains a user-owned scan plus DeltaFlow-owned enqueue guardrails, not a managed backfill runtime.
- SQLite remains a single-worker backfill store; v0.12.0 does not add distributed SQLite worker support.
- The core runtime, worker lease model, and connector boundaries remain application-composed.

## Upgrade Notes

- If you need backfill support, start from [BACKFILL.md](BACKFILL.md) and keep your source cursor/checkpoint state in application-owned storage.
- Use `NewBackfillDelta` plus `EnqueueBatch` for restart-safe bulk enqueue instead of building a separate Delta insertion path.
- When retrying a source slice after a crash or ambiguous failure, reuse the same dedup window for that logical lane.
- For Postgres-backed large backfills, prefer measured batch-size and pacing changes over one-off manual scripts that bypass `EnqueueBatch`.
- Do not expect a bundled backfill playground in the core repository; the CRM backfill example lives in the external playground repository.

## Verification

Before publishing the release, verify:

- Root tests pass with `go test ./...`.
- The public `NewBackfillDelta` helper is covered by focused tests.
- Postgres batch enqueue still reports inserted and duplicate counts correctly after the multi-row insert optimization.
- [BACKFILL.md](BACKFILL.md) clearly states that the caller owns source enumeration and checkpoints.
- The v0.12.0 docs describe the external backfill playground boundary rather than implying bundled playground applications.
- The external CRM playground demonstrates the backfill scenario before v0.12.0 is published.
