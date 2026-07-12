# Plan v0.12.0

Goal: turn the existing backfill design into a practical, adoption-ready workflow built on application-owned source scanning and DeltaFlow-owned enqueue guardrails.

## Product Positioning

- v0.12.0 is not a managed backfill engine milestone.
- DeltaFlow should provide the safe enqueue primitives, helper APIs, and operator guidance needed for restart-safe bulk catch-up.
- Source enumeration, checkpoint storage, scheduling, and pacing remain application-owned responsibilities.
- The main adoption artifact should be a dedicated backfill guide, not just scattered roadmap bullets.
- The backfill story must fit the current durable store split: Postgres supports multi-worker backfill processing, SQLite remains single-worker only.

## Scope

- Add a dedicated [BACKFILL.md](BACKFILL.md) guide for operator and application adoption.
- Add a concrete `PLAN_v0.12.0.md` implementation plan and keep the roadmap linked to it.
- Provide a `NewBackfillDelta` helper that makes the intended dedup-window flow easier to use correctly.
- Document backfill as user-owned source scan plus `EnqueueBatch`.
- Provide at least one SQL seek-pagination example.
- Provide at least one checkpoint/high-watermark recovery example.
- Document restart-safe backfills using stable ordering and caller-owned progress.
- Define timing-mode compatibility notes for backfill usage.
- Provide large-backfill guidance covering batch size, pacing, worker count, destination throughput, and retention/pruning.
- Define the contract for the external CRM playground repository that demonstrates source seeding, enqueue, and destination catch-up.
- Evaluate whether durable stores should expose optional bulk insert optimizations behind the existing batch API.

## Non-Goals

- No managed scheduler, pause/resume engine, or DeltaFlow-owned checkpoint runtime.
- No new generic stock CLI backfill command in the core operator binary.
- No SQLite multi-process or distributed-worker support.
- No attempt to generalize every source enumeration pattern into framework-owned abstractions.

## Workstreams

### 1. Public API and durable store behavior

1. Define and implement `NewBackfillDelta` on top of the existing Delta semantics.
2. Confirm the helper does not bypass existing dedup-key, projection-key, or origin rules.
3. Evaluate optional `EnqueueBatch` throughput improvements for durable stores without changing the public batch contract.
4. Preserve current duplicate suppression semantics and batch result reporting.
5. Keep SQLite behavior explicitly single-worker and document unsupported concurrency expectations.

### 2. Backfill guide and examples

1. Publish [BACKFILL.md](BACKFILL.md) as the main adoption guide.
2. Explain the invariant: DeltaFlow returns enqueue results, the caller owns the source checkpoint.
3. Add a SQL seek-pagination example with a stable ordering requirement.
4. Add a high-watermark example and a caller-owned cursor recovery example.
5. Add dry-run guidance where practical, including how to validate scan order and window construction before enqueue.
6. Add large-backfill tuning guidance for queue pressure, worker count, destination bulk APIs, and pruning.

### 3. Runtime compatibility and playground validation

1. Document how backfills behave under current timing modes and note open questions for future timing-mode expansion.
2. Validate that Postgres-backed backfills can use existing worker lease and batch semantics at normal runtime scale.
3. Keep SQLite examples limited to single-worker operation.
4. Use [lemenendez/deltaflow-playground-crm](https://github.com/lemenendez/deltaflow-playground-crm/) as the external backfill playground repository, with a fixed-seed scenario and a larger source-count variant.
5. Include one destination-oriented example, preferably Elasticsearch population or a CRM read model, because that is a common adoption path.

## Proposed Implementation Order

1. Create `BACKFILL.md` with the stable conceptual model and operator workflow.
2. Add `NewBackfillDelta` and its focused tests.
3. Add source-scan examples and checkpoint examples.
4. Add or update durable store tests if `EnqueueBatch` performance-related behavior changes.
5. Write the external playground repository contract and link target.
6. Finish the CRM playground backfill scenario before closing v0.12.0.
7. Refresh README, DESIGN references, roadmap links, and release notes.

## Testing Plan

- Run focused unit tests for any new helper API and touched store behavior.
- Run `go test ./...` from the repository root.
- Run `go test ./...` from `integration/` if Postgres backfill behavior or contracts are touched.
- Manually review [BACKFILL.md](BACKFILL.md) against [DESIGN.md](DESIGN.md) to ensure the ownership boundary is consistent.
- Confirm examples use stable ordering and do not imply DeltaFlow-owned checkpointing.
- Confirm roadmap and release-notes links point to the new docs.

## Acceptance Criteria

- v0.12.0 has a dedicated backfill guide suitable for application adoption.
- The guide clearly states that backfill is user-owned source enumeration plus DeltaFlow-owned enqueue guardrails.
- The repo exposes a helper path for constructing backfill deltas correctly.
- At least one restart-safe scan example and one checkpoint strategy example are documented.
- Dry-run behavior is documented as application-owned preflight validation rather than a separate DeltaFlow enqueue API.
- Large-backfill tuning covers batch size, pacing, worker count, destination throughput, and retention/pruning.
- Current timing-mode compatibility is documented for `latest_state`, with future timing modes left roadmap-scoped.
- Postgres and SQLite backfill constraints are explicit and consistent with current runtime guarantees.
- The core repo documents the expected shape of the external backfill playground and does not reintroduce bundled playground applications.
- The CRM playground demonstrates source seeding, backfill enqueue, worker drain, and destination catch-up before v0.12.0 is closed.

## Resolved Decisions

- `NewBackfillDelta` keeps `OriginOperationType` caller-provided so source-side business semantics remain explicit.
- Dry-run stays documentation-only in v0.12.0: callers validate source scan order and delta construction before calling `EnqueueBatch`.
- Queue-depth helpers are deferred; v0.12.0 documents pacing and existing store/runtime observations instead of adding a new public API.
- Elasticsearch population guidance is enough for the first backfill release; Redis cache rehydration can be added later if needed.
