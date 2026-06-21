# Plan v0.9.0

Goal: improve worker throughput with configurable concurrency and batching while
preserving lease ownership guarantees, per-job retry/dead/ghost semantics, and
observability.

## Scope

- [ ] Add configurable worker concurrency (`workers.concurrency`) for parallel job processing.
- [ ] Add configurable batch size (`workers.batch_size`) so each routine can claim multiple jobs per cycle.
- [ ] Define and implement batch-claim API shape in stores (for example, `ClaimNextBatch(sync_id, worker_id, limit, lock_for)`).
- [ ] Preserve lease ownership checks for every claimed job in batched execution paths.
- [ ] Keep per-job retry, dead-letter, and ghost handling behavior explicit and observable inside batch runs.
- [ ] Expose new throughput controls through CLI/YAML config and validation.
- [ ] Add benchmarks and playground measurements comparing baseline one-job drain vs `N x M` throughput configuration.
- [ ] Add test coverage for batch claim semantics, concurrency safety, and failure handling under load.

## Throughput Model

Target behavior in v0.9.0:

- A worker process runs `N` routines per configured pipeline.
- Each routine claims up to `M` jobs per pull/drain cycle.
- Each claimed job still follows the existing lifecycle:
  - claim with lease ownership
  - projector execution
  - applier execution
  - state transition (`synced` / `retrying` / `dead`, with `ghost_detected` on synced ghost-delete handling)
- Lease checks remain authoritative on every transition and renewal path.

Config surface:

- `workers.concurrency`: number of worker routines per pipeline (default remains safe/minimal).
- `workers.batch_size`: max jobs claimed per routine cycle (default remains safe/minimal).

## Benchmark and Validation Plan

Benchmark and playground validation should use fixed simulation inputs:

- fixed seed
- fixed source universe size
- fixed mutation count

Compare:

- current baseline: one-job-per-cycle drain behavior
- target throughput modes: `N` routines x `M` batch claim size

Collect at least:

- total drain time
- jobs processed/sec
- retry/dead counts (must remain behaviorally consistent)
- lease conflict/ownership error counts (must not regress)

## Acceptance Criteria

- [ ] CLI/YAML supports and validates `workers.concurrency` and `workers.batch_size`.
- [ ] Store layer supports batch claims with deterministic lease ownership behavior.
- [ ] Worker executes batched jobs safely with per-job outcomes preserved.
- [ ] Existing single-job mode remains supported and behaviorally equivalent when `concurrency=1` and `batch_size=1`.
- [ ] Repository tests remain green.
- [ ] Throughput benchmarks demonstrate measurable improvement over baseline in playground scenarios.

## Out of Scope

- New target appliers (Redis/Postgres) remain v0.10.0.
- Full operational telemetry/trace command set remains v0.11.0.
- Backfill orchestration and APIs remain v0.12.0.
