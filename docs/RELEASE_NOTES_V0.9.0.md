# v0.9.0

DeltaFlow v0.9.0 is focused on worker throughput improvements through
configurable concurrency and batching, while keeping correctness guardrails
(lease ownership, per-job retry/dead/ghost behavior) intact.

This release aims to improve drain performance for larger workloads without
changing the fundamental latest-state reconciliation model.

## Planned Highlights

- Add configurable worker concurrency per pipeline (`workers.concurrency`).
- Add configurable batch claim size per routine (`workers.batch_size`).
- Introduce batch-claim store API semantics for efficient job acquisition.
- Preserve lease ownership checks on every claimed job and state transition.
- Keep retry, dead-letter, and ghost handling observable per job inside batches.
- Extend CLI/YAML validation and runtime wiring for throughput settings.
- Add benchmark comparisons against the one-job baseline using playground scenarios.

## Throughput Model

- Worker process can run `N` routines per pipeline.
- Each routine can claim up to `M` jobs per cycle.
- Per-job outcomes remain explicit and map to canonical job states (`synced`, `retrying`, `dead`) plus `ghost_detected` on synced ghost-delete handling.
- `concurrency=1` and `batch_size=1` should preserve current baseline behavior.

## Verification Plan

- `go test ./...`
- Focused unit tests for batch claim and lease semantics.
- Throughput benchmarks and playground comparison runs using fixed seed and workload shape.

## Deferred

- Redis/Postgres target appliers remain v0.10.0.
- Metrics/logging operational safety and trace tooling remain v0.11.0.
