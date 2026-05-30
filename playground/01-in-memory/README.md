# 01-in-memory

This playground demonstrates DeltaFlow's latest-state flow in memory:

- Build a tiny deterministic delta batch for `Customer` projections
- Project the latest state from an in-memory source store
- Apply `upsert` or `delete` operations to an in-memory target cache index
- Showcase ghost handling with one missing customer (`cus-999`)

## Code layout

- `main.go`: tiny orchestration and `runDeltas` processing loop
- `custom_scenario.go`: scenario-specific adapter code (fixed source data + in-memory cache target)

## Why DeltaStore/JobStore are not used here

`DeltaStore`, `JobStore`, and `DispatchStore` are public interfaces in DeltaFlow.

This first playground is intentionally minimal and focuses on the core latest-state projector/applier path, so it runs directly over a synthetic delta slice.

Later playgrounds can add concrete in-memory implementations of those stores to demonstrate queueing, dispatching, worker claims, retries, and dead-letter behavior end to end.

This scenario mirrors a common cache/search use case (website customer profile cache, CRM read model).

## About non-idempotent order handoffs

Order forwarding to non-idempotent systems is valid, but it is a different shape of integration.

For non-idempotent side effects, a safe pattern is:

- keep DeltaFlow for idempotent projections/read models
- use a dedicated delivery component with idempotency keys, dedupe, and delivery-state tracking for side effects

## Run

```bash
cd playground/01-in-memory
go run .
```

The scenario is fixed on purpose so the output is easy to understand in demos.
