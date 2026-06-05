# Playground

This folder hosts standalone modules that showcase specific DeltaFlow configurations and integration patterns.

Each playground is intentionally isolated with its own `go.mod` so it can:

- depend on the public DeltaFlow API like an external consumer, and use internal helpers where a demo is specifically about repo-local orchestration
- pull extra libraries needed for demos (for example faker/test data packages)
- evolve independently from the root module

Available playgrounds:

- `01-in-memory`: latest-state customer cache sync simulation with in-memory source and target stores
- `02-postgres`: contact delta flow using Postgres DeltaStore, DispatchStore, and JobStore with docker compose
- `03-postgres-e-commerce`: concurrent product search workload with deterministic web/logistics writers, two DeltaFlow workers, ghost deletion, retry, and dead-letter simulation
- `04-postgres-crm`: concurrent CRM read-model workload with Redis/OpenSearch fanout simulation, two DeltaFlow workers, ghost deletion, retry, and dead-letter simulation
