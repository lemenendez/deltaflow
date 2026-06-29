# Playground

This folder hosts standalone modules that showcase specific DeltaFlow configurations and integration patterns.

Each playground is intentionally isolated with its own `go.mod` so it can:

- depend on the public DeltaFlow API like an external consumer, and use internal helpers where a demo is specifically about repo-local orchestration
- pull extra libraries needed for demos (for example faker/test data packages)
- evolve independently from the root module

Available playgrounds:

- `01-in-memory`: latest-state customer cache sync simulation with in-memory source and target stores
- `02-postgres`: contact delta flow using Postgres DeltaStore, DispatchStore, and JobStore with docker compose
- `03-postgres-e-commerce`: concurrent product search workload with deterministic web/logistics writers, Elasticsearch, two DeltaFlow workers, ghost deletion, retry, and dead-letter simulation
- `04-postgres-crm`: concurrent CRM read-model workload with simulated Redis views, Elasticsearch search fanout, two DeltaFlow workers, ghost deletion, retry, and dead-letter simulation
- `05-sqlite`: SQLite durable-store example for the supported single-node / single-worker model with transactional source write + delta enqueue + worker apply cycle
- `06-postgres-redis`: Postgres transactional source/outbox synchronized asynchronously into Redis, with application-owned keys, TTL refresh, retry, ghost deletion, and Valkey compatibility
