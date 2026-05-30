# Playground

This folder hosts standalone modules that showcase specific DeltaFlow configurations and integration patterns.

Each playground is intentionally isolated with its own `go.mod` so it can:

- depend on the public DeltaFlow API like an external consumer
- pull extra libraries needed for demos (for example faker/test data packages)
- evolve independently from the root module

Available playgrounds:

- `01-in-memory`: latest-state customer cache sync simulation with in-memory source and target stores
