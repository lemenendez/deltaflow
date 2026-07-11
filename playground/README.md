# Playground

This folder hosts standalone modules that showcase specific DeltaFlow configurations and integration patterns.

Each playground is intentionally isolated with its own `go.mod` so it can:

- depend on the public DeltaFlow API like an external consumer, and use internal helpers where a demo is specifically about repo-local orchestration
- pull extra libraries needed for demos (for example faker/test data packages)
- evolve independently from the root module

Available playgrounds:

- `01-in-memory`: latest-state customer cache sync simulation with in-memory source and target stores

v0.11.2 removed the larger bundled playground applications from the core repository.
They were archived for backup and are expected to move to external playground repositories/modules.
