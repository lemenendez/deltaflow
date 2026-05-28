# Connectors

In early Deltaflow versions, connectors should live under `internal/connectors`.
This keeps experimental connectors out of the public API while they are still
changing.

Once a connector is stable, it can graduate to a user-facing package under
`pkg/connectors` or be distributed from a separate repository.
