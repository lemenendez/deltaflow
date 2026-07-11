# Plan v0.11.2

Goal: remove the bundled playground applications from the core repository while keeping only the minimal in-memory playground for quick local sanity checks.

## Product Positioning

- v0.11.2 is a repository-cleanup milestone, not a feature milestone.
- The core repo should keep the smallest useful playground surface needed for fast local verification.
- The removed playgrounds should live outside the core repository as separate playground repositories or modules.
- Backfill work in v0.12.0 should start from the cleaned-up boundary, not from the old bundled demos.

## Scope

- Remove bundled playground applications from the core repository.
- Keep `playground/01-in-memory` as the lightweight local playground.
- Remove the mistakenly shared `pkg/examples/contactsruntime` package from the core repository.
- Externalize the removed playgrounds into separate repositories or modules.
- Preserve the documentation trail so users can find the new external playground locations later.

## Implementation Steps

1. Identify every playground directory except the in-memory one as a removal candidate.
2. Back up the removed playgrounds into `backups/deltaflow-playgrounds-backup.zip`.
3. Run the root test suite from the repository root with `go test ./...`.
4. Run the integration test module separately from `integration/` with `go test ./...`.
5. Remove the bundled playground directories from the core repository.
6. Add `pkg/examples/contactsruntime` to the backup zip and remove it from the repository.
7. Update the playground documentation and catalog to point to the external locations.
8. Keep the in-memory playground documentation in place so local verification remains available.
9. Verify the core repository still builds and tests cleanly after the cleanup.

## Testing Plan

- Run `go test ./...` from the repository root.
- Run `go test ./...` from the `integration/` module.
- Confirm the remaining in-memory playground still runs.
- Confirm the removed playground content is present in the backup archive.
- Confirm `pkg/examples/contactsruntime` is present in the backup archive and removed from the repository.
- Review documentation links for any references that now need to point to external repositories.

## Acceptance Criteria

- Only the in-memory playground remains bundled in the core repository.
- The removed playgrounds are safely archived in the backup zip.
- `pkg/examples/contactsruntime` is removed and archived in the backup zip.
- The repository docs clearly reflect the new boundary.
- Root tests and integration tests remain green after the cleanup.

## Out of Scope

- New backfill feature implementation.
- New playground scenarios for v0.12.0.
- Connector module splitting.
- Any changes to the in-memory playground behavior beyond keeping it available.
