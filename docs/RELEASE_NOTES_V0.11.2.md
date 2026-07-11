# DeltaFlow v0.11.2 Release Notes

DeltaFlow v0.11.2 is a cleanup release that removes the bundled playground applications from the core repository while keeping the minimal in-memory playground available for local sanity checks.

> Status: draft. The notes below describe the intended cleanup and should be confirmed against the final repository state before publication.

## What Changed

- Keeps `playground/01-in-memory` as the lightweight bundled playground.
- Removes the larger bundled playground applications from the core repository.
- Removes the mistakenly shared `pkg/examples/contactsruntime` package from the core repository.
- Archives the removed playgrounds and `pkg/examples/contactsruntime` as a GitHub Release asset named `deltaflow-playgrounds-backup.zip` for reference and recovery.
- Repositions the stock CLI as an operator tool: `doctor` and `migrate` remain in the generic binary, while worker execution stays application-owned.
- Renames the public stock config-check command to `doctor` and keeps `validate` as a compatibility alias.
- Sets up a cleaner boundary for the v0.12.0 backfill work.

## What Stayed the Same

- Core library behavior remains unchanged.
- The existing in-memory playground remains available.
- The main DeltaFlow contracts, worker behavior, and connector APIs are not changed by this release.
- Application-specific worker hosts can still be built on top of `pkg/runtime`.

## Upgrade Notes

- If you relied on the removed playgrounds, download `deltaflow-playgrounds-backup.zip` from the v0.11.2 GitHub Release assets, or move to the future external playground repositories/modules.
- If you relied on `pkg/examples/contactsruntime`, move to your own host/runtime wiring in application code.
- If you used `deltaflow validate`, prefer `deltaflow doctor`; `validate` remains accepted as an alias.
- If you previously expected the stock CLI to run workers, move that runtime composition into an application-specific binary.
- Documentation should be updated to point to the new external locations once they are published.
- v0.11.2 does not introduce new runtime APIs.

## Verification

Before publishing the release, verify:

- Root tests pass with `go test ./...`.
- The in-memory playground still works as expected.
- The v0.11.2 release includes a `deltaflow-playgrounds-backup.zip` asset.
- The release asset contains the removed playgrounds and `pkg/examples/contactsruntime`.
- The stock CLI exposes `doctor` and `migrate`, but not a generic `run` command.
- Remaining docs no longer present the removed playgrounds as bundled examples.
