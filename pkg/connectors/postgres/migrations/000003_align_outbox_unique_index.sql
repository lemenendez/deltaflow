BEGIN;

-- Align the partial unique index predicate with DispatchStore ON CONFLICT
-- inference so outbox dispatch can rely on migration-owned schema only.
DROP INDEX IF EXISTS deltaflow.deltaflow_sync_jobs_outbox_delta_unique;

CREATE UNIQUE INDEX IF NOT EXISTS deltaflow_sync_jobs_outbox_delta_unique
    ON deltaflow.deltaflow_sync_jobs (delta_id)
    WHERE origin = 'outbox';

COMMIT;
