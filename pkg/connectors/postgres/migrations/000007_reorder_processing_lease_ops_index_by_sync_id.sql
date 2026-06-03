BEGIN;

-- Lease helper queries now use two shapes:
--
-- scoped:
--   WHERE state = 'processing'
--   AND ...locked_until...
--   AND sync_id = $N
--   ORDER BY locked_until ASC [NULLS FIRST], updated_at ASC, id ASC
--
-- unscoped:
--   WHERE state = 'processing'
--   AND ...locked_until...
--   ORDER BY locked_until ASC [NULLS FIRST], updated_at ASC, id ASC
--
-- Prioritize the scoped incident path by leading with sync_id so Postgres can
-- seek directly to a single sync partition before scanning in lease order.
DROP INDEX IF EXISTS deltaflow.deltaflow_sync_jobs_processing_lease_ops_idx;

CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_processing_lease_ops_idx
    ON deltaflow.deltaflow_sync_jobs (sync_id, locked_until ASC NULLS FIRST, updated_at ASC, id ASC)
    WHERE state = 'processing';

COMMIT;
