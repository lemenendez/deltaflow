BEGIN;

-- Operational lease helpers use separate scoped and unscoped query shapes:
--
--   scoped:   WHERE state = 'processing' AND locked_until ... AND sync_id = $N
--   unscoped: WHERE state = 'processing' AND locked_until ...
--   ORDER BY locked_until ASC [NULLS FIRST], updated_at ASC, id ASC
--
-- Add a partial index scoped to processing rows so these incident-time
-- queries can seek directly on sync_id for per-sync lookups and still cover
-- the incident ordering path.
CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_processing_lease_ops_idx
    ON deltaflow.deltaflow_sync_jobs (sync_id, locked_until ASC NULLS FIRST, updated_at ASC, id ASC)
    WHERE state = 'processing';

COMMIT;
