BEGIN;

-- Operational lease helpers use separate scoped and unscoped query shapes:
--
--   scoped:   WHERE state = 'processing' AND locked_until ... AND sync_id = $N
--   unscoped: WHERE state = 'processing' AND locked_until ...
--   ORDER BY locked_until ASC [NULLS FIRST], updated_at ASC, id ASC
--
-- Keep two partial indexes scoped to processing rows:
--
-- 1) sync-first for per-sync incident lookups.
-- 2) locked_until-first for global unscoped lease ordering.
CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_processing_lease_ops_idx
    ON deltaflow.deltaflow_sync_jobs (sync_id, locked_until ASC NULLS FIRST, updated_at ASC, id ASC)
    WHERE state = 'processing';

CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_processing_lease_unscoped_ops_idx
    ON deltaflow.deltaflow_sync_jobs (locked_until ASC NULLS FIRST, updated_at ASC, id ASC)
    WHERE state = 'processing';

COMMIT;
