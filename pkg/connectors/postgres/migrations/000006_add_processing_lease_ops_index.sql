BEGIN;

-- Operational lease helpers scan processing jobs ordered by lease timing:
--
--   WHERE state = 'processing'
--   AND locked_until ...
--   AND ($N::text = '' OR sync_id = $N)
--   ORDER BY locked_until ASC [NULLS FIRST], updated_at ASC, id ASC
--
-- Add a partial index scoped to processing rows so these incident-time
-- queries avoid broad seq scans + sort on large tables.
CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_processing_lease_ops_idx
    ON deltaflow.deltaflow_sync_jobs (locked_until ASC NULLS FIRST, updated_at ASC, id ASC, sync_id)
    WHERE state = 'processing';

COMMIT;
