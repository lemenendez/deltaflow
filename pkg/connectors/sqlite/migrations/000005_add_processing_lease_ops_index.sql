BEGIN;

CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_processing_lease_ops_idx
    ON deltaflow_sync_jobs (sync_id, locked_until_micros, updated_at_micros, id)
    WHERE state = 'processing';

CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_processing_lease_unscoped_ops_idx
    ON deltaflow_sync_jobs (locked_until_micros, updated_at_micros, id)
    WHERE state = 'processing';

COMMIT;
