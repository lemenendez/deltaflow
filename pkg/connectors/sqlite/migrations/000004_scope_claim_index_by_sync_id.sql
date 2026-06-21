BEGIN;

CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_claim_idx
    ON deltaflow_sync_jobs (sync_id, state, available_at_micros, created_at_micros, id);

COMMIT;
