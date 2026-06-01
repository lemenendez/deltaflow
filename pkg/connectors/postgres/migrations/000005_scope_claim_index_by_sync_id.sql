BEGIN;

-- ClaimNext filters by sync_id before state/available_at:
--
--   WHERE sync_id = $N
--   AND (
--       (state IN ('pending', 'retrying') AND available_at <= $1)
--       OR (state = 'processing' AND (locked_until IS NULL OR locked_until <= $1))
--   )
--   ORDER BY available_at ASC, created_at ASC, id ASC
--
-- The old index (state, available_at, created_at, id) cannot seek to a
-- specific sync_id; every claim scans all matching state rows across every
-- sync before applying the sync_id filter.  Replacing it with
-- (sync_id, state, available_at, created_at, id) lets Postgres seek directly
-- to the (sync_id) prefix, filter by state and available_at within that
-- partition, and return rows already in order without an extra sort pass.

DROP INDEX IF EXISTS deltaflow.deltaflow_sync_jobs_claim_idx;

CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_claim_idx
    ON deltaflow.deltaflow_sync_jobs (sync_id, state, available_at, created_at, id);

COMMIT;
