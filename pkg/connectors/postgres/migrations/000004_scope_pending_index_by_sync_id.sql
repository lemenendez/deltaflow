BEGIN;

-- Pull and dispatch queries now filter by both state and sync_id:
--
--   WHERE state = 'pending' AND sync_id = $N
--   ORDER BY occurred_at ASC, created_at ASC, id ASC
--
-- The old index (state, occurred_at, created_at, id) can satisfy the state
-- equality and the ordering, but must scan all pending rows across every sync
-- to apply the sync_id filter.  Replacing it with
-- (state, sync_id, occurred_at, created_at, id) lets Postgres perform an
-- index seek directly to the (state, sync_id) pair and return rows in order
-- without an extra sort pass.

DROP INDEX IF EXISTS deltaflow.deltaflow_deltas_pending_order_idx;

CREATE INDEX IF NOT EXISTS deltaflow_deltas_pending_sync_order_idx
    ON deltaflow.deltaflow_deltas (state, sync_id, occurred_at, created_at, id);

COMMIT;
