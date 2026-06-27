BEGIN;

CREATE INDEX IF NOT EXISTS deltaflow_deltas_pending_sync_order_idx
    ON deltaflow_deltas (state, sync_id, occurred_at_micros, created_at_micros, id);

COMMIT;
