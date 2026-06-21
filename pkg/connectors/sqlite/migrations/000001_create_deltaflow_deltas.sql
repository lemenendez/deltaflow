BEGIN;

CREATE TABLE IF NOT EXISTS deltaflow_deltas (
    id TEXT PRIMARY KEY,
    sync_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    projection_type TEXT NOT NULL,
    projection_key TEXT NOT NULL,
    projection_key_hash TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    occurred_at_micros INTEGER NOT NULL,
    created_at_micros INTEGER NOT NULL,
    dispatched_at_micros INTEGER,
    metadata TEXT,
    CHECK (state IN ('pending', 'dispatched', 'ignored'))
);

CREATE INDEX IF NOT EXISTS deltaflow_deltas_projection_idx
    ON deltaflow_deltas (projection_type, projection_key_hash);

COMMIT;
