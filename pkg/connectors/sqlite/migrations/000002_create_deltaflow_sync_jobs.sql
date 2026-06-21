BEGIN;

CREATE TABLE IF NOT EXISTS deltaflow_sync_jobs (
    id TEXT PRIMARY KEY,
    sync_id TEXT NOT NULL,
    delta_id TEXT,
    origin TEXT NOT NULL,
    projection_type TEXT NOT NULL,
    projection_key TEXT NOT NULL,
    projection_key_hash TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    last_error TEXT,
    last_error_code TEXT,
    available_at_micros INTEGER NOT NULL,
    locked_by TEXT,
    locked_until_micros INTEGER,
    ghost_detected INTEGER NOT NULL DEFAULT 0,
    synced_at_micros INTEGER,
    dead_at_micros INTEGER,
    created_at_micros INTEGER NOT NULL,
    updated_at_micros INTEGER NOT NULL,
    FOREIGN KEY (delta_id) REFERENCES deltaflow_deltas (id) ON DELETE SET NULL,
    CHECK (state IN ('pending', 'processing', 'synced', 'retrying', 'dead')),
    CHECK (origin IN ('backfill', 'replay', 'manual', 'outbox', 'unknown')),
    CHECK (attempt_count >= 0 AND max_attempts > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS deltaflow_sync_jobs_outbox_delta_unique
    ON deltaflow_sync_jobs (delta_id)
    WHERE origin = 'outbox';

CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_projection_idx
    ON deltaflow_sync_jobs (projection_type, projection_key_hash);

COMMIT;
