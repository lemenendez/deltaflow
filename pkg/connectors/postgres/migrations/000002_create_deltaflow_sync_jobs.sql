BEGIN;

CREATE SCHEMA IF NOT EXISTS deltaflow;

CREATE TABLE IF NOT EXISTS deltaflow.deltaflow_sync_jobs (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    sync_id TEXT NOT NULL,
    delta_id UUID,
    origin TEXT NOT NULL,
    projection_type TEXT NOT NULL,
    projection_key JSONB NOT NULL,
    projection_key_hash TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    last_error TEXT,
    last_error_code TEXT,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    ghost_detected BOOLEAN NOT NULL DEFAULT FALSE,
    synced_at TIMESTAMPTZ,
    dead_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT deltaflow_sync_jobs_delta_fk
        FOREIGN KEY (delta_id)
        REFERENCES deltaflow.deltaflow_deltas (id)
        ON DELETE SET NULL,
    CONSTRAINT deltaflow_sync_jobs_state_check
        CHECK (state IN ('pending', 'processing', 'synced', 'retrying', 'dead')),
    CONSTRAINT deltaflow_sync_jobs_origin_check
        CHECK (origin IN ('backfill', 'replay', 'manual', 'outbox', 'unknown')),
    CONSTRAINT deltaflow_sync_jobs_attempts_check
        CHECK (attempt_count >= 0 AND max_attempts > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS deltaflow_sync_jobs_outbox_delta_unique
    ON deltaflow.deltaflow_sync_jobs (delta_id)
    WHERE origin = 'outbox' AND delta_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_claim_idx
    ON deltaflow.deltaflow_sync_jobs (state, available_at, created_at, id);

CREATE INDEX IF NOT EXISTS deltaflow_sync_jobs_projection_idx
    ON deltaflow.deltaflow_sync_jobs (projection_type, projection_key_hash);

COMMIT;
