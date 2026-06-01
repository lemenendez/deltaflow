BEGIN;

CREATE SCHEMA IF NOT EXISTS deltaflow;

CREATE TABLE IF NOT EXISTS deltaflow.deltaflow_deltas (
    id UUID PRIMARY KEY DEFAULT uuidv7(),
    sync_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    projection_type TEXT NOT NULL,
    projection_key JSONB NOT NULL,
    projection_key_hash TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at TIMESTAMPTZ,
    metadata JSONB,
    CONSTRAINT deltaflow_deltas_state_check CHECK (state IN ('pending', 'dispatched', 'ignored'))
);

CREATE INDEX IF NOT EXISTS deltaflow_deltas_pending_order_idx
    ON deltaflow.deltaflow_deltas (state, occurred_at, created_at, id);

CREATE INDEX IF NOT EXISTS deltaflow_deltas_projection_idx
    ON deltaflow.deltaflow_deltas (projection_type, projection_key_hash);

COMMIT;
