BEGIN;

ALTER TABLE deltaflow.deltaflow_deltas
    ADD COLUMN IF NOT EXISTS dedup_window TEXT,
    ADD COLUMN IF NOT EXISTS dedup_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS deltaflow_deltas_dedup_key_uidx
    ON deltaflow.deltaflow_deltas (dedup_key)
    WHERE dedup_key IS NOT NULL;

COMMIT;
