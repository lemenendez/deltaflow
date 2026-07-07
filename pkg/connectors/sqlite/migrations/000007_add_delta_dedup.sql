BEGIN;

ALTER TABLE deltaflow_deltas ADD COLUMN dedup_window TEXT;
ALTER TABLE deltaflow_deltas ADD COLUMN dedup_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS deltaflow_deltas_dedup_key_uidx
    ON deltaflow_deltas (dedup_key)
    WHERE dedup_key IS NOT NULL;

COMMIT;
