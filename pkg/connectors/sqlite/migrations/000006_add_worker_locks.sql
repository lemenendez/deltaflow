BEGIN;

CREATE TABLE IF NOT EXISTS deltaflow_worker_locks (
    lock_name TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    acquired_at_micros INTEGER NOT NULL,
    expires_at_micros INTEGER NOT NULL
);

COMMIT;