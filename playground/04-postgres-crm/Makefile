DC ?= docker compose
PG_SERVICE ?= postgres
SYNC_ID ?= playground-04-crm-to-redis-opensearch
LOG_LINES ?= 100
SIM_SEED ?= 4004
USER_COUNT ?= 8
CUSTOMER_COUNT ?= 18
ORDER_COUNT ?= 22
MUTATION_COUNT ?= 64
WRITER_COUNT ?= 4
WORKER_COUNT ?= 2
MAX_ATTEMPTS ?= 3
PSQL = $(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1

export SIM_SEED USER_COUNT CUSTOMER_COUNT ORDER_COUNT MUTATION_COUNT WRITER_COUNT WORKER_COUNT MAX_ATTEMPTS DELTAFLOW_WORKER_LOG

.PHONY: up down pgcompat migrate run reset logs counts deltas pending-deltas jobs jobs-by-state dead-jobs worker-log worker-log-beautify beautify

up:
	$(DC) up -d --wait $(PG_SERVICE) elasticsearch

down:
	$(DC) down --volumes --remove-orphans

pgcompat: up
	$(PSQL) -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
	$(PSQL) -c 'CREATE OR REPLACE FUNCTION uuidv7() RETURNS uuid LANGUAGE SQL AS $$$$ SELECT gen_random_uuid(); $$$$;'

migrate: pgcompat
	$(PSQL) -f /workspace/pkg/connectors/postgres/migrations/000001_create_deltaflow_deltas.sql
	$(PSQL) -f /workspace/pkg/connectors/postgres/migrations/000002_create_deltaflow_sync_jobs.sql
	$(PSQL) -f /workspace/pkg/connectors/postgres/migrations/000003_align_outbox_unique_index.sql
	$(PSQL) -f /workspace/pkg/connectors/postgres/migrations/000004_scope_pending_index_by_sync_id.sql
	$(PSQL) -f /workspace/pkg/connectors/postgres/migrations/000005_scope_claim_index_by_sync_id.sql
	$(PSQL) -f /workspace/pkg/connectors/postgres/migrations/000006_add_processing_lease_ops_index.sql

run: migrate
	$(DC) run --rm playground

reset: up
	$(PSQL) -c "DROP SCHEMA IF EXISTS deltaflow CASCADE;"
	$(MAKE) migrate

logs:
	$(DC) logs -f $(PG_SERVICE) elasticsearch

counts: up
	$(PSQL) -c "SELECT 'deltas' AS table_name, state, COUNT(*) AS count FROM deltaflow.deltaflow_deltas WHERE sync_id = '$(SYNC_ID)' GROUP BY state UNION ALL SELECT 'jobs' AS table_name, state, COUNT(*) AS count FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = '$(SYNC_ID)' GROUP BY state ORDER BY table_name, state;"

deltas: up
	$(PSQL) -c "SELECT id::text, state, origin, projection_type, projection_key, metadata, occurred_at, created_at, dispatched_at FROM deltaflow.deltaflow_deltas WHERE sync_id = '$(SYNC_ID)' ORDER BY occurred_at ASC, created_at ASC, id ASC LIMIT 50;"

pending-deltas: up
	$(PSQL) -c "SELECT id::text, origin, projection_type, projection_key, metadata, occurred_at, created_at FROM deltaflow.deltaflow_deltas WHERE sync_id = '$(SYNC_ID)' AND state = 'pending' ORDER BY occurred_at ASC, created_at ASC, id ASC LIMIT 50;"

jobs-by-state: up
	$(PSQL) -c "SELECT state, COUNT(*) AS count, COUNT(*) FILTER (WHERE ghost_detected) AS ghosts, MIN(created_at) AS first_created_at, MAX(updated_at) AS last_updated_at FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = '$(SYNC_ID)' GROUP BY state ORDER BY state;"

jobs: up
	$(PSQL) -c "SELECT id::text, state, attempt_count, max_attempts, projection_type, projection_key, locked_by, locked_until, ghost_detected, last_error, created_at, updated_at FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = '$(SYNC_ID)' ORDER BY created_at ASC, id ASC LIMIT 50;"

dead-jobs: up
	$(PSQL) -c "SELECT id::text, attempt_count, max_attempts, projection_type, projection_key, last_error, dead_at FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = '$(SYNC_ID)' AND state = 'dead' ORDER BY dead_at ASC, id ASC;"

worker-log:
	@if [ -f logs/deltaflow-worker.log ]; then tail -n $(LOG_LINES) logs/deltaflow-worker.log; else echo "logs/deltaflow-worker.log does not exist yet. Run make run first."; fi

worker-log-beautify:
	@if [ ! -f logs/deltaflow-worker.log ]; then echo "logs/deltaflow-worker.log does not exist yet. Run make run first."; elif command -v jq >/dev/null 2>&1; then tail -n $(LOG_LINES) logs/deltaflow-worker.log | jq .; else echo "jq is required to beautify JSON logs. Install jq or use make worker-log."; fi

beautify: worker-log-beautify
