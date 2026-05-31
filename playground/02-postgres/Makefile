DC ?= docker compose
PG_SERVICE ?= postgres

.PHONY: up down pgcompat migrate run reset logs

up:
	$(DC) up -d --wait $(PG_SERVICE)

down:
	$(DC) down --volumes --remove-orphans

pgcompat: up
	$(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
	$(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1 -c 'CREATE OR REPLACE FUNCTION uuidv7() RETURNS uuid LANGUAGE SQL AS $$$$ SELECT gen_random_uuid(); $$$$;'

migrate: pgcompat
	$(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1 -f /workspace/pkg/connectors/postgres/migrations/000001_create_deltaflow_deltas.sql
	$(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1 -f /workspace/pkg/connectors/postgres/migrations/000002_create_deltaflow_sync_jobs.sql
	$(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1 -f /workspace/pkg/connectors/postgres/migrations/000003_align_outbox_unique_index.sql
	$(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1 -f /workspace/pkg/connectors/postgres/migrations/000004_scope_pending_index_by_sync_id.sql
	$(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1 -f /workspace/pkg/connectors/postgres/migrations/000005_scope_claim_index_by_sync_id.sql

run: migrate
	$(DC) run --rm playground

reset: up
	$(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS deltaflow CASCADE;"
	$(MAKE) migrate

logs:
	$(DC) logs -f $(PG_SERVICE)
