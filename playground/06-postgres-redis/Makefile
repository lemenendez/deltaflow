DC ?= docker compose
PG_SERVICE ?= postgres
REDIS_SERVICE ?= redis
REDIS_IMAGE ?= redis:8.0
DELTAFLOW_REDIS_TTL ?= 5m
PSQL = $(DC) exec -T $(PG_SERVICE) psql -U deltaflow -d deltaflow -v ON_ERROR_STOP=1

export REDIS_IMAGE DELTAFLOW_REDIS_TTL

.PHONY: up down pgcompat migrate run run-valkey reset redis-value redis-ttl jobs

up:
	$(DC) up -d --wait $(PG_SERVICE) $(REDIS_SERVICE)

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

run-valkey:
	$(MAKE) run REDIS_IMAGE=valkey/valkey:9.1.0

reset: down run

redis-value: up
	$(DC) exec -T $(REDIS_SERVICE) sh -c '(redis-cli --raw GET contacts:con-001 || valkey-cli --raw GET contacts:con-001)'

redis-ttl: up
	$(DC) exec -T $(REDIS_SERVICE) sh -c '(redis-cli TTL contacts:con-001 || valkey-cli TTL contacts:con-001)'

jobs: up
	$(PSQL) -c "SELECT state, attempt_count, projection_type, projection_key, ghost_detected, last_error FROM deltaflow.deltaflow_sync_jobs WHERE sync_id = 'playground-06-contacts-to-redis' ORDER BY created_at, id;"
