# v0.7.0

DeltaFlow v0.7.0 adds the first concrete target applier: Elasticsearch.
This release turns the latest-state worker path into an end-to-end search sync
example: application data changes in Postgres, a Delta is enqueued in the same
transaction, workers reconcile the latest projection, and Elasticsearch receives
idempotent upsert/delete operations.

## Highlights

- Added `pkg/connectors/elasticsearch`, a concrete `deltaflow.ProjectionApplier`.
- Supports `ProjectionOpUpsert` through Elasticsearch document indexing.
- Supports `ProjectionOpDelete`, treating missing Elasticsearch documents as a successful idempotent delete.
- Keeps document ID mapping explicit and configurable from Go code.
- Classifies Elasticsearch response failures with status code and retryability metadata.
- Updated the product search and CRM playgrounds to use Elasticsearch in docker compose.
- Kept `deltaflow run` out of this release; CLI runtime wiring remains v0.8.0.

## Elasticsearch Applier

The new applier is constructed explicitly by application code:

```go
applier, err := elasticsearch.NewApplier(elasticsearch.ApplierConfig{
    Endpoint: "http://localhost:9200",
    Index:    "products",
})
```

Applications can provide a custom HTTP `Client` and `DocumentID` function when
projection keys should map to readable target IDs, for example a product SKU or
CRM entity ID. If no document ID function is provided, DeltaFlow uses a stable
hash derived from projection type and projection key.

## Playground Updates

- `playground/03-postgres-e-commerce`
  - Starts Postgres and Elasticsearch with docker compose.
  - Writes product source rows and deltas transactionally through `DeltaStore.EnqueueInTx`.
  - Applies product search documents to Elasticsearch.
  - Verifies indexed documents after worker drain.
  - Keeps retry, dead-letter, and ghost delete behavior visible.

- `playground/04-postgres-crm`
  - Starts Postgres and Elasticsearch with docker compose.
  - Keeps Redis-style read views simulated locally.
  - Applies CRM search fanout documents to Elasticsearch.
  - Verifies indexed search documents after worker drain.
  - Keeps retry, dead-letter, and ghost delete behavior visible.

Both playgrounds still fall back to the simulator when `DELTAFLOW_ES_ENDPOINT`
is unset, so local `go run .` remains lightweight.

## Verification

- `go test ./...`
- `go test ./...` in `playground/03-postgres-e-commerce`
- `go test ./...` in `playground/04-postgres-crm`
- `make run DC=docker-compose` in `playground/03-postgres-e-commerce`
- `make run DC=docker-compose` in `playground/04-postgres-crm`

## Deferred

- `deltaflow run` and YAML/runtime wiring model remain v0.8.0.
- Worker batching and throughput controls remain v0.9.0.
- Redis/Postgres target appliers remain v0.10.0.
- Applier-level metrics/logging, Prometheus examples, Grafana dashboards, and trace/debug commands remain v0.11.0.
