# Plan v0.7.0

Goal: add a real Elasticsearch target path so an application can write through
its REST API, enqueue a Delta in the same source transaction, and let DeltaFlow
reconcile Elasticsearch through a concrete applier.

## Scope

- [x] Add a concrete Elasticsearch `ProjectionApplier` package.
- [x] Support idempotent upsert and delete operations.
- [x] Map `deltaflow.ProjectionOperation` documents to Elasticsearch document IDs and request bodies.
- [x] Classify common Elasticsearch failures as retryable or permanent where the client response makes that practical.
- [x] Add focused unit tests for operation mapping, delete behavior, and error classification.
- [x] Update the current search-oriented playgrounds to use Elasticsearch instead of only simulated search targets.
- [x] Document how the REST write path stays consistent: application write and Delta enqueue in one Postgres transaction, then asynchronous worker apply to Elasticsearch.
- [x] Keep `deltaflow run` out of v0.7.0; CLI runtime wiring remains the v0.8.0 milestone.

## Elasticsearch Applier

Package target:

```text
pkg/connectors/elasticsearch
```

Initial behavior:

- `ProjectionOpUpsert` indexes the projected document by projection identity.
- `ProjectionOpDelete` deletes the indexed document by projection identity.
- Re-applying the same upsert is safe.
- Re-applying a delete for a missing document is safe.
- The applier accepts index configuration explicitly from Go code.
- The applier does not infer application projectors from YAML.

The applier should be usable directly by applications and playgrounds:

```go
applier, err := elasticsearch.NewApplier(elasticsearch.ApplierConfig{
    Client:   client,
    Endpoint: "http://localhost:9200",
    Index:    "products",
})
if err != nil {
    log.Fatal(err)
}
```

## Playground Updates

Update existing playgrounds before adding new ones:

- `playground/03-postgres-e-commerce`: use Elasticsearch in docker compose, with simulator fallback for local runs without `DELTAFLOW_ES_ENDPOINT`.
- `playground/04-postgres-crm`: keep Redis-style simulated read views, but use Elasticsearch for the search/order projection path.

Each updated playground should show:

- source data written to Postgres
- Delta enqueued transactionally with the source write
- DeltaFlow worker claiming and applying jobs
- Elasticsearch index receiving upserts and deletes
- retry and dead-letter behavior still visible when Elasticsearch rejects or cannot accept an operation

## Acceptance Criteria

- [x] The Elasticsearch applier passes unit tests without requiring a live Elasticsearch service.
- [x] Both search-oriented playgrounds can run with Postgres plus Elasticsearch through docker compose.
- [x] The playgrounds verify indexed documents after worker drain, not only worker counters.
- [x] Ghost deletion removes the Elasticsearch document or treats a missing Elasticsearch document as successful.
- [x] README/playground docs explain the REST/API consistency model in concrete terms.
- [x] `docs/ROADMAP.md` still keeps CLI `run` and runtime registry decisions in v0.8.0.

## Out of Scope

- `deltaflow run`.
- YAML-to-projector or YAML-to-applier runtime instantiation.
- Connector registry/plugin model.
- Worker batching and throughput changes.
- Redis/Postgres target appliers.
- Applier-level metrics/logging, Prometheus examples, and Grafana dashboards; these remain part of the v0.11 observability milestone.
