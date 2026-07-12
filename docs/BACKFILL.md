# Backfill Guide

This guide describes the intended DeltaFlow backfill model for v0.12.0 and later.

Backfill is not a DeltaFlow-managed runtime.
Backfill is an application-owned source scan that uses DeltaFlow's durable enqueue guardrails.

The core rule is:

```text
DeltaFlow returns enqueue results.
The caller owns the source checkpoint.
```

## What DeltaFlow Owns

- Idempotent batch enqueue through `EnqueueBatch`
- Deduplication within an application-chosen `DedupWindow`
- Durable delta creation for the source records you decide to scan
- Normal worker lease, retry, and apply behavior after deltas are queued

## What the Application Owns

- Source enumeration
- Stable scan ordering
- Cursor or checkpoint persistence
- Batch sizing and pacing
- Dry-run behavior
- Pause/resume orchestration
- Scheduling and operator controls

## Recommended Mental Model

Use backfill when you need to populate or repair a destination for an existing source universe.

Examples:

- populate a new Elasticsearch index from existing rows
- rehydrate a cache destination after data loss
- repair a subset of projections after an operator mistake
- rescan a tenant or date range after destination schema changes

The intended flow is:

```text
scan stable source slice -> build backfill deltas -> EnqueueBatch -> workers apply -> persist caller checkpoint
```

## Stable Ordering Requirement

Restart-safe backfills require application-provided stable ordering.

Good scan keys include:

- monotonically increasing primary key
- `(updated_at, id)` composite ordering
- rowversion or log sequence number where available
- `(tenant_id, id)` for per-tenant parallel lanes

Avoid unstable pagination such as offset-based scans over mutating datasets when restart safety matters.

## Dedup Windows

Choose a `DedupWindow` that names the logical scan lane being retried.

Examples:

- `customers-full-2026-07`
- `tenant-42-users`
- `orders-2026-q1`
- `catalog-year-2024`

Inside one dedup window, re-enqueuing the same projection identity should collapse into one durable delta.
Across different dedup windows, the same projection may be intentionally re-enqueued.

## Checkpoint Strategy

Store your source checkpoint outside DeltaFlow.

Typical checkpoints:

- last scanned primary key
- last `(updated_at, id)` tuple
- per-tenant cursor
- source-system high watermark token

Persist the checkpoint only after a batch enqueue succeeds.
If the process crashes after enqueue and before checkpoint persistence, rerun the same source slice with the same dedup window so duplicates are suppressed.

DeltaFlow does not own your source cursor, checkpoint table, scheduler, or pause/resume state.
If you want restart-safe enumeration, your application must persist enough progress to resume the same stable lane.

## Checkpoint Table Example

One practical shape is a caller-owned checkpoint table keyed by sync and lane.

```sql
CREATE TABLE backfill_checkpoints (
	sync_id text NOT NULL,
	lane text NOT NULL,
	last_updated_at timestamptz,
	last_id text,
	high_watermark_updated_at timestamptz,
	high_watermark_id text,
	checkpoint_token text,
	updated_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (sync_id, lane)
);
```

Suggested meaning:

- `sync_id`: the DeltaFlow sync being populated
- `lane`: one logical producer lane such as `tenant-42` or `customers-full-2026-07`
- `last_updated_at` and `last_id`: the last committed seek cursor
- `high_watermark_updated_at` and `high_watermark_id`: the upper bound captured for the current pass
- `checkpoint_token`: an optional opaque application token if the source system exposes one

The checkpoint token is caller-defined.
It might be a rowversion, change-stream token, source snapshot identifier, or any other source-owned resume marker.
DeltaFlow does not interpret it.

## Basic Workflow

1. Choose a stable ordering and checkpoint format.
2. Choose a dedup window naming strategy.
3. Read the next source slice.
4. Convert each source record into a Delta intended for the destination sync.
5. Call `EnqueueBatch`.
6. If enqueue succeeds, persist the caller-owned checkpoint.
7. Repeat until the source universe is exhausted.

## Building Backfill Deltas

Use `NewBackfillDelta` to build a ready-to-enqueue Delta with the correct sync, identity, and dedup window fields already populated.

```go
identity := deltaflow.ProjectionIdentity{
	Type: "Customer",
	Key: deltaflow.ProjectionKey{
		"customer_id": json.RawMessage(`"42"`),
	},
}

delta, err := deltaflow.NewBackfillDelta(
	"customers-to-elasticsearch",
	deltaflow.OriginOperationUpdated,
	identity,
	"customers-full-2026-07",
)
if err != nil {
	return err
}
```

The helper does not enqueue anything and does not set timestamps, IDs, dedup keys, or hashes.
Those remain store-owned concerns at enqueue time.

`OriginOperationType` should still describe the source-side business change semantics your application wants to record.
Backfill itself is represented by the dedup window and by the operational workflow around enqueue, not by a special Delta origin value.

## Seek Pagination Pattern

Prefer seek pagination over offset pagination for large scans.

Example shape:

```sql
SELECT id, updated_at, payload
FROM customers
WHERE (updated_at, id) > ($1, $2)
ORDER BY updated_at, id
LIMIT $3;
```

The caller keeps the last seen `(updated_at, id)` tuple as the checkpoint.
On restart, rerun from that tuple using the same dedup window policy for the affected lane.

## Restart-Safe Recovery Example

The pattern below shows one practical recovery loop using:

- stable ordering by `(updated_at, id)`
- one caller-owned lane checkpoint row
- one dedup window for the lane
- one captured high watermark per pass

```go
type BackfillCheckpoint struct {
	LastUpdatedAt          time.Time
	LastID                 string
	HighWatermarkUpdatedAt time.Time
	HighWatermarkID        string
	CheckpointToken        string
}

func runCustomerBackfill(ctx context.Context, db *sql.DB, deltaStore deltaflow.DeltaStore) error {
	const (
		syncID      = deltaflow.SyncID("customers-to-elasticsearch")
		lane        = "customers-full"
		dedupWindow = deltaflow.DedupWindow("customers-full-2026-07")
		batchSize   = 500
	)

	checkpoint, err := loadCheckpoint(ctx, db, syncID, lane)
	if err != nil {
		return err
	}
	if checkpoint.HighWatermarkUpdatedAt.IsZero() {
		checkpoint.HighWatermarkUpdatedAt, checkpoint.HighWatermarkID, err = loadHighWatermark(ctx, db)
		if err != nil {
			return err
		}
		if err := saveHighWatermark(ctx, db, syncID, lane, checkpoint); err != nil {
			return err
		}
	}

	for {
		rows, err := loadCustomerSlice(
			ctx,
			db,
			checkpoint.LastUpdatedAt,
			checkpoint.LastID,
			checkpoint.HighWatermarkUpdatedAt,
			checkpoint.HighWatermarkID,
			batchSize,
		)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return markLaneComplete(ctx, db, syncID, lane)
		}

		deltas := make([]deltaflow.Delta, 0, len(rows))
		for _, row := range rows {
			delta, err := deltaflow.NewBackfillDelta(
				syncID,
				deltaflow.OriginOperationUpdated,
				deltaflow.ProjectionIdentity{
					Type: "Customer",
					Key: deltaflow.ProjectionKey{
						"customer_id": json.RawMessage(strconv.Quote(row.ID)),
					},
				},
				dedupWindow,
			)
			if err != nil {
				return err
			}
			deltas = append(deltas, delta)
		}

		if _, err := deltaStore.EnqueueBatch(ctx, deltas); err != nil {
			return err
		}

		last := rows[len(rows)-1]
		checkpoint.LastUpdatedAt = last.UpdatedAt
		checkpoint.LastID = last.ID
		if err := saveCheckpoint(ctx, db, syncID, lane, checkpoint); err != nil {
			return err
		}
	}
}
```

Crash behavior:

- crash before `EnqueueBatch`: no checkpoint moves, so the same source slice is scanned again
- crash after `EnqueueBatch` but before `saveCheckpoint`: the same source slice is retried with the same dedup window, so duplicates are suppressed
- crash after `saveCheckpoint`: the next run resumes strictly after the last committed source row

The important boundary is that checkpoint persistence happens after a successful enqueue batch.
That is what turns the dedup window into a restart-safe retry guardrail instead of a best-effort replay hint.

## High-Watermark Pattern

For mutating source tables, many teams first capture a high watermark and then scan up to that boundary.

Example approach:

1. Read `max(updated_at, id)` or an equivalent source watermark.
2. Scan forward from the saved checkpoint up to that captured boundary.
3. Enqueue in batches.
4. Persist the final checkpoint after each successful enqueue.
5. Start a new pass for the next captured boundary if needed.

This avoids a moving target while still allowing the destination to catch up incrementally.

When the source system already exposes an opaque resume token, store that token alongside the seek cursor.
The seek cursor remains useful for stable ordering, while the token can record the source-specific snapshot or change-feed position that bounded the pass.

## Dry Run

Dry run is usually application-owned.

Practical dry-run behaviors:

- log the records that would become deltas
- validate stable ordering and checkpoint movement
- count source rows per planned dedup window
- estimate queue growth before enabling enqueue

DeltaFlow does not currently provide a separate dry-run enqueue API.
The practical v0.12.0 support is guidance: build the same source scan and delta construction path, then stop before `EnqueueBatch`.

## Throughput Guidance

Large backfills can enqueue faster than workers can apply.

Tune deliberately:

- batch size: start modestly and increase only after measuring queue depth and destination latency
- pacing: insert sleeps or rate limiting between batches when pending jobs grow too quickly
- worker count: increase sync workers for Postgres-backed deployments when the destination can absorb more throughput
- destination bulk APIs: use connector-specific bulk apply capabilities where available
- retention and pruning: plan how long queued and completed delta/job history should be kept during large runs

## Store Constraints

Postgres:

- suitable for multi-worker backfill processing using the normal lease and batch semantics
- use multiple producer lanes only when each lane has stable ordering and a distinct checkpoint strategy
- backfill workers can safely drain jobs concurrently because the normal job lease uses `ClaimNext` and `ClaimNextBatch` semantics with `FOR UPDATE SKIP LOCKED`
- if one lane is too large for a single worker, split the source scan into stable lanes and let multiple workers process separate lanes in parallel

SQLite:

- keep backfills single-worker only
- the SQLite worker lock is a singleton lease, so a second worker on the same database should fail to acquire the lock rather than run concurrently
- do not treat SQLite as a distributed or competing-worker backfill store

## Timing Mode Compatibility

Backfill guidance must stay aligned with the active projection timing mode.

- latest_state: best current fit, because the source scanner can enqueue the current projection state for each record
- future timing modes: must document who computes projection state during backfill, what checkpoint semantics apply, and whether replay is deterministic

## External Playground Contract

The backfill playground should live in a separate repository, not in the core DeltaFlow repo.
The first target repository is [lemenendez/deltaflow-playground-crm](https://github.com/lemenendez/deltaflow-playground-crm/).

Minimum contract for that external playground:

- seed a realistic source dataset with a fixed deterministic seed
- run a backfill producer that uses stable ordering and caller-owned checkpoints
- enqueue through `NewBackfillDelta` and `EnqueueBatch`
- drain through normal worker behavior rather than a special backfill runtime
- show destination catch-up for at least one practical target such as Elasticsearch or a CRM read model
- include a larger source-count scenario for throughput and pacing experiments
- pin DeltaFlow and connector versions explicitly
- document which pieces are example-only application code versus reusable DeltaFlow APIs

The core repository should only document this contract and link to the external repository.
It should not reintroduce bundled playground applications for backfill.

## Destination Population Example

The most common adoption path is populating a new destination such as Elasticsearch.

Typical shape:

1. Application scans source rows with stable seek pagination.
2. Application builds backfill deltas for the Elasticsearch sync.
3. Application enqueues via `EnqueueBatch`.
4. Normal workers drain the queue and write projections to Elasticsearch.
5. Application tracks the source checkpoint until the initial population completes.

The producer should not call Elasticsearch directly during the backfill.
It should enqueue the same projection identities the normal sync uses and let the existing worker/projector/applier path populate the index.

## Future Additions

This guide is expected to grow with:

- store-specific tuning notes
- connector-specific destination bulk guidance
