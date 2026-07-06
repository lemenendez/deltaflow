package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/lemenendez/deltaflow/pkg/connectors"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type DeltaStore struct {
	connectors.DeltaStoreBase
}

func NewDeltaStore(db *sql.DB, cfg connectors.DeltaStoreConfig) *DeltaStore {
	return &DeltaStore{DeltaStoreBase: connectors.NewDeltaStoreBase(db, cfg)}
}

type queryRowContextExecutor interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Enqueue inserts a delta outside the caller's application transaction.
// Useful for tests, backfills, CLI/admin tools, and non-application writes.
// For durable outbox with application writes, prefer EnqueueInTx. This is the way.
func (s *DeltaStore) Enqueue(ctx context.Context, delta deltaflow.Delta) (*deltaflow.Delta, error) {
	return s.enqueue(ctx, s.DB, delta)
}

// EnqueueInTx inserts a delta using the caller-provided SQL transaction.
// Use this when application writes and outbox insert must commit or roll back together.
func (s *DeltaStore) EnqueueInTx(ctx context.Context, tx *sql.Tx, delta deltaflow.Delta) (*deltaflow.Delta, error) {
	if tx == nil {
		return nil, errors.New("delta store requires transaction")
	}

	return s.enqueue(ctx, tx, delta)
}

func (s *DeltaStore) enqueue(ctx context.Context, exec queryRowContextExecutor, delta deltaflow.Delta) (*deltaflow.Delta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if delta.ID != "" {
		return nil, deltaflow.ErrDeltaIDProvided
	}

	normalized, projectionKeyJSON, metadataJSON, err := s.PrepareDeltaForEnqueue(delta)
	if err != nil {
		return nil, err
	}

	const returning = `
RETURNING
	id::text,
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	dedup_window,
	dedup_key,
	state,
	occurred_at,
	created_at,
	dispatched_at,
	metadata`

	row := exec.QueryRowContext(ctx, `
INSERT INTO deltaflow.deltaflow_deltas (
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	occurred_at,
	created_at,
	metadata,
	dedup_window,
	dedup_key
)
VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9::jsonb, NULLIF($10, ''), NULLIF($11, ''))
ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL
DO NOTHING
`+returning,
		normalized.SyncID,
		normalized.Origin,
		normalized.ProjectionType,
		projectionKeyJSON,
		normalized.ProjectionKeyHash,
		normalized.State,
		normalized.OccurredAt,
		normalized.CreatedAt,
		metadataJSON,
		normalized.DedupWindow,
		normalized.DedupKey,
	)

	inserted, err := s.ScanDelta(row)
	if errors.Is(err, sql.ErrNoRows) && normalized.DedupKey != "" {
		// A duplicate is an idempotent read, not an update. Keep this as a
		// separate statement rather than a CTE: if the INSERT waits for a
		// concurrent transaction to commit, READ COMMITTED gives this SELECT a
		// fresh snapshot that can see the winning row.
		row = exec.QueryRowContext(ctx, `
SELECT
	id::text,
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	dedup_window,
	dedup_key,
	state,
	occurred_at,
	created_at,
	dispatched_at,
	metadata
FROM deltaflow.deltaflow_deltas
WHERE dedup_key = $1`, normalized.DedupKey)
		inserted, err = s.ScanDelta(row)
	}
	if err != nil {
		return nil, err
	}
	return inserted, nil
}

func (s *DeltaStore) EnqueueBatch(ctx context.Context, deltas []deltaflow.Delta) (result *deltaflow.EnqueueBatchResult, err error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	result, err = s.EnqueueBatchTx(ctx, tx, deltas)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// EnqueueBatchTx atomically enqueues a batch using a caller-owned transaction.
// It never commits or rolls back the transaction.
func (s *DeltaStore) EnqueueBatchTx(ctx context.Context, tx *sql.Tx, deltas []deltaflow.Delta) (*deltaflow.EnqueueBatchResult, error) {
	if tx == nil {
		return nil, errors.New("delta store requires transaction")
	}
	window, err := s.ValidateEnqueueBatch(ctx, deltas)
	if err != nil {
		return nil, err
	}
	result := &deltaflow.EnqueueBatchResult{RequestedCount: len(deltas), DedupWindow: window}
	if len(deltas) == 0 {
		return result, nil
	}

	type preparedDelta struct {
		delta         deltaflow.Delta
		key, metadata []byte
	}
	prepared := make([]preparedDelta, len(deltas))
	for i, delta := range deltas {
		if delta.ID != "" {
			return nil, deltaflow.ErrDeltaIDProvided
		}
		normalized, key, metadata, prepErr := s.PrepareDeltaForEnqueue(delta)
		if prepErr != nil {
			return nil, prepErr
		}
		prepared[i] = preparedDelta{normalized, key, metadata}
	}
	for _, item := range prepared {
		res, execErr := tx.ExecContext(ctx, `
INSERT INTO deltaflow.deltaflow_deltas
(sync_id, origin, projection_type, projection_key, projection_key_hash, state, occurred_at, created_at, metadata, dedup_window, dedup_key)
VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9::jsonb, $10, $11)
ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING`, item.delta.SyncID, item.delta.Origin,
			item.delta.ProjectionType, item.key, item.delta.ProjectionKeyHash, item.delta.State,
			item.delta.OccurredAt, item.delta.CreatedAt, item.metadata, item.delta.DedupWindow, item.delta.DedupKey)
		if execErr != nil {
			return nil, execErr
		}
		count, countErr := res.RowsAffected()
		if countErr != nil {
			return nil, countErr
		}
		if count == 1 {
			result.InsertedCount++
		} else {
			result.DuplicateCount++
		}
	}
	return result, nil
}

func (s *DeltaStore) Get(ctx context.Context, deltaID deltaflow.DeltaID) (*deltaflow.Delta, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	row := s.DB.QueryRowContext(ctx, `
SELECT
	id::text,
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	dedup_window,
	dedup_key,
	state,
	occurred_at,
	created_at,
	dispatched_at,
	metadata
FROM deltaflow.deltaflow_deltas
WHERE id = $1::uuid`, deltaID)

	delta, err := s.ScanDelta(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return delta, true, nil
}

func (s *DeltaStore) Pull(ctx context.Context, syncID deltaflow.SyncID, limit int) ([]*deltaflow.Delta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}

	rows, err := s.DB.QueryContext(ctx, `
SELECT
	id::text,
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	dedup_window,
	dedup_key,
	state,
	occurred_at,
	created_at,
	dispatched_at,
	metadata
FROM deltaflow.deltaflow_deltas
WHERE state = 'pending'
	AND sync_id = $2
ORDER BY occurred_at ASC, created_at ASC, id ASC
LIMIT $1`, limit, syncID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pulled := make([]*deltaflow.Delta, 0, limit)
	for rows.Next() {
		delta, err := s.ScanDelta(rows)
		if err != nil {
			return nil, err
		}
		pulled = append(pulled, delta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return pulled, nil
}

func (s *DeltaStore) pullPendingForDispatchTx(ctx context.Context, tx *sql.Tx, syncID deltaflow.SyncID, limit int) ([]dispatchPendingDelta, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT
	id::text,
	sync_id,
	projection_type,
	projection_key,
	projection_key_hash
FROM deltaflow.deltaflow_deltas
WHERE state = 'pending'
	AND sync_id = $2
ORDER BY occurred_at ASC, created_at ASC, id ASC
LIMIT $1
FOR UPDATE SKIP LOCKED`, limit, syncID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pending := make([]dispatchPendingDelta, 0, limit)
	for rows.Next() {
		var item dispatchPendingDelta
		if err := rows.Scan(&item.ID, &item.SyncID, &item.ProjectionType, &item.ProjectionKeyJSON, &item.ProjectionKeyHash); err != nil {
			return nil, err
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return pending, nil
}

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *DeltaStore) markDispatchedTx(ctx context.Context, exec sqlExecutor, deltaID deltaflow.DeltaID, at time.Time) error {
	res, err := exec.ExecContext(ctx, `
UPDATE deltaflow.deltaflow_deltas
SET
	state = CASE WHEN state = 'dispatched' THEN state ELSE 'dispatched' END,
	dispatched_at = CASE WHEN state = 'dispatched' THEN dispatched_at ELSE $2 END
WHERE id = $1::uuid`, deltaID, at.UTC())
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return deltaflow.ErrDeltaNotFound
	}

	return nil
}

func (s *DeltaStore) MarkDispatched(ctx context.Context, deltaID deltaflow.DeltaID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return s.markDispatchedTx(ctx, s.DB, deltaID, time.Now().UTC())
}
