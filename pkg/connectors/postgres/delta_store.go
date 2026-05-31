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

func (s *DeltaStore) Enqueue(ctx context.Context, delta deltaflow.Delta) (*deltaflow.Delta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	normalized, projectionKeyJSON, metadataJSON, err := s.PrepareDeltaForEnqueue(delta)
	if err != nil {
		return nil, err
	}

	// Keep compatibility with callers that still pass IDs, but durable Postgres
	// storage always uses DB-generated UUIDv7 values for clustered PK locality.
	normalized.ID = ""

	const returning = `
RETURNING
	id::text,
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	occurred_at,
	created_at,
	dispatched_at,
	metadata`

	row := s.DB.QueryRowContext(ctx, `
INSERT INTO deltaflow.deltaflow_deltas (
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	occurred_at,
	created_at,
	metadata
)
VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9::jsonb)
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
	)

	inserted, err := s.ScanDelta(row)
	if err != nil {
		if connectors.IsUniqueViolation(err) {
			return nil, deltaflow.ErrDeltaAlreadyExists
		}
		return nil, err
	}
	return inserted, nil
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
