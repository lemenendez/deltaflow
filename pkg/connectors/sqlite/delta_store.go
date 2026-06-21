package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
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

type execContextExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type queryRowContextExecutor interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *DeltaStore) Enqueue(ctx context.Context, delta deltaflow.Delta) (*deltaflow.Delta, error) {
	return s.enqueue(ctx, s.DB, s.DB, delta)
}

func (s *DeltaStore) EnqueueInTx(ctx context.Context, tx *sql.Tx, delta deltaflow.Delta) (*deltaflow.Delta, error) {
	if tx == nil {
		return nil, errors.New("delta store requires transaction")
	}
	return s.enqueue(ctx, tx, tx, delta)
}

func (s *DeltaStore) enqueue(ctx context.Context, exec execContextExecutor, queryer queryRowContextExecutor, delta deltaflow.Delta) (*deltaflow.Delta, error) {
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
	id, err := newID("delta", normalized.CreatedAt)
	if err != nil {
		return nil, err
	}

	_, err = exec.ExecContext(ctx, `
INSERT INTO deltaflow_deltas (
	id,
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	occurred_at_micros,
	created_at_micros,
	metadata
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		normalized.SyncID,
		normalized.Origin,
		normalized.ProjectionType,
		string(projectionKeyJSON),
		normalized.ProjectionKeyHash,
		normalized.State,
		microsFromTime(normalized.OccurredAt),
		microsFromTime(normalized.CreatedAt),
		string(metadataJSON),
	)
	if err != nil {
		return nil, err
	}

	inserted, ok, err := s.getWithQueryer(ctx, queryer, deltaflow.DeltaID(id))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return inserted, nil
}

func (s *DeltaStore) Get(ctx context.Context, deltaID deltaflow.DeltaID) (*deltaflow.Delta, bool, error) {
	return s.getWithQueryer(ctx, s.DB, deltaID)
}

func (s *DeltaStore) getWithQueryer(ctx context.Context, queryer queryRowContextExecutor, deltaID deltaflow.DeltaID) (*deltaflow.Delta, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	row := queryer.QueryRowContext(ctx, `
SELECT
	id,
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	occurred_at_micros,
	created_at_micros,
	dispatched_at_micros,
	metadata
FROM deltaflow_deltas
WHERE id = ?`, deltaID)

	delta, err := scanDelta(row)
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
	id,
	sync_id,
	origin,
	projection_type,
	projection_key,
	projection_key_hash,
	state,
	occurred_at_micros,
	created_at_micros,
	dispatched_at_micros,
	metadata
FROM deltaflow_deltas
WHERE state = 'pending'
	AND sync_id = ?
ORDER BY occurred_at_micros ASC, created_at_micros ASC, id ASC
LIMIT ?`, syncID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pulled := make([]*deltaflow.Delta, 0, limit)
	for rows.Next() {
		delta, scanErr := scanDelta(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		pulled = append(pulled, delta)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return pulled, nil
}

type dispatchPendingDelta struct {
	ID                deltaflow.DeltaID
	SyncID            deltaflow.SyncID
	ProjectionType    deltaflow.ProjectionType
	ProjectionKeyJSON []byte
	ProjectionKeyHash deltaflow.ProjectionKeyHash
}

func (s *DeltaStore) pullPendingForDispatchTx(ctx context.Context, tx *sql.Tx, syncID deltaflow.SyncID, limit int) ([]dispatchPendingDelta, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT
	id,
	sync_id,
	projection_type,
	projection_key,
	projection_key_hash
FROM deltaflow_deltas
WHERE state = 'pending'
	AND sync_id = ?
ORDER BY occurred_at_micros ASC, created_at_micros ASC, id ASC
LIMIT ?`, syncID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pending := make([]dispatchPendingDelta, 0, limit)
	for rows.Next() {
		var item dispatchPendingDelta
		var projectionKeyJSON string
		if err := rows.Scan(&item.ID, &item.SyncID, &item.ProjectionType, &projectionKeyJSON, &item.ProjectionKeyHash); err != nil {
			return nil, err
		}
		item.ProjectionKeyJSON = []byte(projectionKeyJSON)
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return pending, nil
}

func (s *DeltaStore) MarkDispatched(ctx context.Context, deltaID deltaflow.DeltaID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.markDispatchedTx(ctx, s.DB, deltaID, time.Now().UTC())
}

func (s *DeltaStore) markDispatchedTx(ctx context.Context, exec execContextExecutor, deltaID deltaflow.DeltaID, at time.Time) error {
	res, err := exec.ExecContext(ctx, `
UPDATE deltaflow_deltas
SET
	state = CASE WHEN state = 'dispatched' THEN state ELSE 'dispatched' END,
	dispatched_at_micros = CASE WHEN state = 'dispatched' THEN dispatched_at_micros ELSE ? END
WHERE id = ?`, microsFromTime(at), deltaID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return deltaflow.ErrDeltaNotFound
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDelta(scanner rowScanner) (*deltaflow.Delta, error) {
	var (
		id                string
		syncID            string
		origin            string
		projectionType    string
		projectionKeyText string
		projectionKeyHash string
		state             string
		occurredAtMicros  int64
		createdAtMicros   int64
		dispatchedAt      sql.NullInt64
		metadataText      sql.NullString
	)

	if err := scanner.Scan(
		&id,
		&syncID,
		&origin,
		&projectionType,
		&projectionKeyText,
		&projectionKeyHash,
		&state,
		&occurredAtMicros,
		&createdAtMicros,
		&dispatchedAt,
		&metadataText,
	); err != nil {
		return nil, err
	}

	var projectionKey deltaflow.ProjectionKey
	if err := json.Unmarshal([]byte(projectionKeyText), &projectionKey); err != nil {
		return nil, err
	}

	var metadata map[string]any
	if metadataText.Valid && metadataText.String != "" && metadataText.String != "null" {
		if err := json.Unmarshal([]byte(metadataText.String), &metadata); err != nil {
			return nil, err
		}
	}

	delta := &deltaflow.Delta{
		ID:                deltaflow.DeltaID(id),
		SyncID:            deltaflow.SyncID(syncID),
		Origin:            deltaflow.OriginOperationType(origin),
		ProjectionType:    deltaflow.ProjectionType(projectionType),
		ProjectionKey:     projectionKey,
		ProjectionKeyHash: deltaflow.ProjectionKeyHash(projectionKeyHash),
		State:             deltaflow.DeltaState(state),
		OccurredAt:        timeFromMicros(occurredAtMicros),
		CreatedAt:         timeFromMicros(createdAtMicros),
		Metadata:          metadata,
	}
	if dispatchedAt.Valid {
		t := timeFromMicros(dispatchedAt.Int64)
		delta.DispatchedAt = &t
	}

	return delta, nil
}
