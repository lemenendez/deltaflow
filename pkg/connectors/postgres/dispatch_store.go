package postgres

import (
	"context"
	"errors"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type DispatchStoreConfig struct{}

type dispatchPendingDelta struct {
	ID                deltaflow.DeltaID
	SyncID            deltaflow.SyncID
	ProjectionType    deltaflow.ProjectionType
	ProjectionKeyJSON []byte
	ProjectionKeyHash deltaflow.ProjectionKeyHash
}

type DispatchStore struct {
	deltaStore *DeltaStore
	jobStore   *JobStore
}

func NewDispatchStore(deltaStore *DeltaStore, jobStore *JobStore, _ DispatchStoreConfig) *DispatchStore {
	return &DispatchStore{deltaStore: deltaStore, jobStore: jobStore}
}

func (s *DispatchStore) DispatchPending(ctx context.Context, syncID deltaflow.SyncID, limit int) ([]*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	if s.deltaStore == nil || s.jobStore == nil {
		return nil, errors.New("dispatch store requires delta and job stores")
	}
	if s.deltaStore.DB == nil || s.jobStore.DB == nil {
		return nil, errors.New("dispatch store requires configured databases")
	}
	if s.deltaStore.DB != s.jobStore.DB {
		return nil, errors.New("dispatch store requires delta and job stores sharing same database")
	}

	tx, err := s.deltaStore.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	pending, err := s.deltaStore.pullPendingForDispatchTx(ctx, tx, syncID, limit)
	if err != nil {
		return nil, err
	}

	if len(pending) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	now := s.jobStore.Now().UTC()
	jobs := make([]*deltaflow.SyncJob, 0, len(pending))

	for _, delta := range pending {
		job, ok, err := s.jobStore.createOutboxFromDeltaTx(ctx, tx, delta, now)
		if err != nil {
			return nil, err
		}
		if ok {
			jobs = append(jobs, job)
		}

		if err := s.deltaStore.markDispatchedTx(ctx, tx, delta.ID, now); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return jobs, nil
}
