package internal

import (
	"context"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type MemoryDispatcher struct {
	Store *MemoryDeltaStore
}

func NewMemoryDispatcher(store *MemoryDeltaStore) *MemoryDispatcher {
	if store == nil {
		store = NewMemoryDeltaStore()
	}
	return &MemoryDispatcher{Store: store}
}

func (d *MemoryDispatcher) Dispatch(ctx context.Context, delta deltaflow.Delta) (*deltaflow.Delta, error) {
	return d.Store.Insert(ctx, delta)
}
