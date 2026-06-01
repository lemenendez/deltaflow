package postgres

import (
	"context"
	"database/sql"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type txDeltaEnqueuer interface {
	EnqueueInTx(ctx context.Context, tx *sql.Tx, delta deltaflow.Delta) (*deltaflow.Delta, error)
}

var (
	_ deltaflow.DeltaStore = (*DeltaStore)(nil)
	_ txDeltaEnqueuer      = (*DeltaStore)(nil)
)
