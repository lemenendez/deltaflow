package sqlite

import deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"

var (
	_ deltaflow.DeltaStore    = (*DeltaStore)(nil)
	_ deltaflow.JobStore      = (*JobStore)(nil)
	_ deltaflow.DispatchStore = (*DispatchStore)(nil)
)
