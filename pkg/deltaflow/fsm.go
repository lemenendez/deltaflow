package deltaflow

type DeltaState string

const (
	StatePending    DeltaState = "PENDING"
	StateProcessing DeltaState = "PROCESSING"
	StateSynced     DeltaState = "SYNCED"
	StateRetrying   DeltaState = "RETRYING"
	StateDead       DeltaState = "DEAD"
)
