package deltaflow

import "time"

// LeaseTelemetry captures low-cardinality lease signals for metrics exporters.
// Implementations should avoid high-cardinality labels (for example, job IDs).
type LeaseTelemetry interface {
	ObserveLeaseClaim(result string)
	ObserveLeaseRenew(result string, duration time.Duration)
	ObserveLeaseOwnershipCheck(transition string, result string)
	ObserveLeaseReclaim()
}

type noopLeaseTelemetry struct{}

func (noopLeaseTelemetry) ObserveLeaseClaim(string) {}

func (noopLeaseTelemetry) ObserveLeaseRenew(string, time.Duration) {}

func (noopLeaseTelemetry) ObserveLeaseOwnershipCheck(string, string) {}

func (noopLeaseTelemetry) ObserveLeaseReclaim() {}

// NoopLeaseTelemetry returns a telemetry implementation that records nothing.
func NoopLeaseTelemetry() LeaseTelemetry {
	return noopLeaseTelemetry{}
}
