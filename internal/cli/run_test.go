package cli

import (
	"testing"

	"github.com/lemenendez/deltaflow/internal/config"
)

func TestWorkerSizingDefaultsPullSizeToUnsetAndBatchSizeToOne(t *testing.T) {
	pullSize, batchSize := workerSizing(config.WorkersConfig{})
	if pullSize != 0 {
		t.Fatalf("pullSize = %d, want 0", pullSize)
	}
	if batchSize != 1 {
		t.Fatalf("batchSize = %d, want 1", batchSize)
	}
}

func TestWorkerSizingUsesConfiguredValues(t *testing.T) {
	pullSizeValue := 24
	batchSizeValue := 6

	pullSize, batchSize := workerSizing(config.WorkersConfig{
		PullSize:  &pullSizeValue,
		BatchSize: &batchSizeValue,
	})
	if pullSize != pullSizeValue {
		t.Fatalf("pullSize = %d, want %d", pullSize, pullSizeValue)
	}
	if batchSize != batchSizeValue {
		t.Fatalf("batchSize = %d, want %d", batchSize, batchSizeValue)
	}
}
