package cli

import (
	"errors"
	"testing"
	"time"

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

func TestSQLiteLockHeartbeatInterval(t *testing.T) {
	tests := []struct {
		name     string
		leaseTTL time.Duration
		want     time.Duration
	}{
		{name: "non-positive lease", leaseTTL: 0, want: time.Second},
		{name: "small lease rounds up", leaseTTL: 1200 * time.Millisecond, want: time.Second},
		{name: "uses half lease", leaseTTL: 8 * time.Second, want: 4 * time.Second},
		{name: "caps long lease", leaseTTL: 40 * time.Second, want: 10 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sqliteLockHeartbeatInterval(tt.leaseTTL)
			if got != tt.want {
				t.Fatalf("sqliteLockHeartbeatInterval(%v) = %v, want %v", tt.leaseTTL, got, tt.want)
			}
		})
	}
}

func TestSQLiteHeartbeatWatcherRetainsError(t *testing.T) {
	errCh := make(chan error, 1)
	heartbeatErr := errors.New("heartbeat failed")
	errCh <- heartbeatErr
	close(errCh)

	cancelled := make(chan struct{}, 1)
	w := startSQLiteHeartbeatWatcher(errCh, func() {
		select {
		case cancelled <- struct{}{}:
		default:
		}
	})
	w.wait()

	if got := w.err(); !errors.Is(got, heartbeatErr) {
		t.Fatalf("watcher.err() = %v, want %v", got, heartbeatErr)
	}
	if got := w.err(); !errors.Is(got, heartbeatErr) {
		t.Fatalf("watcher.err() second read = %v, want %v", got, heartbeatErr)
	}

	select {
	case <-cancelled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cancel callback was not invoked")
	}
}

func TestSQLiteHeartbeatWatcherIgnoresClosedChannelWithoutError(t *testing.T) {
	errCh := make(chan error)
	close(errCh)

	cancelled := false
	w := startSQLiteHeartbeatWatcher(errCh, func() { cancelled = true })
	w.wait()

	if got := w.err(); got != nil {
		t.Fatalf("watcher.err() = %v, want nil", got)
	}
	if cancelled {
		t.Fatal("cancel callback called unexpectedly")
	}
}
