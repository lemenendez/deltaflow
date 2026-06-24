package cli

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/lemenendez/deltaflow/internal/config"

	_ "modernc.org/sqlite"
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
		{name: "tiny positive lease clamps above zero", leaseTTL: time.Nanosecond, want: time.Nanosecond},
		{name: "small lease uses half", leaseTTL: 1200 * time.Millisecond, want: 600 * time.Millisecond},
		{name: "very short lease uses half", leaseTTL: 500 * time.Millisecond, want: 250 * time.Millisecond},
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

func TestSQLiteLockRenewTimeout(t *testing.T) {
	tests := []struct {
		name     string
		leaseTTL time.Duration
		want     time.Duration
	}{
		{name: "non-positive lease uses 2s floor", leaseTTL: 0, want: 2 * time.Second},
		{name: "small lease uses 2s floor", leaseTTL: 1200 * time.Millisecond, want: 2 * time.Second},
		{name: "large lease follows interval", leaseTTL: 20 * time.Second, want: 10 * time.Second},
		{name: "capped interval remains timeout", leaseTTL: 40 * time.Second, want: 10 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sqliteLockRenewTimeout(tt.leaseTTL)
			if got != tt.want {
				t.Fatalf("sqliteLockRenewTimeout(%v) = %v, want %v", tt.leaseTTL, got, tt.want)
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

func TestConfigurePoolForStoreTypeSQLite(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	configurePoolForStoreType(db, "sqlite")

	stats := db.Stats()
	if stats.MaxOpenConnections != 1 {
		t.Fatalf("MaxOpenConnections = %d, want 1", stats.MaxOpenConnections)
	}
}

func TestConfigurePoolForStoreTypeNonSQLiteUnchanged(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	configurePoolForStoreType(db, "postgres")

	stats := db.Stats()
	if stats.MaxOpenConnections != 0 {
		t.Fatalf("MaxOpenConnections = %d, want 0", stats.MaxOpenConnections)
	}
}

func TestSQLiteForeignKeysCanBeEnabled(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open error: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatalf("enable foreign keys error: %v", err)
	}

	var enabled int
	if err := db.QueryRow(`PRAGMA foreign_keys`).Scan(&enabled); err != nil {
		t.Fatalf("query foreign keys pragma error: %v", err)
	}
	if enabled != 1 {
		t.Fatalf("foreign_keys = %d, want 1", enabled)
	}
}
