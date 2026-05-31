package testenv

import (
	"context"
	"database/sql"
	"os"
	"testing"
)

// Provider boots a disposable test database and returns a live sql.DB and cleanup hook.
type Provider interface {
	Name() string
	Open(ctx context.Context, t *testing.T) (*sql.DB, func())
}

// NewFromEnv chooses the integration provider based on DELTAFLOW_IT_DB.
// Supported values: postgres (default).
func NewFromEnv(t *testing.T) Provider {
	t.Helper()
	engine := os.Getenv("DELTAFLOW_IT_DB")
	if engine == "" {
		engine = "postgres"
	}

	switch engine {
	case "postgres":
		return NewPostgresProvider()
	default:
		t.Fatalf("unsupported DELTAFLOW_IT_DB=%q (supported: postgres)", engine)
		return nil
	}
}
