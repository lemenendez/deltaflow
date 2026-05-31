package testenv

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type PostgresProvider struct{}

func NewPostgresProvider() *PostgresProvider {
	return &PostgresProvider{}
}

func (p *PostgresProvider) Name() string { return "postgres" }

func (p *PostgresProvider) Open(ctx context.Context, t *testing.T) (*sql.DB, func()) {
	t.Helper()

	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:        "postgres:17",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       "deltaflow",
				"POSTGRES_USER":     "deltaflow",
				"POSTGRES_PASSWORD": "deltaflow",
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("5432/tcp"),
				wait.ForLog("database system is ready to accept connections").WithOccurrence(1),
			).WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("container mapped port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://deltaflow:deltaflow@%s:%s/deltaflow?sslmode=disable", host, port.Port())
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("sql.Open: %v", err)
	}

	if err := waitForPing(ctx, db); err != nil {
		db.Close()
		_ = container.Terminate(ctx)
		t.Fatalf("db ping: %v", err)
	}

	if err := preparePostgresCompat(ctx, db); err != nil {
		db.Close()
		_ = container.Terminate(ctx)
		t.Fatalf("postgres compatibility bootstrap: %v", err)
	}

	if err := applyRepoMigrations(ctx, db); err != nil {
		db.Close()
		_ = container.Terminate(ctx)
		t.Fatalf("apply migrations: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		_ = container.Terminate(context.Background())
	}

	return db, cleanup
}

func waitForPing(ctx context.Context, db *sql.DB) error {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := db.PingContext(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("ping timeout: %w", lastErr)
}

func preparePostgresCompat(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pgcrypto"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "CREATE OR REPLACE FUNCTION uuidv7() RETURNS uuid LANGUAGE SQL AS $$ SELECT gen_random_uuid(); $$;"); err != nil {
		return err
	}
	return nil
}

func applyRepoMigrations(ctx context.Context, db *sql.DB) error {
	migrationsDir := repoMigrationsDir()
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return err
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(migrationsDir, e.Name()))
	}
	sort.Strings(files)

	for _, file := range files {
		sqlBytes, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(file), err)
		}
	}

	return nil
}

func repoMigrationsDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	integrationRoot := filepath.Dir(filepath.Dir(thisFile))
	repoRoot := filepath.Dir(integrationRoot)
	return filepath.Join(repoRoot, "pkg", "connectors", "postgres", "migrations")
}
