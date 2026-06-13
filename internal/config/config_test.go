package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadFileExpandsEnvironmentAndValidatesCanonicalShape(t *testing.T) {
	t.Setenv("DELTAFLOW_STORE_DSN", "postgres://deltaflow:deltaflow@localhost:5432/deltaflow?sslmode=disable")

	cfg, err := LoadFile(writeConfig(t, canonicalConfig()), LoadOptions{})
	if err != nil {
		t.Fatalf("LoadFile error: %v", err)
	}

	if cfg.Store.DSN != "postgres://deltaflow:deltaflow@localhost:5432/deltaflow?sslmode=disable" {
		t.Fatalf("Store.DSN = %q", cfg.Store.DSN)
	}
	if cfg.Pipelines[0].SyncID != "contacts-to-elasticsearch" {
		t.Fatalf("SyncID = %q", cfg.Pipelines[0].SyncID)
	}
	if _, err := cfg.Workers.LeaseTTLDuration(); err != nil {
		t.Fatalf("LeaseTTLDuration error: %v", err)
	}
}

func TestLoadFileRejectsVersionField(t *testing.T) {
	t.Setenv("DELTAFLOW_STORE_DSN", "postgres://deltaflow:deltaflow@localhost:5432/deltaflow?sslmode=disable")

	_, err := LoadFile(writeConfig(t, "version: 1\n"+canonicalConfig()), LoadOptions{})
	if err == nil {
		t.Fatal("LoadFile error = nil")
	}
	if !strings.Contains(err.Error(), "version is not supported yet") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadFileReportsInvalidDurationAndUnsupportedTypes(t *testing.T) {
	body := `
store:
  type: memory
  dsn: postgres://deltaflow:deltaflow@localhost:5432/deltaflow?sslmode=disable

workers:
  concurrency: 8
  lease_ttl: soon

pipelines:
  - name: contacts-to-elasticsearch
    sync_id: contacts-to-elasticsearch
    source:
      type: kafka
      projection_type: contact
    projector:
      name: contact-projector
    target:
      type: opensearch
      index: contacts
    applier:
      mode: insert
`

	_, err := LoadFile(writeConfig(t, body), LoadOptions{})
	if err == nil {
		t.Fatal("LoadFile error = nil")
	}

	for _, want := range []string{
		"store.type must be postgres",
		"workers.lease_ttl must be a valid duration",
		"pipelines[0].source.type must be postgres-outbox",
		"pipelines[0].target.type must be elasticsearch",
		"pipelines[0].applier.mode must be upsert",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
}

func TestLoadFileAppliesOverrides(t *testing.T) {
	t.Setenv("DELTAFLOW_STORE_DSN", "postgres://old")

	cfg, err := LoadFile(writeConfig(t, canonicalConfig()), LoadOptions{
		Overrides: map[string]any{"store.dsn": "postgres://override"},
	})
	if err != nil {
		t.Fatalf("LoadFile error: %v", err)
	}
	if cfg.Store.DSN != "postgres://override" {
		t.Fatalf("Store.DSN = %q", cfg.Store.DSN)
	}
}

func canonicalConfig() string {
	return `
store:
  type: postgres
  dsn: ${DELTAFLOW_STORE_DSN}

workers:
  concurrency: 8
  lease_ttl: 30s
  pull_size: 1
  max_attempts: 5

pipelines:
  - name: contacts-to-elasticsearch
    sync_id: contacts-to-elasticsearch
    source:
      type: postgres-outbox
      projection_type: contact
    projector:
      name: contact-projector
    target:
      type: elasticsearch
      index: contacts
    applier:
      mode: upsert
`
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "deltaflow.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
