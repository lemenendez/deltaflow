package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	runtimepkg "github.com/lemenendez/deltaflow/pkg/runtime"
)

func TestRootCommandSilencesCobraErrors(t *testing.T) {
	cmd := NewRootCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"doctor", "--config", "does-not-exist.yaml"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("Execute error = nil")
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	if !strings.Contains(err.Error(), "does-not-exist.yaml") {
		t.Fatalf("error = %v", err)
	}
}

func TestRootCommandDoctorFlagWiring(t *testing.T) {
	validConfig := writeCLIConfig(t, `
store:
  type: postgres
  dsn: postgres://deltaflow:deltaflow@localhost:5432/deltaflow?sslmode=disable

workers:
  concurrency: 8
  lease_ttl: 30s

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
`)
	missingDSNConfig := writeCLIConfig(t, `
store:
  type: postgres

workers:
  concurrency: 8
  lease_ttl: 30s

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
`)
	blankDSNConfig := writeCLIConfig(t, `
store:
  type: postgres
  dsn: "   "

workers:
  concurrency: 8
  lease_ttl: 30s

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
`)
	unsupportedStoreConfig := writeCLIConfig(t, `
store:
  type: mysql
  dsn: mysql://localhost:3306/deltaflow

workers:
  concurrency: 8
  lease_ttl: 30s

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
`)
	sqliteConfig := writeCLIConfig(t, `
store:
  type: sqlite
  dsn: file:test-validate.db

workers:
  concurrency: 1
  lease_ttl: 30s

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
`)
	tests := []struct {
		name        string
		args        []string
		wantOutput  string
		wantErrText string
	}{
		{
			name:       "config flag before subcommand",
			args:       []string{"--config", validConfig, "doctor"},
			wantOutput: "config OK\nnote: this stock CLI does not run workers; worker execution must be implemented by your application\nnote: see playground/01-in-memory and playground/README.md for current examples; larger archived playgrounds are described in the v0.11.2 release notes\n",
		},
		{
			name:       "config shorthand on doctor subcommand",
			args:       []string{"doctor", "-c", validConfig},
			wantOutput: "config OK\nnote: this stock CLI does not run workers; worker execution must be implemented by your application\nnote: see playground/01-in-memory and playground/README.md for current examples; larger archived playgrounds are described in the v0.11.2 release notes\n",
		},
		{
			name:       "validate alias remains supported",
			args:       []string{"validate", "-c", validConfig},
			wantOutput: "config OK\nnote: this stock CLI does not run workers; worker execution must be implemented by your application\nnote: see playground/01-in-memory and playground/README.md for current examples; larger archived playgrounds are described in the v0.11.2 release notes\n",
		},
		{
			name:       "store dsn override satisfies required dsn",
			args:       []string{"doctor", "--config", missingDSNConfig, "--store-dsn", "postgres://override"},
			wantOutput: "config OK\nnote: this stock CLI does not run workers; worker execution must be implemented by your application\nnote: see playground/01-in-memory and playground/README.md for current examples; larger archived playgrounds are described in the v0.11.2 release notes\n",
		},
		{
			name:        "missing config returns load error without cobra stderr",
			args:        []string{"doctor", "--config", filepath.Join(t.TempDir(), "missing.yaml")},
			wantErrText: "missing.yaml",
		},
		{
			name:        "run command proceeds past registration checks",
			args:        []string{"run", "--config", validConfig},
			wantErrText: "connect postgres",
		},
		{
			name:        "run command rejects blank dsn with clear error",
			args:        []string{"run", "--config", blankDSNConfig},
			wantErrText: "run requires store.dsn to be set",
		},
		{
			name:        "run command rejects unsupported store type",
			args:        []string{"run", "--config", unsupportedStoreConfig},
			wantErrText: "store.type must be postgres or sqlite",
		},
		{
			name:       "doctor sqlite emits stock-cli and single-worker notes",
			args:       []string{"doctor", "--config", sqliteConfig},
			wantOutput: "config OK\nnote: this stock CLI does not run workers; worker execution must be implemented by your application\nnote: see playground/01-in-memory and playground/README.md for current examples; larger archived playgrounds are described in the v0.11.2 release notes\nnote: sqlite supports only workers.concurrency=1\nnote: sqlite does not support multiple competing worker processes\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewRootCommandWithRegistry(testRuntimeRegistry())
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs(tt.args)

			err := cmd.Execute()
			if tt.wantErrText == "" && err != nil {
				t.Fatalf("Execute error: %v", err)
			}
			if tt.wantErrText != "" {
				if err == nil {
					t.Fatal("Execute error = nil")
				}
				if !strings.Contains(err.Error(), tt.wantErrText) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErrText)
				}
			}
			if stdout.String() != tt.wantOutput {
				t.Fatalf("stdout = %q, want %q", stdout.String(), tt.wantOutput)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestRootCommandRegistersSubcommands(t *testing.T) {
	cmd := NewRootCommand()

	for _, name := range []string{"doctor", "migrate"} {
		if _, _, err := cmd.Find([]string{name}); err != nil {
			t.Fatalf("Find(%q) error: %v", name, err)
		}
	}

	if _, _, err := cmd.Find([]string{"validate"}); err != nil {
		t.Fatalf("Find(%q) error: %v", "validate", err)
	}

	if _, _, err := cmd.Find([]string{"run"}); err == nil {
		t.Fatal("Find(\"run\") error = nil, want unknown command")
	}
}

func testRuntimeRegistry() *runtimepkg.Registry {
	registry := runtimepkg.NewRegistry()
	registry.RegisterProjector("contact-projector", func(context.Context, runtimepkg.PipelineSpec) (deltaflow.Projector, error) {
		return deltaflow.ProjectorFunc(func(context.Context, deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
			return deltaflow.Projection{}, nil
		}), nil
	})
	registry.RegisterApplier("elasticsearch", func(context.Context, runtimepkg.PipelineSpec) (deltaflow.ProjectionApplier, error) {
		return deltaflow.ProjectionApplierFunc(func(context.Context, deltaflow.ProjectionOperation) error {
			return nil
		}), nil
	})
	return registry
}

func writeCLIConfig(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "deltaflow.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
