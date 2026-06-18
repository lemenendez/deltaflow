package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootCommandSilencesCobraErrors(t *testing.T) {
	cmd := NewRootCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"validate", "--config", "does-not-exist.yaml"})

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

func TestRootCommandValidateFlagWiring(t *testing.T) {
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
	tests := []struct {
		name        string
		args        []string
		wantOutput  string
		wantErrText string
	}{
		{
			name:       "config flag before subcommand",
			args:       []string{"--config", validConfig, "validate"},
			wantOutput: "config OK\n",
		},
		{
			name:       "config shorthand on subcommand",
			args:       []string{"validate", "-c", validConfig},
			wantOutput: "config OK\n",
		},
		{
			name:       "store dsn override satisfies required dsn",
			args:       []string{"validate", "--config", missingDSNConfig, "--store-dsn", "postgres://override"},
			wantOutput: "config OK\n",
		},
		{
			name:        "missing config returns load error without cobra stderr",
			args:        []string{"validate", "--config", filepath.Join(t.TempDir(), "missing.yaml")},
			wantErrText: "missing.yaml",
		},
		{
			name:        "run command fails early without runtime registrations",
			args:        []string{"run", "--config", validConfig},
			wantErrText: "runtime projector not registered",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewRootCommand()
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

	for _, name := range []string{"validate", "migrate", "run"} {
		if _, _, err := cmd.Find([]string{name}); err != nil {
			t.Fatalf("Find(%q) error: %v", name, err)
		}
	}
}

func writeCLIConfig(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "deltaflow.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
