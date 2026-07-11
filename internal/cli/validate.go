package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func newValidateCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:     "doctor",
		Aliases: []string{"validate"},
		Short:   "Check DeltaFlow config; this is not the worker run command",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := opts.loadConfig()
			if err != nil {
				return err
			}

			fmt.Fprintln(cmd.OutOrStdout(), "config OK")
			fmt.Fprintln(cmd.OutOrStdout(), "note: this stock CLI does not run workers; worker execution must be implemented by your application")
			fmt.Fprintln(cmd.OutOrStdout(), "note: see playground/01-in-memory and playground/README.md for current examples; larger archived playgrounds are described in the v0.11.2 release notes")
			if strings.TrimSpace(cfg.Store.Type) == "sqlite" {
				fmt.Fprintln(cmd.OutOrStdout(), "note: sqlite supports only workers.concurrency=1")
				fmt.Fprintln(cmd.OutOrStdout(), "note: sqlite does not support multiple competing worker processes")
			}
			return nil
		},
	}
}
