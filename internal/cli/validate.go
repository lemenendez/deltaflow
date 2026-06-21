package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func newValidateCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate a DeltaFlow YAML config",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := opts.loadConfig()
			if err != nil {
				return err
			}

			fmt.Fprintln(cmd.OutOrStdout(), "config OK")
			if strings.TrimSpace(cfg.Store.Type) == "sqlite" {
				fmt.Fprintln(cmd.OutOrStdout(), "note: sqlite supports only workers.concurrency=1")
				fmt.Fprintln(cmd.OutOrStdout(), "note: sqlite does not support multiple competing worker processes")
			}
			return nil
		},
	}
}
