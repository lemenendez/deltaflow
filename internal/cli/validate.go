package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newValidateCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate a DeltaFlow YAML config",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if _, err := opts.loadConfig(); err != nil {
				return err
			}

			fmt.Fprintln(cmd.OutOrStdout(), "config OK")
			return nil
		},
	}
}
