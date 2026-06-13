package cli

import (
	"context"

	"github.com/lemenendez/deltaflow/internal/config"
	"github.com/spf13/cobra"
)

type options struct {
	configPath string
	storeDSN   string
}

func Execute(ctx context.Context) error {
	return NewRootCommand().ExecuteContext(ctx)
}

func NewRootCommand() *cobra.Command {
	opts := &options{}

	cmd := &cobra.Command{
		Use:          "deltaflow",
		Short:        "DeltaFlow command line tools",
		SilenceUsage: true,
	}

	cmd.PersistentFlags().StringVarP(&opts.configPath, "config", "c", "deltaflow.yaml", "path to the DeltaFlow YAML config")
	cmd.PersistentFlags().StringVar(&opts.storeDSN, "store-dsn", "", "override store.dsn from config")

	cmd.AddCommand(newValidateCommand(opts))
	cmd.AddCommand(newMigrateCommand(opts))

	return cmd
}

func (o *options) loadConfig() (*config.Config, error) {
	overrides := map[string]any{}
	if o.storeDSN != "" {
		overrides["store.dsn"] = o.storeDSN
	}

	return config.LoadFile(o.configPath, config.LoadOptions{Overrides: overrides})
}
