package cli

import (
	"context"

	"github.com/lemenendez/deltaflow/internal/config"
	runtimepkg "github.com/lemenendez/deltaflow/pkg/runtime"
	"github.com/spf13/cobra"
)

type options struct {
	configPath      string
	storeDSN        string
	runtimeRegistry *runtimepkg.Registry
}

func Execute(ctx context.Context) error {
	return NewRootCommand().ExecuteContext(ctx)
}

func NewRootCommand() *cobra.Command {
	return NewRootCommandWithRegistry(nil)
}

func NewRootCommandWithRegistry(registry *runtimepkg.Registry) *cobra.Command {
	opts := &options{}
	opts.runtimeRegistry = registry

	cmd := &cobra.Command{
		Use:           "deltaflow",
		Short:         "DeltaFlow command line tools",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.PersistentFlags().StringVarP(&opts.configPath, "config", "c", "deltaflow.yaml", "path to the DeltaFlow YAML config")
	cmd.PersistentFlags().StringVar(&opts.storeDSN, "store-dsn", "", "override store.dsn from config")

	cmd.AddCommand(newValidateCommand(opts))
	cmd.AddCommand(newMigrateCommand(opts))
	if opts.runtimeRegistry != nil {
		cmd.AddCommand(newRunCommand(opts))
	}

	return cmd
}

func (o *options) loadConfig() (*config.Config, error) {
	overrides := map[string]any{}
	if o.storeDSN != "" {
		overrides["store.dsn"] = o.storeDSN
	}

	return config.LoadFile(o.configPath, config.LoadOptions{Overrides: overrides})
}
