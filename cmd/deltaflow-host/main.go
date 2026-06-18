package main

import (
	"context"
	"fmt"
	"os"

	"github.com/lemenendez/deltaflow/internal/cli"
	"github.com/lemenendez/deltaflow/pkg/examples/contactsruntime"
	runtimepkg "github.com/lemenendez/deltaflow/pkg/runtime"
)

func main() {
	registry := runtimepkg.NewRegistry()
	if err := contactsruntime.Register(registry, contactsruntime.RegisterConfig{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	cmd := cli.NewRootCommandWithRegistry(registry)
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
