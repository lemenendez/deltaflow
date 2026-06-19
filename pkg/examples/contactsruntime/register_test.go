package contactsruntime

import (
	"context"
	"strings"
	"testing"

	runtimepkg "github.com/lemenendez/deltaflow/pkg/runtime"
)

func TestRegisterProjectorRejectsStoreDSNChangeAfterInitialization(t *testing.T) {
	registry := runtimepkg.NewRegistry()
	if err := Register(registry, RegisterConfig{}); err != nil {
		t.Fatalf("Register error: %v", err)
	}

	firstCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, firstErr := registry.ResolvePipeline(firstCtx, runtimepkg.PipelineSpec{
		Name:          "contacts",
		ProjectorName: "contact-projector",
		TargetType:    "elasticsearch",
		TargetIndex:   "contacts",
		StoreType:     "postgres",
		StoreDSN:      "postgres://first",
	})
	if firstErr == nil {
		t.Fatal("ResolvePipeline firstErr = nil")
	}

	_, secondErr := registry.ResolvePipeline(context.Background(), runtimepkg.PipelineSpec{
		Name:          "contacts",
		ProjectorName: "contact-projector",
		TargetType:    "elasticsearch",
		TargetIndex:   "contacts",
		StoreType:     "postgres",
		StoreDSN:      "postgres://second",
	})
	if secondErr == nil {
		t.Fatal("ResolvePipeline secondErr = nil")
	}
	if !strings.Contains(secondErr.Error(), "store dsn changed after initialization") {
		t.Fatalf("error = %v", secondErr)
	}
}
