package contactsruntime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lemenendez/deltaflow/pkg/connectors/elasticsearch"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	runtimepkg "github.com/lemenendez/deltaflow/pkg/runtime"
)

type RegisterConfig struct {
	ProjectorName string
	TargetType    string
	SourceTable   string

	ESEndpointEnv string
	ESRefreshEnv  string

	HTTPClient *http.Client
}

func Register(registry *runtimepkg.Registry, cfg RegisterConfig) error {
	if registry == nil {
		return errors.New("contact runtime register: registry is required")
	}

	projectorName := cfg.ProjectorName
	if projectorName == "" {
		projectorName = "contact-projector"
	}
	targetType := cfg.TargetType
	if targetType == "" {
		targetType = "elasticsearch"
	}
	sourceTable := cfg.SourceTable
	if sourceTable == "" {
		sourceTable = "app_contacts"
	}
	esEndpointEnv := cfg.ESEndpointEnv
	if esEndpointEnv == "" {
		esEndpointEnv = "DELTAFLOW_ES_ENDPOINT"
	}
	esRefreshEnv := cfg.ESRefreshEnv
	if esRefreshEnv == "" {
		esRefreshEnv = "DELTAFLOW_ES_REFRESH"
	}

	var projectorOnce sync.Once
	var projectorDB *sql.DB
	var projectorDSN string
	var projectorInitErr error
	registry.RegisterProjector(projectorName, func(ctx context.Context, spec runtimepkg.PipelineSpec) (deltaflow.Projector, error) {
		if spec.StoreType != "postgres" {
			return nil, fmt.Errorf("contact projector requires postgres store, got %q", spec.StoreType)
		}
		dsn := strings.TrimSpace(spec.StoreDSN)
		if dsn == "" {
			return nil, errors.New("contact projector requires store dsn")
		}

		projectorOnce.Do(func() {
			projectorDSN = dsn
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				projectorInitErr = err
				return
			}
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := db.PingContext(pingCtx); err != nil {
				_ = db.Close()
				projectorInitErr = fmt.Errorf("contact projector connect postgres: %w", err)
				return
			}
			projectorDB = db
		})
		if dsn != projectorDSN {
			return nil, errors.New("contact projector store dsn changed after initialization; use a new runtime registry")
		}
		if projectorInitErr != nil {
			return nil, projectorInitErr
		}
		return NewContactProjector(projectorDB, sourceTable)
	})

	registry.RegisterApplier(targetType, func(_ context.Context, spec runtimepkg.PipelineSpec) (deltaflow.ProjectionApplier, error) {
		endpoint := os.Getenv(esEndpointEnv)
		if endpoint == "" {
			endpoint = "http://localhost:9200"
		}
		if spec.TargetIndex == "" {
			return nil, errors.New("contact runtime applier requires target index")
		}
		return elasticsearch.NewApplier(elasticsearch.ApplierConfig{
			Client:   cfg.HTTPClient,
			Endpoint: endpoint,
			Index:    spec.TargetIndex,
			Refresh:  os.Getenv(esRefreshEnv),
		})
	})

	return nil
}
