package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	hostpkg "github.com/lemenendez/deltaflow/internal/host"
)

const (
	syncID         = "playground-03-products-to-elasticsearch"
	projectionType = "ProductSearchDocument"
)

var (
	seed                  = uint64(3003)
	productCount          = 14
	mutationCount         = 56
	writerCount           = 4
	workerConcurrency     = 2
	workerBatchSize       = 64
	workerMaxAttempts     = 3
	elasticsearchEndpoint = ""
)

var baseActorNames = []string{
	"web-server-1",
	"web-server-2",
	"logistics-worker-1",
	"pricing-worker-1",
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := loadConfig(); err != nil {
		log.Fatalf("load config: %v", err)
	}

	dsn := os.Getenv("DELTAFLOW_PG_DSN")
	if dsn == "" {
		dsn = hostpkg.DefaultDSN()
	}

	result, err := runDemo(ctx, dsn)
	if err != nil {
		log.Fatalf("scenario failed: %v", err)
	}

	printReport(result)
}

func loadConfig() error {
	var err error
	if seed, err = hostpkg.EnvUint64("SIM_SEED", seed); err != nil {
		return err
	}
	if productCount, err = hostpkg.EnvInt("PRODUCT_COUNT", productCount); err != nil {
		return err
	}
	if mutationCount, err = hostpkg.EnvInt("MUTATION_COUNT", mutationCount); err != nil {
		return err
	}
	if writerCount, err = hostpkg.EnvInt("WRITER_COUNT", writerCount); err != nil {
		return err
	}
	if writerCount < 4 {
		return fmt.Errorf("WRITER_COUNT must be >= 4 for this scenario")
	}
	if workerConcurrency, err = hostpkg.EnvInt("WORKERS_CONCURRENCY", workerConcurrency); err != nil {
		return err
	}
	if workerBatchSize, err = hostpkg.EnvInt("WORKERS_BATCH_SIZE", workerBatchSize); err != nil {
		return err
	}
	if workerMaxAttempts, err = hostpkg.EnvInt("WORKERS_MAX_ATTEMPTS", workerMaxAttempts); err != nil {
		return err
	}
	elasticsearchEndpoint = os.Getenv("DELTAFLOW_ES_ENDPOINT")
	return nil
}

func actorName(actorID int) string {
	if actorID >= 0 && actorID < len(baseActorNames) {
		return baseActorNames[actorID]
	}
	return fmt.Sprintf("web-server-%d", actorID+1)
}
