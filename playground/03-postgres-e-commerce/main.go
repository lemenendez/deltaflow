package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lemenendez/deltaflow/playground/internal/playpg"
)

const (
	syncID         = "playground-03-products-to-elasticsearch"
	projectionType = "ProductSearchDocument"
)

var (
	seed          = uint64(3003)
	productCount  = 14
	mutationCount = 56
	writerCount   = 4
	workerCount   = 2
	maxAttempts   = 3
)

var baseActorNames = []string{
	"web-server-1",
	"web-server-2",
	"logistics-worker-1",
	"pricing-worker-1",
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := loadConfig(); err != nil {
		log.Fatalf("load config: %v", err)
	}

	dsn := os.Getenv("DELTAFLOW_PG_DSN")
	if dsn == "" {
		dsn = playpg.DefaultDSN()
	}

	result, err := runDemo(ctx, dsn)
	if err != nil {
		log.Fatalf("scenario failed: %v", err)
	}

	printReport(result)
}

func loadConfig() error {
	var err error
	if seed, err = playpg.EnvUint64("SIM_SEED", seed); err != nil {
		return err
	}
	if productCount, err = playpg.EnvInt("PRODUCT_COUNT", productCount); err != nil {
		return err
	}
	if mutationCount, err = playpg.EnvInt("MUTATION_COUNT", mutationCount); err != nil {
		return err
	}
	if writerCount, err = playpg.EnvInt("WRITER_COUNT", writerCount); err != nil {
		return err
	}
	if writerCount < 4 {
		return fmt.Errorf("WRITER_COUNT must be >= 4 for this scenario")
	}
	if workerCount, err = playpg.EnvInt("WORKER_COUNT", workerCount); err != nil {
		return err
	}
	if maxAttempts, err = playpg.EnvInt("MAX_ATTEMPTS", maxAttempts); err != nil {
		return err
	}
	return nil
}

func actorName(actorID int) string {
	if actorID >= 0 && actorID < len(baseActorNames) {
		return baseActorNames[actorID]
	}
	return fmt.Sprintf("web-server-%d", actorID+1)
}
