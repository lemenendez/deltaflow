package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	"github.com/lemenendez/deltaflow/playground/internal/playpg"
)

type mutation struct {
	Seq       int
	ActorID   int
	ProductID string
	Kind      string
	Value     any
	At        time.Time
}

type scenario struct {
	source *catalogStore
	target searchTarget
	events []mutation
}

type writerRunResult struct {
	Enqueued int
	Planned  int
}

type writerAck struct {
	event mutation
	err   error
}

func buildScenario(ctx context.Context, stores *playpg.Stores) (*scenario, error) {
	faker := gofakeit.New(seed)
	source := newCatalogStore(stores.DB)
	if err := source.ensureSchema(ctx); err != nil {
		return nil, err
	}

	products := make([]product, 0, productCount+1)
	for i := 1; i <= productCount; i++ {
		id := fmt.Sprintf("sku-%03d", i)
		products = append(products, product{
			ID:              id,
			Name:            faker.ProductName(),
			Description:     faker.ProductDescription(),
			ImageURL:        fmt.Sprintf("https://cdn.example.test/products/%s/main.jpg", id),
			BasePriceCents:  int(faker.Price(12, 900) * 100),
			DiscountPct:     faker.Number(0, 25),
			Inventory:       randomInventory(faker),
			FreeShipping:    faker.Number(0, 1) == 1,
			Promotion:       promotionCode(faker, i),
			CheckoutWording: faker.Sentence(7),
			Version:         1,
			UpdatedAt:       fixedTime(0).Format(time.RFC3339),
		})
	}
	normalProductIDs := sortedProductIDs(products)
	retryProductID := normalProductIDs[0]
	if len(normalProductIDs) > 1 {
		retryProductID = normalProductIDs[1]
	}

	deadID := "sku-dead-001"
	products = append(products, product{
		ID:              deadID,
		Name:            faker.ProductName(),
		Description:     "Document intentionally rejected by the target search index.",
		ImageURL:        "invalid://dead-letter-image",
		BasePriceCents:  4299,
		DiscountPct:     10,
		Inventory:       map[string]int{"atl": 4, "den": 2, "sea": 1},
		FreeShipping:    true,
		Promotion:       "DEADLETTER",
		CheckoutWording: "This update demonstrates dead-letter behavior.",
		Version:         1,
		UpdatedAt:       fixedTime(0).Format(time.RFC3339),
	})

	if err := source.reset(ctx, products); err != nil {
		return nil, err
	}

	target, err := newSearchTarget(ctx, retryProductID, deadID)
	if err != nil {
		return nil, err
	}

	events := make([]mutation, 0, mutationCount+3)
	kinds := []string{"description", "image", "discount", "inventory", "free_shipping", "promotion", "checkout_wording"}
	for seq := 1; seq <= mutationCount; seq++ {
		productID := normalProductIDs[faker.Number(0, len(normalProductIDs)-1)]
		kind := kinds[(seq+faker.Number(0, len(kinds)-1))%len(kinds)]
		events = append(events, mutation{
			Seq:       seq,
			ActorID:   seq % writerCount,
			ProductID: productID,
			Kind:      kind,
			Value:     mutationValue(faker, kind, seq),
			At:        fixedTime(seq),
		})
	}
	events = append(events, mutation{
		Seq:       mutationCount + 1,
		ActorID:   1,
		ProductID: retryProductID,
		Kind:      "image",
		Value:     fmt.Sprintf("https://cdn.example.test/products/%s/retry-once.jpg", retryProductID),
		At:        fixedTime(mutationCount + 1),
	})
	events = append(events, mutation{
		Seq:       mutationCount + 2,
		ActorID:   3,
		ProductID: deadID,
		Kind:      "image",
		Value:     "invalid://dead-letter-image",
		At:        fixedTime(mutationCount + 2),
	})
	events = append(events, mutation{
		Seq:       mutationCount + 3,
		ActorID:   0,
		ProductID: "sku-ghost-001",
		Kind:      "ghost",
		At:        fixedTime(mutationCount + 3),
	})

	return &scenario{source: source, target: target, events: events}, nil
}

func runWriters(ctx context.Context, stores *playpg.Stores, source *catalogStore, events []mutation) (writerRunResult, error) {
	result := writerRunResult{Planned: len(events)}
	actorEvents := make([][]mutation, writerCount)
	for _, event := range events {
		if event.ActorID < 0 || event.ActorID >= len(actorEvents) {
			return result, fmt.Errorf("invalid actor id %d for WRITER_COUNT=%d", event.ActorID, writerCount)
		}
		actorEvents[event.ActorID] = append(actorEvents[event.ActorID], event)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ackCh := make(chan writerAck)
	var wg sync.WaitGroup
	for _, eventsForActor := range actorEvents {
		wg.Add(1)
		go func(eventsForActor []mutation) {
			defer wg.Done()
			for _, event := range eventsForActor {
				if runCtx.Err() != nil {
					return
				}
				err := applyAndEnqueue(runCtx, stores, source, event)
				ackCh <- writerAck{event: event, err: err}
				if err != nil {
					cancel()
					return
				}
			}
		}(eventsForActor)
	}

	go func() {
		wg.Wait()
		close(ackCh)
	}()

	var firstFailure writerAck
	for ack := range ackCh {
		if ack.err == nil {
			result.Enqueued++
			continue
		}
		if firstFailure.err == nil {
			firstFailure = ack
			cancel()
		}
	}

	if firstFailure.err != nil {
		return result, fmt.Errorf(
			"writer stage failed after %d/%d committed deltas (actor %s sequence %d): %w",
			result.Enqueued,
			result.Planned,
			actorName(firstFailure.event.ActorID),
			firstFailure.event.Seq,
			firstFailure.err,
		)
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("writer stage stopped after %d/%d committed deltas: %w", result.Enqueued, result.Planned, err)
	}
	return result, nil
}

func applyAndEnqueue(ctx context.Context, stores *playpg.Stores, source *catalogStore, event mutation) error {
	tx, err := stores.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if event.Kind != "ghost" {
		if err := source.applyMutation(ctx, tx, event); err != nil {
			return err
		}
	}

	_, err = stores.DeltaStore.EnqueueInTx(ctx, tx, playpg.NewDelta(
		syncID,
		projectionType,
		playpg.StringKey("product_id", event.ProductID),
		deltaflow.OriginOperationUpdated,
		event.At,
		map[string]any{
			"actor":    actorName(event.ActorID),
			"change":   event.Kind,
			"sequence": event.Seq,
		},
	))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func fixedTime(step int) time.Time {
	return time.Date(2026, time.June, 4, 15, 0, 0, 0, time.UTC).Add(time.Duration(step) * time.Second)
}
