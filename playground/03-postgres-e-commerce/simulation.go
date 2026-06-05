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
	target *searchIndexSimulator
	events []mutation
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

	target := newSearchIndexSimulator(
		map[string]bool{"sku-004": true},
		map[string]bool{deadID: true},
	)

	events := make([]mutation, 0, mutationCount+3)
	productIDs := sortedProductIDs(products)
	kinds := []string{"description", "image", "discount", "inventory", "free_shipping", "promotion", "checkout_wording"}
	for seq := 1; seq <= mutationCount; seq++ {
		productID := productIDs[faker.Number(0, len(productIDs)-2)]
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
		ProductID: "sku-004",
		Kind:      "image",
		Value:     "https://cdn.example.test/products/sku-004/retry-once.jpg",
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

func runWriters(ctx context.Context, stores *playpg.Stores, source *catalogStore, events []mutation) (int, error) {
	type request struct {
		event mutation
		ack   chan error
	}
	chans := make([]chan request, writerCount)
	for i := range chans {
		chans[i] = make(chan request)
	}

	var wg sync.WaitGroup
	for i := range chans {
		wg.Add(1)
		go func(ch <-chan request) {
			defer wg.Done()
			for req := range ch {
				err := applyAndEnqueue(ctx, stores, source, req.event)
				req.ack <- err
			}
		}(chans[i])
	}

	enqueued := 0
	for _, event := range events {
		ack := make(chan error, 1)
		chans[event.ActorID] <- request{event: event, ack: ack}
		if err := <-ack; err != nil {
			for _, ch := range chans {
				close(ch)
			}
			wg.Wait()
			return enqueued, err
		}
		enqueued++
	}

	for _, ch := range chans {
		close(ch)
	}
	wg.Wait()
	return enqueued, nil
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
