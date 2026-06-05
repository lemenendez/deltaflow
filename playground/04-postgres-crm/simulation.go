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
	Seq        int
	ActorID    int
	Entity     string
	EntityID   string
	Kind       string
	Value      any
	At         time.Time
	Projection deltaflow.ProjectionType
}

type scenario struct {
	source *crmStore
	target *crmTargetSimulator
	events []mutation
}

func buildScenario(ctx context.Context, stores *playpg.Stores) (*scenario, error) {
	faker := gofakeit.New(seed)
	source := newCRMStore(stores.DB)
	if err := source.ensureSchema(ctx); err != nil {
		return nil, err
	}

	users := make([]user, 0, userCount)
	for i := 1; i <= userCount; i++ {
		id := fmt.Sprintf("usr-%03d", i)
		users = append(users, user{
			ID:        id,
			Name:      faker.Name(),
			Roles:     rolesFor(faker, i),
			Version:   1,
			UpdatedAt: fixedTime(0).Format(time.RFC3339),
		})
	}

	customers := make([]customer, 0, customerCount)
	for i := 1; i <= customerCount; i++ {
		id := fmt.Sprintf("cus-%03d", i)
		customers = append(customers, customer{
			ID:        id,
			Name:      faker.Name(),
			Email:     faker.Email(),
			Phone:     faker.Phone(),
			VIP:       faker.Number(0, 5) == 0,
			Version:   1,
			UpdatedAt: fixedTime(0).Format(time.RFC3339),
		})
	}

	userIDs := sortedKeys(users)
	customerIDs := sortedKeys(customers)
	orders := make([]order, 0, orderCount+1)
	for i := 1; i <= orderCount; i++ {
		id := fmt.Sprintf("ord-%03d", i)
		orders = append(orders, order{
			ID:         id,
			CustomerID: customerIDs[faker.Number(0, len(customerIDs)-1)],
			UserID:     userIDs[faker.Number(0, len(userIDs)-1)],
			TotalCents: faker.Number(1200, 98000),
			Status:     statusFor(i),
			Version:    1,
			UpdatedAt:  fixedTime(0).Format(time.RFC3339),
		})
	}
	deadOrderID := "ord-dead-001"
	orders = append(orders, order{
		ID:         deadOrderID,
		CustomerID: customerIDs[0],
		UserID:     userIDs[0],
		TotalCents: 9900,
		Status:     "accepted",
		Version:    1,
		UpdatedAt:  fixedTime(0).Format(time.RFC3339),
	})

	if err := source.reset(ctx, users, customers, orders); err != nil {
		return nil, err
	}

	retryCustomerID := customerIDs[0]
	if len(customerIDs) > 1 {
		retryCustomerID = customerIDs[1]
	}

	target := newCRMTargetSimulator(
		map[string]bool{string(custProjection) + "/" + retryCustomerID: true},
		map[string]bool{string(orderProjection) + "/" + deadOrderID: true},
	)

	events := make([]mutation, 0, mutationCount+3)
	orderIDs := sortedKeys(orders)
	entityCycle := []string{"user", "customer", "order", "customer", "order"}
	for seq := 1; seq <= mutationCount; seq++ {
		entity := entityCycle[(seq+faker.Number(0, len(entityCycle)-1))%len(entityCycle)]
		m := mutation{
			Seq:     seq,
			ActorID: seq % writerCount,
			Entity:  entity,
			At:      fixedTime(seq),
		}
		switch entity {
		case "user":
			m.EntityID = userIDs[faker.Number(0, len(userIDs)-1)]
			m.Projection = userProjection
			if seq%2 == 0 {
				m.Kind = "roles"
				m.Value = rolesFor(faker, seq)
			} else {
				m.Kind = "name"
				m.Value = faker.Name()
			}
		case "customer":
			m.EntityID = customerIDs[faker.Number(0, len(customerIDs)-1)]
			m.Projection = custProjection
			switch seq % 3 {
			case 0:
				m.Kind = "email"
				m.Value = faker.Email()
			case 1:
				m.Kind = "phone"
				m.Value = faker.Phone()
			default:
				m.Kind = "vip"
				m.Value = faker.Number(0, 1) == 1
			}
		case "order":
			m.EntityID = orderIDs[faker.Number(0, len(orderIDs)-2)]
			m.Projection = orderProjection
			if seq%2 == 0 {
				m.Kind = "status"
				m.Value = statusFor(seq)
			} else {
				m.Kind = "total"
				m.Value = faker.Number(1200, 120000)
			}
		}
		events = append(events, m)
	}
	events = append(events, mutation{
		Seq:        mutationCount + 1,
		ActorID:    2,
		Entity:     "customer",
		EntityID:   retryCustomerID,
		Kind:       "phone",
		Value:      faker.Phone(),
		Projection: custProjection,
		At:         fixedTime(mutationCount + 1),
	})
	events = append(events, mutation{
		Seq:        mutationCount + 2,
		ActorID:    3,
		Entity:     "order",
		EntityID:   deadOrderID,
		Kind:       "status",
		Value:      "accepted",
		Projection: orderProjection,
		At:         fixedTime(mutationCount + 2),
	})
	events = append(events, mutation{
		Seq:        mutationCount + 3,
		ActorID:    0,
		Entity:     "customer",
		EntityID:   "cus-ghost-001",
		Kind:       "ghost",
		Projection: custProjection,
		At:         fixedTime(mutationCount + 3),
	})

	return &scenario{source: source, target: target, events: events}, nil
}

func runWriters(ctx context.Context, stores *playpg.Stores, source *crmStore, events []mutation) (int, error) {
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
	type pendingAck struct {
		idx int
		ack chan error
	}
	pending := make([]pendingAck, 0, len(events))
	for _, event := range events {
		if event.ActorID < 0 || event.ActorID >= len(chans) {
			for _, ch := range chans {
				close(ch)
			}
			wg.Wait()
			return enqueued, fmt.Errorf("invalid actor id %d for WRITER_COUNT=%d", event.ActorID, writerCount)
		}
		ack := make(chan error, 1)
		chans[event.ActorID] <- request{event: event, ack: ack}
		pending = append(pending, pendingAck{idx: enqueued, ack: ack})
		enqueued++
	}

	for _, p := range pending {
		if err := <-p.ack; err != nil {
			for _, ch := range chans {
				close(ch)
			}
			wg.Wait()
			return p.idx, err
		}
	}

	for _, ch := range chans {
		close(ch)
	}
	wg.Wait()
	return enqueued, nil
}

func applyAndEnqueue(ctx context.Context, stores *playpg.Stores, source *crmStore, event mutation) error {
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

	origin := deltaflow.OriginOperationUpdated
	if event.Entity == "order" && event.Kind == "status" && event.Value == "accepted" {
		origin = deltaflow.OriginOperationInserted
	}
	_, err = stores.DeltaStore.EnqueueInTx(ctx, tx, playpg.NewDelta(
		syncID,
		event.Projection,
		playpg.StringKey("id", event.EntityID),
		origin,
		event.At,
		map[string]any{
			"actor":    actorName(event.ActorID),
			"entity":   event.Entity,
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
	return time.Date(2026, time.June, 4, 16, 0, 0, 0, time.UTC).Add(time.Duration(step) * time.Second)
}
