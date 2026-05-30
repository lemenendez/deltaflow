package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type CustomerProfile struct {
	CustomerID      string `json:"customer_id"`
	FullName        string `json:"full_name"`
	Email           string `json:"email"`
	Country         string `json:"country"`
	Segment         string `json:"segment"`
	NewsletterOptIn bool   `json:"newsletter_opt_in"`
	UpdatedAt       string `json:"updated_at"`
}

type customerCacheScenario struct {
	name       string
	source     *sourceStore
	target     *targetIndex
	deltas     []deltaflow.Delta
	ghostCount int
}

type sourceStore struct {
	customers map[string]CustomerProfile
}

func (s *sourceStore) project(_ context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
	if identity.Type != deltaflow.ProjectionType("Customer") {
		return deltaflow.Projection{}, fmt.Errorf("unsupported projection type %q", identity.Type)
	}

	customerID, err := customerIDFromKey(identity.Key)
	if err != nil {
		return deltaflow.Projection{}, err
	}

	customer, ok := s.customers[customerID]
	if !ok {
		return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
	}

	payload, err := json.Marshal(customer)
	if err != nil {
		return deltaflow.Projection{}, err
	}

	checksum := sha256.Sum256(payload)
	return deltaflow.Projection{
		Identity:  identity,
		Payload:   payload,
		MediaType: "application/json",
		Checksum:  fmt.Sprintf("%x", checksum[:]),
	}, nil
}

type targetIndex struct {
	docs map[string][]byte
}

func (t *targetIndex) apply(_ context.Context, op deltaflow.ProjectionOperation) error {
	customerID, err := customerIDFromKey(op.Identity.Key)
	if err != nil {
		return err
	}

	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		if op.Projection == nil {
			return errors.New("upsert operation requires a projection payload")
		}
		t.docs[customerID] = append([]byte(nil), op.Projection.Payload...)
		return nil
	case deltaflow.ProjectionOpDelete:
		delete(t.docs, customerID)
		return nil
	default:
		return fmt.Errorf("unsupported operation %q", op.Type)
	}
}

func buildDemoScenario() customerCacheScenario {
	source := &sourceStore{customers: map[string]CustomerProfile{
		"cus-001": {
			CustomerID:      "cus-001",
			FullName:        "Ava Stone",
			Email:           "ava@example.com",
			Country:         "US",
			Segment:         "customer",
			NewsletterOptIn: true,
			UpdatedAt:       "2026-05-30T10:00:00Z",
		},
		"cus-002": {
			CustomerID:      "cus-002",
			FullName:        "Leo Martin",
			Email:           "leo@example.com",
			Country:         "AR",
			Segment:         "vip",
			NewsletterOptIn: false,
			UpdatedAt:       "2026-05-30T10:05:00Z",
		},
		"cus-003": {
			CustomerID:      "cus-003",
			FullName:        "Nia Gomez",
			Email:           "nia@example.com",
			Country:         "MX",
			Segment:         "prospect",
			NewsletterOptIn: true,
			UpdatedAt:       "2026-05-30T10:10:00Z",
		},
	}}
	target := &targetIndex{docs: make(map[string][]byte, 4)}
	syncID := deltaflow.SyncID("customers-to-web-cache")
	deltas := []deltaflow.Delta{
		makeCustomerDelta(syncID, "cus-001", 1),
		makeCustomerDelta(syncID, "cus-002", 2),
		makeCustomerDelta(syncID, "cus-003", 3),
		makeCustomerDelta(syncID, "cus-999", 4),
	}

	ghostPayload := []byte(`{"customer_id":"cus-999","full_name":"Legacy Ghost","stale":true}`)
	target.docs["cus-999"] = ghostPayload

	return customerCacheScenario{
		name:       "3 upserts + 1 ghost delete",
		source:     source,
		target:     target,
		deltas:     deltas,
		ghostCount: 1,
	}
}

func makeCustomerDelta(syncID deltaflow.SyncID, customerID string, n int) deltaflow.Delta {
	idRaw, err := json.Marshal(customerID)
	if err != nil {
		panic(err)
	}

	return deltaflow.Delta{
		ID:             deltaflow.DeltaID(fmt.Sprintf("delta-%06d", n)),
		SyncID:         syncID,
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: deltaflow.ProjectionType("Customer"),
		ProjectionKey: deltaflow.ProjectionKey{
			"customer_id": idRaw,
		},
		State:      deltaflow.DeltaPending,
		OccurredAt: fixedTime(n),
		CreatedAt:  fixedTime(n),
		Metadata: map[string]any{
			"example": "01-in-memory",
		},
	}
}

func fixedTime(step int) time.Time {
	return time.Date(2026, time.May, 30, 10, step, 0, 0, time.UTC)
}

func customerIDFromKey(key deltaflow.ProjectionKey) (string, error) {
	raw, ok := key["customer_id"]
	if !ok {
		return "", errors.New("projection key is missing customer_id")
	}
	var customerID string
	if err := json.Unmarshal(raw, &customerID); err != nil {
		return "", fmt.Errorf("invalid customer_id in projection key: %w", err)
	}
	if customerID == "" {
		return "", errors.New("customer_id cannot be empty")
	}
	return customerID, nil
}

func printOneSample(indexed map[string][]byte) {
	if len(indexed) == 0 {
		return
	}

	keys := make([]string, 0, len(indexed))
	for k := range indexed {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf("sample=%s => %s\n", keys[0], indexed[keys[0]])
}
