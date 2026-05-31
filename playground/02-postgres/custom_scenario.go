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

type ContactProfile struct {
	ContactID string `json:"contact_id"`
	FullName  string `json:"full_name"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
	UpdatedAt string `json:"updated_at"`
}

type contactSyncScenario struct {
	name           string
	source         *contactSourceStore
	target         *contactTargetIndex
	deltas         []deltaflow.Delta
	expectedGhosts int
}

type contactSourceStore struct {
	contacts map[string]ContactProfile
}

func (s *contactSourceStore) project(_ context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
	if identity.Type != deltaflow.ProjectionType("Contact") {
		return deltaflow.Projection{}, fmt.Errorf("unsupported projection type %q", identity.Type)
	}

	contactID, err := contactIDFromKey(identity.Key)
	if err != nil {
		return deltaflow.Projection{}, err
	}

	contact, ok := s.contacts[contactID]
	if !ok {
		return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
	}

	payload, err := json.Marshal(contact)
	if err != nil {
		return deltaflow.Projection{}, err
	}

	sum := sha256.Sum256(payload)
	return deltaflow.Projection{
		Identity:  identity,
		Payload:   payload,
		MediaType: "application/json",
		Checksum:  fmt.Sprintf("%x", sum[:]),
	}, nil
}

type contactTargetIndex struct {
	docs map[string][]byte
}

func (t *contactTargetIndex) apply(_ context.Context, op deltaflow.ProjectionOperation) error {
	contactID, err := contactIDFromKey(op.Identity.Key)
	if err != nil {
		return err
	}

	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		if op.Projection == nil {
			return errors.New("upsert operation requires projection payload")
		}
		t.docs[contactID] = append([]byte(nil), op.Projection.Payload...)
		return nil
	case deltaflow.ProjectionOpDelete:
		delete(t.docs, contactID)
		return nil
	default:
		return fmt.Errorf("unsupported operation %q", op.Type)
	}
}

func buildDemoScenario() contactSyncScenario {
	source := &contactSourceStore{contacts: map[string]ContactProfile{
		"con-001": {
			ContactID: "con-001",
			FullName:  "Ava Stone",
			Email:     "ava@example.com",
			Phone:     "+1-555-0101",
			UpdatedAt: "2026-05-31T10:00:00Z",
		},
		"con-002": {
			ContactID: "con-002",
			FullName:  "Leo Martin",
			Email:     "leo@example.com",
			Phone:     "+54-11-5555-0202",
			UpdatedAt: "2026-05-31T10:02:00Z",
		},
	}}

	target := &contactTargetIndex{docs: make(map[string][]byte, 4)}
	target.docs["con-999"] = []byte(`{"contact_id":"con-999","full_name":"Legacy Ghost","stale":true}`)

	syncID := deltaflow.SyncID("contacts-to-crm-cache")
	deltas := []deltaflow.Delta{
		makeContactDelta(syncID, "con-001", 1),
		makeContactDelta(syncID, "con-002", 2),
		makeContactDelta(syncID, "con-999", 3),
	}

	return contactSyncScenario{
		name:           "2 upserts + 1 ghost delete",
		source:         source,
		target:         target,
		deltas:         deltas,
		expectedGhosts: 1,
	}
}

func makeContactDelta(syncID deltaflow.SyncID, contactID string, n int) deltaflow.Delta {
	idRaw, err := json.Marshal(contactID)
	if err != nil {
		panic(err)
	}

	return deltaflow.Delta{
		SyncID:         syncID,
		Origin:         deltaflow.OriginOperationUpdated,
		ProjectionType: deltaflow.ProjectionType("Contact"),
		ProjectionKey: deltaflow.ProjectionKey{
			"contact_id": idRaw,
		},
		State:      deltaflow.DeltaPending,
		OccurredAt: fixedTime(n),
		CreatedAt:  fixedTime(n),
		Metadata: map[string]any{
			"example": "02-postgres",
		},
	}
}

func fixedTime(step int) time.Time {
	return time.Date(2026, time.May, 31, 10, step, 0, 0, time.UTC)
}

func contactIDFromKey(key deltaflow.ProjectionKey) (string, error) {
	raw, ok := key["contact_id"]
	if !ok {
		return "", errors.New("projection key is missing contact_id")
	}
	var contactID string
	if err := json.Unmarshal(raw, &contactID); err != nil {
		return "", fmt.Errorf("invalid contact_id in projection key: %w", err)
	}
	if contactID == "" {
		return "", errors.New("contact_id cannot be empty")
	}
	return contactID, nil
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
