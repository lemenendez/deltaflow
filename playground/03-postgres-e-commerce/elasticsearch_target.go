package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	es "github.com/lemenendez/deltaflow/pkg/connectors/elasticsearch"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	"github.com/lemenendez/deltaflow/playground/internal/playpg"
)

const elasticsearchIndex = "deltaflow_products"
const elasticsearchHTTPTimeout = 10 * time.Second

func newSearchTarget(ctx context.Context, retryProductID, deadID string) (searchTarget, error) {
	if elasticsearchEndpoint == "" {
		return newSearchIndexSimulator(
			map[string]bool{retryProductID: true},
			map[string]bool{deadID: true},
		), nil
	}

	target, err := newElasticsearchTarget(elasticsearchEndpoint, elasticsearchIndex, retryProductID, deadID)
	if err != nil {
		return nil, err
	}
	if err := target.reset(ctx); err != nil {
		return nil, err
	}
	return target, nil
}

type elasticsearchTarget struct {
	client   *http.Client
	endpoint string
	index    string
	applier  *es.Applier

	mu          sync.Mutex
	failOnce    map[string]bool
	deadLetters map[string]bool
	upserts     int
	deletes     int
	failures    int
}

func newElasticsearchTarget(endpoint, index, retryProductID, deadID string) (*elasticsearchTarget, error) {
	client := &http.Client{Timeout: elasticsearchHTTPTimeout}
	applier, err := es.NewApplier(es.ApplierConfig{
		Client:   client,
		Endpoint: endpoint,
		Index:    index,
		DocumentID: func(identity deltaflow.ProjectionIdentity) (string, error) {
			return playpg.StringFromKey(identity.Key, "product_id")
		},
		Refresh: "wait_for",
	})
	if err != nil {
		return nil, err
	}
	return &elasticsearchTarget{
		client:      client,
		endpoint:    strings.TrimRight(endpoint, "/"),
		index:       index,
		applier:     applier,
		failOnce:    map[string]bool{retryProductID: true},
		deadLetters: map[string]bool{deadID: true},
	}, nil
}

func (t *elasticsearchTarget) reset(ctx context.Context) error {
	deleteReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, t.indexURL(), nil)
	if err != nil {
		return err
	}
	deleteResp, err := t.client.Do(deleteReq)
	if err != nil {
		return err
	}
	if err := closeResponse(deleteResp); err != nil && deleteResp.StatusCode != http.StatusNotFound {
		return err
	}

	createReq, err := http.NewRequestWithContext(ctx, http.MethodPut, t.indexURL(), strings.NewReader(indexMapping))
	if err != nil {
		return err
	}
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := t.client.Do(createReq)
	if err != nil {
		return err
	}
	if err := closeResponse(createResp); err != nil {
		return err
	}

	return t.seedGhost(ctx)
}

func (t *elasticsearchTarget) seedGhost(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, t.documentURL("sku-ghost-001"), bytes.NewReader([]byte(`{"product_id":"sku-ghost-001","stale":true}`)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	return closeResponse(resp)
}

func (t *elasticsearchTarget) Apply(ctx context.Context, op deltaflow.ProjectionOperation) error {
	productID, err := playpg.StringFromKey(op.Identity.Key, "product_id")
	if err != nil {
		return err
	}

	t.mu.Lock()
	if op.Type == deltaflow.ProjectionOpUpsert {
		if t.deadLetters[productID] {
			t.failures++
			t.mu.Unlock()
			return fmt.Errorf("elasticsearch rejected product %s: invalid image payload", productID)
		}
		if t.failOnce[productID] {
			delete(t.failOnce, productID)
			t.failures++
			t.mu.Unlock()
			return fmt.Errorf("elasticsearch temporary 429 for product %s", productID)
		}
	}
	t.mu.Unlock()

	if err := t.applier.Apply(ctx, op); err != nil {
		t.mu.Lock()
		t.failures++
		t.mu.Unlock()
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		t.upserts++
	case deltaflow.ProjectionOpDelete:
		t.deletes++
	}
	return nil
}

func (t *elasticsearchTarget) snapshot(ctx context.Context) (map[string][]byte, int, int, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.indexURL()+"/_search?size=10000", nil)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, 0, 0, 0, fmt.Errorf("elasticsearch snapshot failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Hits struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, 0, 0, 0, err
	}
	docs := make(map[string][]byte, len(payload.Hits.Hits))
	for _, hit := range payload.Hits.Hits {
		docs[hit.ID] = append([]byte(nil), hit.Source...)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	return docs, t.upserts, t.deletes, t.failures, nil
}

func (t *elasticsearchTarget) indexURL() string {
	return t.endpoint + "/" + url.PathEscape(t.index)
}

func (t *elasticsearchTarget) documentURL(documentID string) string {
	return t.indexURL() + "/_doc/" + url.PathEscape(documentID) + "?refresh=wait_for"
}

func closeResponse(resp *http.Response) error {
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_, _ = io.Copy(io.Discard, resp.Body)
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = resp.Status
	}
	return errors.New(message)
}

const indexMapping = `{
  "mappings": {
    "properties": {
      "product_id": { "type": "keyword" },
      "name": { "type": "text" },
      "description": { "type": "text" },
      "image_url": { "type": "keyword" },
      "base_price_cents": { "type": "integer" },
      "discount_pct": { "type": "integer" },
      "sale_price_cents": { "type": "integer" },
      "inventory_total": { "type": "integer" },
      "available": { "type": "boolean" },
      "free_shipping": { "type": "boolean" },
      "promotion": { "type": "keyword" },
      "checkout_wording": { "type": "text" },
      "version": { "type": "integer" },
      "updated_at": { "type": "date" }
    }
  }
}`
