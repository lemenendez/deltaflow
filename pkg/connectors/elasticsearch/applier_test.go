package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestApplierUpsertMapsProjectionToIndexRequest(t *testing.T) {
	var gotMethod, gotPath, gotBody, gotContentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.String()
		gotContentType = r.Header.Get("Content-Type")
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		gotBody = string(body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	applier, err := NewApplier(ApplierConfig{
		Client:   server.Client(),
		Endpoint: server.URL,
		Index:    "products",
		DocumentID: func(deltaflow.ProjectionIdentity) (string, error) {
			return "sku-001", nil
		},
		Refresh: "wait_for",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = applier.Apply(context.Background(), deltaflow.ProjectionOperation{
		Type:     deltaflow.ProjectionOpUpsert,
		Identity: identity(t, "sku-001"),
		Projection: &deltaflow.Projection{
			Payload:   []byte(`{"product_id":"sku-001"}`),
			MediaType: "application/json",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPut {
		t.Fatalf("method = %s, want %s", gotMethod, http.MethodPut)
	}
	if gotPath != "/products/_doc/sku-001?refresh=wait_for" {
		t.Fatalf("path = %s", gotPath)
	}
	if gotContentType != "application/json" {
		t.Fatalf("content-type = %s", gotContentType)
	}
	if gotBody != `{"product_id":"sku-001"}` {
		t.Fatalf("body = %s", gotBody)
	}
}

func TestApplierDeleteTreatsMissingDocumentAsSuccess(t *testing.T) {
	var gotMethod, gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	applier, err := NewApplier(ApplierConfig{
		Client:   server.Client(),
		Endpoint: server.URL,
		Index:    "products",
		DocumentID: func(deltaflow.ProjectionIdentity) (string, error) {
			return "sku-ghost-001", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = applier.Apply(context.Background(), deltaflow.ProjectionOperation{
		Type:     deltaflow.ProjectionOpDelete,
		Identity: identity(t, "sku-ghost-001"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodDelete {
		t.Fatalf("method = %s, want %s", gotMethod, http.MethodDelete)
	}
	if gotPath != "/products/_doc/sku-ghost-001" {
		t.Fatalf("path = %s", gotPath)
	}
}

func TestApplierClassifiesRetryableResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	}))
	defer server.Close()

	applier, err := NewApplier(ApplierConfig{
		Client:   server.Client(),
		Endpoint: server.URL,
		Index:    "products",
	})
	if err != nil {
		t.Fatal(err)
	}

	err = applier.Apply(context.Background(), deltaflow.ProjectionOperation{
		Type:       deltaflow.ProjectionOpUpsert,
		Identity:   identity(t, "sku-001"),
		Projection: &deltaflow.Projection{Payload: []byte(`{}`)},
	})
	var responseErr *ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error = %T, want ResponseError", err)
	}
	if !responseErr.Retryable {
		t.Fatal("Retryable = false, want true")
	}
}

func TestDefaultDocumentIDIsStableForCanonicalProjectionKey(t *testing.T) {
	a := identity(t, "9007199254740993")
	b := deltaflow.ProjectionIdentity{
		Type: "Product",
		Key: deltaflow.ProjectionKey{
			"product_id": json.RawMessage(`"9007199254740993"`),
		},
	}

	first, err := DefaultDocumentID(a)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DefaultDocumentID(b)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("document IDs differ: %s != %s", first, second)
	}
}

func identity(t *testing.T, productID string) deltaflow.ProjectionIdentity {
	t.Helper()
	raw, err := json.Marshal(productID)
	if err != nil {
		t.Fatal(err)
	}
	return deltaflow.ProjectionIdentity{
		Type: "Product",
		Key:  deltaflow.ProjectionKey{"product_id": raw},
	}
}
