package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestNewApplierUsesTimeoutDefaultClient(t *testing.T) {
	applier, err := NewApplier(ApplierConfig{
		Endpoint: "http://localhost:9200",
		Index:    "products",
	})
	if err != nil {
		t.Fatal(err)
	}

	client, ok := applier.client.(*http.Client)
	if !ok {
		t.Fatalf("client = %T, want *http.Client", applier.client)
	}
	if client.Timeout != 10*time.Second {
		t.Fatalf("Timeout = %s, want 10s", client.Timeout)
	}
}

func TestNewApplierUsesConfiguredClient(t *testing.T) {
	configured := &http.Client{Timeout: 250 * time.Millisecond}
	applier, err := NewApplier(ApplierConfig{
		Client:   configured,
		Endpoint: "http://localhost:9200",
		Index:    "products",
	})
	if err != nil {
		t.Fatal(err)
	}
	if applier.client != configured {
		t.Fatal("applier did not keep configured client")
	}
}

func TestApplierUpsertMapsProjectionToIndexRequest(t *testing.T) {
	var gotMethod, gotPath, gotBody, gotContentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.String()
		gotContentType = r.Header.Get("Content-Type")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
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

func TestApplierDrainsSuccessfulResponseBody(t *testing.T) {
	body := &trackingReadCloser{reader: strings.NewReader(`{"result":"updated"}`)}
	applier, err := NewApplier(ApplierConfig{
		Client: staticResponseClient{response: &http.Response{
			StatusCode: http.StatusOK,
			Body:       body,
		}},
		Endpoint: "http://localhost:9200",
		Index:    "products",
		DocumentID: func(deltaflow.ProjectionIdentity) (string, error) {
			return "sku-001", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = applier.Apply(context.Background(), deltaflow.ProjectionOperation{
		Type:     deltaflow.ProjectionOpDelete,
		Identity: identity(t, "sku-001"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !body.readEOF {
		t.Fatal("successful response body was not drained")
	}
	if !body.closed {
		t.Fatal("successful response body was not closed")
	}
}

func TestApplierDeleteTreatsMissingDocumentAsSuccess(t *testing.T) {
	var gotMethod, gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"_index":"products","_id":"sku-ghost-001","result":"not_found"}`))
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

func TestApplierDeleteReturnsResponseErrorForMissingIndex(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"type":"index_not_found_exception","reason":"no such index [products]"},"status":404}`))
	}))
	defer server.Close()

	applier, err := NewApplier(ApplierConfig{
		Client:   server.Client(),
		Endpoint: server.URL,
		Index:    "products",
		DocumentID: func(deltaflow.ProjectionIdentity) (string, error) {
			return "sku-001", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = applier.Apply(context.Background(), deltaflow.ProjectionOperation{
		Type:     deltaflow.ProjectionOpDelete,
		Identity: identity(t, "sku-001"),
	})
	var responseErr *ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error = %T, want ResponseError", err)
	}
	if responseErr.StatusCode != http.StatusNotFound {
		t.Fatalf("StatusCode = %d, want %d", responseErr.StatusCode, http.StatusNotFound)
	}
	if responseErr.Retryable {
		t.Fatal("Retryable = true, want false")
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

type staticResponseClient struct {
	response *http.Response
	err      error
}

func (c staticResponseClient) Do(*http.Request) (*http.Response, error) {
	return c.response, c.err
}

type trackingReadCloser struct {
	reader  *strings.Reader
	readEOF bool
	closed  bool
}

func (b *trackingReadCloser) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err == io.EOF {
		b.readEOF = true
	}
	return n, err
}

func (b *trackingReadCloser) Close() error {
	b.closed = true
	return nil
}
