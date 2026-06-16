package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	"github.com/lemenendez/deltaflow/playground/internal/playpg"
)

func TestNewElasticsearchCRMTargetUsesTimeoutClient(t *testing.T) {
	target, err := newElasticsearchCRMTarget("http://localhost:9200", "crm", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if target.client.Timeout != 10*time.Second {
		t.Fatalf("Timeout = %s, want 10s", target.client.Timeout)
	}
}

func TestElasticsearchCRMTargetRetryErrorNamesElasticsearch(t *testing.T) {
	target, err := newElasticsearchCRMTarget(
		"http://localhost:9200",
		"crm",
		map[string]bool{"CRMCustomerView/cus-001": true},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = target.Apply(context.Background(), customerUpsertOperation(t, "cus-001"))
	if err == nil {
		t.Fatal("err = nil, want retry error")
	}
	if !strings.Contains(err.Error(), "elasticsearch temporary timeout") {
		t.Fatalf("error = %q, want Elasticsearch retry message", err.Error())
	}
}

func TestCloseResponseDrainsSuccessfulBody(t *testing.T) {
	body := &trackingReadCloser{reader: strings.NewReader(`{"acknowledged":true}`)}
	err := closeResponse(&http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       body,
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

func TestCloseResponseUsesStatusWhenBodyIsEmpty(t *testing.T) {
	err := closeResponse(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Status:     "500 Internal Server Error",
		Body:       io.NopCloser(strings.NewReader("")),
	})
	if err == nil {
		t.Fatal("err = nil, want error")
	}
	if err.Error() != "500 Internal Server Error" {
		t.Fatalf("error = %q, want status text", err.Error())
	}
}

func TestCloseResponseUsesBodyWhenPresent(t *testing.T) {
	err := closeResponse(&http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     "400 Bad Request",
		Body:       io.NopCloser(strings.NewReader("  mapping rejected  ")),
	})
	if err == nil {
		t.Fatal("err = nil, want error")
	}
	if err.Error() != "mapping rejected" {
		t.Fatalf("error = %q, want body text", err.Error())
	}
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

func customerUpsertOperation(t *testing.T, id string) deltaflow.ProjectionOperation {
	t.Helper()
	key := playpg.StringKey("id", id)
	return deltaflow.ProjectionOperation{
		Type:     deltaflow.ProjectionOpUpsert,
		Identity: deltaflow.ProjectionIdentity{Type: custProjection, Key: key},
		Projection: &deltaflow.Projection{
			Payload:   []byte(`{"customer":{"id":"cus-001"}}`),
			MediaType: "application/json",
		},
	}
}
