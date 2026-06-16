package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNewElasticsearchTargetUsesTimeoutClient(t *testing.T) {
	target, err := newElasticsearchTarget("http://localhost:9200", "products", "retry-001", "dead-001")
	if err != nil {
		t.Fatal(err)
	}
	if target.client.Timeout != 10*time.Second {
		t.Fatalf("Timeout = %s, want 10s", target.client.Timeout)
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
