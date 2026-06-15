package elasticsearch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type DocumentIDFunc func(deltaflow.ProjectionIdentity) (string, error)

type ApplierConfig struct {
	Client     HTTPClient
	Endpoint   string
	Index      string
	DocumentID DocumentIDFunc
	Refresh    string
}

type Applier struct {
	client     HTTPClient
	endpoint   *url.URL
	index      string
	documentID DocumentIDFunc
	refresh    string
}

type ResponseError struct {
	Operation  deltaflow.ProjectionOperationType
	StatusCode int
	Retryable  bool
	Body       string
}

func (e *ResponseError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("elasticsearch %s failed with status %d", e.Operation, e.StatusCode)
	}
	return fmt.Sprintf("elasticsearch %s failed with status %d: %s", e.Operation, e.StatusCode, e.Body)
}

func NewApplier(cfg ApplierConfig) (*Applier, error) {
	if cfg.Client == nil {
		cfg.Client = http.DefaultClient
	}
	if cfg.Endpoint == "" {
		return nil, errors.New("elasticsearch applier: endpoint is required")
	}
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch applier: invalid endpoint: %w", err)
	}
	if endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, errors.New("elasticsearch applier: endpoint must include scheme and host")
	}
	if cfg.Index == "" {
		return nil, errors.New("elasticsearch applier: index is required")
	}
	documentID := cfg.DocumentID
	if documentID == nil {
		documentID = DefaultDocumentID
	}
	return &Applier{
		client:     cfg.Client,
		endpoint:   endpoint,
		index:      cfg.Index,
		documentID: documentID,
		refresh:    cfg.Refresh,
	}, nil
}

func (a *Applier) Apply(ctx context.Context, op deltaflow.ProjectionOperation) error {
	documentID, err := a.documentID(op.Identity)
	if err != nil {
		return err
	}
	if documentID == "" {
		return errors.New("elasticsearch applier: document id cannot be empty")
	}

	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		if op.Projection == nil {
			return errors.New("elasticsearch applier: upsert operation requires projection")
		}
		return a.applyUpsert(ctx, op, documentID)
	case deltaflow.ProjectionOpDelete:
		return a.applyDelete(ctx, op, documentID)
	default:
		return fmt.Errorf("elasticsearch applier: unsupported operation %q", op.Type)
	}
}

func (a *Applier) applyUpsert(ctx context.Context, op deltaflow.ProjectionOperation, documentID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, a.documentURL(documentID), bytes.NewReader(op.Projection.Payload))
	if err != nil {
		return err
	}
	contentType := op.Projection.MediaType
	if contentType == "" {
		contentType = "application/json"
	}
	req.Header.Set("Content-Type", contentType)
	return a.do(req, op.Type)
}

func (a *Applier) applyDelete(ctx context.Context, op deltaflow.ProjectionOperation, documentID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, a.documentURL(documentID), nil)
	if err != nil {
		return err
	}
	return a.do(req, op.Type)
}

func (a *Applier) do(req *http.Request, opType deltaflow.ProjectionOperationType) error {
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if opType == deltaflow.ProjectionOpDelete && resp.StatusCode == http.StatusNotFound && isMissingDocumentDelete(body) {
		return nil
	}
	return &ResponseError{
		Operation:  opType,
		StatusCode: resp.StatusCode,
		Retryable:  isRetryableStatus(resp.StatusCode),
		Body:       strings.TrimSpace(string(body)),
	}
}

func isMissingDocumentDelete(body []byte) bool {
	var payload struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	return payload.Result == "not_found"
}

func (a *Applier) documentURL(documentID string) string {
	u := *a.endpoint
	base := strings.TrimRight(u.Path, "/")
	u.Path = base + "/" + url.PathEscape(a.index) + "/_doc/" + url.PathEscape(documentID)
	query := u.Query()
	if a.refresh != "" {
		query.Set("refresh", a.refresh)
	}
	u.RawQuery = query.Encode()
	return u.String()
}

func DefaultDocumentID(identity deltaflow.ProjectionIdentity) (string, error) {
	encoded, err := json.Marshal(identity.Key)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(string(identity.Type) + "\x00" + string(encoded)))
	return hex.EncodeToString(sum[:]), nil
}

func isRetryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status >= 500
}
