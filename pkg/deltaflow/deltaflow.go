package deltaflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
)

type ProjectionType string
type ProjectionOperationType string

// ProjectionKey stores projection identity key components as JSON values.
// Its JSON encoding canonicalizes each embedded value before marshaling so the
// serialized form is stable for hashing and persistence.
type ProjectionKey map[string]json.RawMessage

func (k ProjectionKey) MarshalJSON() ([]byte, error) {
	canonical := make(map[string]any, len(k))
	for key, raw := range k {
		if raw == nil {
			canonical[key] = nil
			continue
		}

		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()

		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			if err == nil {
				return nil, errors.New("invalid JSON: multiple top-level values")
			}
			return nil, err
		}
		canonical[key] = value
	}

	return json.Marshal(canonical)
}

type ProjectionIdentity struct {
	Type ProjectionType
	Key  ProjectionKey
}

type Projection struct {
	Identity  ProjectionIdentity
	Payload   []byte
	MediaType string
	Checksum  string
}

const (
	ProjectionOpUpsert ProjectionOperationType = "upsert"
	ProjectionOpDelete ProjectionOperationType = "delete"
)

type ProjectionOperation struct {
	Type       ProjectionOperationType
	Identity   ProjectionIdentity
	Projection *Projection
}

type Projector interface {
	Project(ctx context.Context, identity ProjectionIdentity) (Projection, error)
}

type ProjectionApplier interface {
	Apply(ctx context.Context, op ProjectionOperation) error
}

type ProjectorFunc func(context.Context, ProjectionIdentity) (Projection, error)

func (f ProjectorFunc) Project(ctx context.Context, id ProjectionIdentity) (Projection, error) {
	return f(ctx, id)
}

type ProjectionApplierFunc func(context.Context, ProjectionOperation) error

func (f ProjectionApplierFunc) Apply(ctx context.Context, op ProjectionOperation) error {
	return f(ctx, op)
}

type Engine interface {
	Run(ctx context.Context) error
}

type DeltaStore interface {
	ClaimNext(ctx context.Context, workerID string, lockFor time.Duration) (*Delta, error)

	MarkSynced(ctx context.Context, deltaID string, ghostDetected bool) error

	MarkRetrying(ctx context.Context, deltaID string, err error, nextRunAt time.Time) error

	MarkDead(ctx context.Context, deltaID string, err error) error
}
