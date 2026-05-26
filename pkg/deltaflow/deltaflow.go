package deltaflow

import (
	"context"
	"encoding/json"
)

type ProjectionType string
type ProjectionOperationType string

// ProjectionKey stores projection identity key components as JSON values so they
// can be serialized canonically for stable hashing and persistence.
type ProjectionKey map[string]json.RawMessage

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
