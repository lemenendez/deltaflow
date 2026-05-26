package deltaflow

import "context"

type ProjectionType string
type ProjectionOperationType string

type ProjectionIdentity struct {
	Type ProjectionType
	Key  map[string]any
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
