package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	redisclient "github.com/redis/go-redis/v9"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

// CommandClient is the subset of a go-redis client required by Applier.
// *redis.Client, *redis.ClusterClient, and *redis.Ring satisfy this interface.
type CommandClient interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redisclient.StatusCmd
	Del(ctx context.Context, keys ...string) *redisclient.IntCmd
}

// KeyFunc maps a projection identity to an application-owned Redis key.
// Implementations must be deterministic: the same identity must resolve to the
// same key on every application attempt.
type KeyFunc func(deltaflow.ProjectionIdentity) (string, error)

type ApplierConfig struct {
	Client  CommandClient
	KeyFunc KeyFunc
	// TTL controls expiration for every successful upsert. Zero stores a
	// persistent key; a positive value refreshes expiration on each upsert.
	TTL time.Duration
}

type Applier struct {
	client  CommandClient
	keyFunc KeyFunc
	ttl     time.Duration
}

func NewApplier(cfg ApplierConfig) (*Applier, error) {
	if cfg.Client == nil {
		return nil, errors.New("redis applier: client is required")
	}
	if cfg.KeyFunc == nil {
		return nil, errors.New("redis applier: key func is required")
	}
	if cfg.TTL < 0 {
		return nil, errors.New("redis applier: ttl must be non-negative")
	}

	return &Applier{
		client:  cfg.Client,
		keyFunc: cfg.KeyFunc,
		ttl:     cfg.TTL,
	}, nil
}

func (a *Applier) Apply(ctx context.Context, op deltaflow.ProjectionOperation) error {
	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		if op.Projection == nil {
			return errors.New("redis applier: upsert operation requires projection")
		}
	case deltaflow.ProjectionOpDelete:
		// Delete intentionally permits a nil projection.
	default:
		return fmt.Errorf("redis applier: unsupported operation %q", op.Type)
	}

	key, err := a.keyFunc(op.Identity)
	if err != nil {
		return fmt.Errorf("redis applier: resolve key: %w", err)
	}
	if key == "" {
		return errors.New("redis applier: key func returned an empty key")
	}

	if op.Type == deltaflow.ProjectionOpUpsert {
		if err := a.client.Set(ctx, key, op.Projection.Payload, a.ttl).Err(); err != nil {
			return fmt.Errorf("redis applier: set key %q: %w", key, err)
		}
		return nil
	}
	if err := a.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis applier: delete key %q: %w", key, err)
	}
	return nil
}

var _ deltaflow.ProjectionApplier = (*Applier)(nil)
