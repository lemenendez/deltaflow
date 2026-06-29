//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	redisconnector "github.com/lemenendez/deltaflow/pkg/connectors/redis"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestRedisApplierContainerContract(t *testing.T) {
	if os.Getenv("DELTAFLOW_IT_REDIS_ENABLE") != "1" {
		t.Skip("set DELTAFLOW_IT_REDIS_ENABLE=1 to run Redis-compatible connector integration tests")
	}

	ctx := context.Background()
	image := os.Getenv("DELTAFLOW_IT_REDIS_IMAGE")
	if image == "" {
		image = "redis:8.0"
	}

	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start %s container: %v", image, err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: host + ":" + port.Port()})
	t.Cleanup(func() { _ = client.Close() })
	if err := waitForRedis(ctx, client); err != nil {
		t.Fatalf("wait for %s: %v", image, err)
	}

	keyFunc := func(identity deltaflow.ProjectionIdentity) (string, error) {
		raw, ok := identity.Key["contact_id"]
		if !ok {
			return "", fmt.Errorf("contact_id is required")
		}
		var id string
		if err := json.Unmarshal(raw, &id); err != nil {
			return "", err
		}
		return "deltaflow-it:contact:" + id, nil
	}

	t.Run("persistent binary replacement", func(t *testing.T) {
		applier := newRedisIntegrationApplier(t, client, keyFunc, 0)
		identity := redisContactIdentity("persistent")
		key, _ := keyFunc(identity)
		if err := client.Set(ctx, key, "old", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		payload := []byte{0x00, 0xff, 'n', 'e', 'w'}
		applyRedisUpsert(t, ctx, applier, identity, payload)

		got, err := client.Get(ctx, key).Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("payload = %v, want %v", got, payload)
		}
		if ttl, err := client.TTL(ctx, key).Result(); err != nil || ttl != -1 {
			t.Fatalf("TTL = %s, err = %v, want persistent", ttl, err)
		}
	})

	t.Run("expiring upsert replaces deadline", func(t *testing.T) {
		applier := newRedisIntegrationApplier(t, client, keyFunc, 2*time.Second)
		identity := redisContactIdentity("expiring")
		key, _ := keyFunc(identity)
		if err := client.Set(ctx, key, "old", 100*time.Millisecond).Err(); err != nil {
			t.Fatal(err)
		}
		applyRedisUpsert(t, ctx, applier, identity, []byte("latest"))

		ttl, err := client.PTTL(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if ttl <= time.Second || ttl > 2*time.Second {
			t.Fatalf("PTTL = %s, want refreshed deadline near 2s", ttl)
		}
	})

	t.Run("short ttl expires", func(t *testing.T) {
		applier := newRedisIntegrationApplier(t, client, keyFunc, 50*time.Millisecond)
		identity := redisContactIdentity("short-lived")
		key, _ := keyFunc(identity)
		applyRedisUpsert(t, ctx, applier, identity, []byte("temporary"))

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := client.Get(ctx, key).Err(); err == redisclient.Nil {
				return
			} else if err != nil {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("key did not expire")
	})

	t.Run("delete existing and missing", func(t *testing.T) {
		applier := newRedisIntegrationApplier(t, client, keyFunc, 0)
		identity := redisContactIdentity("deleted")
		key, _ := keyFunc(identity)
		if err := client.Set(ctx, key, "value", 0).Err(); err != nil {
			t.Fatal(err)
		}
		op := deltaflow.ProjectionOperation{Type: deltaflow.ProjectionOpDelete, Identity: identity}
		if err := applier.Apply(ctx, op); err != nil {
			t.Fatal(err)
		}
		if err := applier.Apply(ctx, op); err != nil {
			t.Fatalf("repeat delete: %v", err)
		}
		if err := client.Get(ctx, key).Err(); err != redisclient.Nil {
			t.Fatalf("GET after delete error = %v, want redis.Nil", err)
		}
	})
}

func newRedisIntegrationApplier(t *testing.T, client *redisclient.Client, keyFunc redisconnector.KeyFunc, ttl time.Duration) *redisconnector.Applier {
	t.Helper()
	applier, err := redisconnector.NewApplier(redisconnector.ApplierConfig{
		Client:  client,
		KeyFunc: keyFunc,
		TTL:     ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	return applier
}

func applyRedisUpsert(t *testing.T, ctx context.Context, applier *redisconnector.Applier, identity deltaflow.ProjectionIdentity, payload []byte) {
	t.Helper()
	err := applier.Apply(ctx, deltaflow.ProjectionOperation{
		Type:       deltaflow.ProjectionOpUpsert,
		Identity:   identity,
		Projection: &deltaflow.Projection{Identity: identity, Payload: payload, MediaType: "application/octet-stream"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func redisContactIdentity(id string) deltaflow.ProjectionIdentity {
	raw, _ := json.Marshal(id)
	return deltaflow.ProjectionIdentity{
		Type: "Contact",
		Key:  deltaflow.ProjectionKey{"contact_id": raw},
	}
}

func waitForRedis(ctx context.Context, client *redisclient.Client) error {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := client.Ping(ctx).Err(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("ping timeout: %w", lastErr)
}
