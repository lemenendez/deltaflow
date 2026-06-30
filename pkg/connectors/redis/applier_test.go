package redis

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestNewApplierValidatesConfig(t *testing.T) {
	client := &recordingClient{}
	keyFunc := func(deltaflow.ProjectionIdentity) (string, error) { return "contact:1", nil }

	tests := []struct {
		name string
		cfg  ApplierConfig
		want string
	}{
		{name: "client", cfg: ApplierConfig{KeyFunc: keyFunc}, want: "redis applier: client is required"},
		{name: "key func", cfg: ApplierConfig{Client: client}, want: "redis applier: key func is required"},
		{name: "negative ttl", cfg: ApplierConfig{Client: client, KeyFunc: keyFunc, TTL: -time.Nanosecond}, want: "redis applier: ttl must be non-negative"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewApplier(tt.cfg)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("NewApplier() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestApplierUpsertMapsExactPayloadAndTTL(t *testing.T) {
	client := &recordingClient{}
	applier := newTestApplier(t, client, 15*time.Minute, func(identity deltaflow.ProjectionIdentity) (string, error) {
		if identity.Type != "Contact" {
			t.Fatalf("identity type = %q", identity.Type)
		}
		return "contacts:111", nil
	})
	payload := []byte{0x00, 0xff, '{', 'x', '}'}

	err := applier.Apply(context.Background(), deltaflow.ProjectionOperation{
		Type:       deltaflow.ProjectionOpUpsert,
		Identity:   deltaflow.ProjectionIdentity{Type: "Contact"},
		Projection: &deltaflow.Projection{Payload: payload, MediaType: "application/octet-stream"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(client.setCalls) != 1 {
		t.Fatalf("set calls = %d, want 1", len(client.setCalls))
	}
	call := client.setCalls[0]
	if call.key != "contacts:111" {
		t.Fatalf("key = %q", call.key)
	}
	if !bytes.Equal(call.value, payload) {
		t.Fatalf("payload = %v, want %v", call.value, payload)
	}
	if call.ttl != 15*time.Minute {
		t.Fatalf("ttl = %s, want 15m", call.ttl)
	}
}

func TestApplierZeroTTLMapsToPersistentSet(t *testing.T) {
	client := &recordingClient{}
	applier := newTestApplier(t, client, 0, staticKey("contacts:111"))

	err := applier.Apply(context.Background(), upsertOperation([]byte("opaque")))
	if err != nil {
		t.Fatal(err)
	}
	if got := client.setCalls[0].ttl; got != 0 {
		t.Fatalf("ttl = %s, want zero", got)
	}
}

func TestApplierDeleteIsSuccessfulWhenKeyIsMissing(t *testing.T) {
	client := &recordingClient{deleteResult: 0}
	applier := newTestApplier(t, client, 0, staticKey("contacts:missing"))

	err := applier.Apply(context.Background(), deltaflow.ProjectionOperation{
		Type:     deltaflow.ProjectionOpDelete,
		Identity: deltaflow.ProjectionIdentity{Type: "Contact"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(client.delCalls) != 1 || len(client.delCalls[0]) != 1 || client.delCalls[0][0] != "contacts:missing" {
		t.Fatalf("delete calls = %#v", client.delCalls)
	}
}

func TestApplierRejectsInvalidOperationsBeforeResolvingKey(t *testing.T) {
	keyCalls := 0
	applier := newTestApplier(t, &recordingClient{}, 0, func(deltaflow.ProjectionIdentity) (string, error) {
		keyCalls++
		return "contacts:111", nil
	})

	tests := []deltaflow.ProjectionOperation{
		{Type: deltaflow.ProjectionOpUpsert},
		{Type: deltaflow.ProjectionOperationType("increment")},
	}
	for _, op := range tests {
		if err := applier.Apply(context.Background(), op); err == nil {
			t.Fatalf("Apply(%q) error = nil", op.Type)
		}
	}
	if keyCalls != 0 {
		t.Fatalf("key func calls = %d, want 0", keyCalls)
	}
}

func TestApplierRejectsEmptyKey(t *testing.T) {
	client := &recordingClient{}
	applier := newTestApplier(t, client, 0, staticKey(""))

	err := applier.Apply(context.Background(), upsertOperation([]byte("value")))
	if err == nil || err.Error() != "redis applier: key func returned an empty key" {
		t.Fatalf("error = %v", err)
	}
	if len(client.setCalls) != 0 {
		t.Fatalf("set calls = %d, want 0", len(client.setCalls))
	}
}

func TestApplierPreservesKeyFuncError(t *testing.T) {
	want := errors.New("invalid contact key")
	applier := newTestApplier(t, &recordingClient{}, 0, func(deltaflow.ProjectionIdentity) (string, error) {
		return "", want
	})

	err := applier.Apply(context.Background(), upsertOperation([]byte("value")))
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want wrapped %v", err, want)
	}
}

func TestApplierPreservesClientErrors(t *testing.T) {
	wantSet := errors.New("set unavailable")
	wantDelete := errors.New("delete unavailable")
	client := &recordingClient{setErr: wantSet, deleteErr: wantDelete}
	applier := newTestApplier(t, client, 0, staticKey("contacts:111"))

	if err := applier.Apply(context.Background(), upsertOperation([]byte("value"))); !errors.Is(err, wantSet) {
		t.Fatalf("upsert error = %v, want wrapped %v", err, wantSet)
	}
	if err := applier.Apply(context.Background(), deltaflow.ProjectionOperation{Type: deltaflow.ProjectionOpDelete}); !errors.Is(err, wantDelete) {
		t.Fatalf("delete error = %v, want wrapped %v", err, wantDelete)
	}
}

func TestApplierPassesContextToClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &recordingClient{}
	applier := newTestApplier(t, client, 0, staticKey("contacts:111"))

	if err := applier.Apply(ctx, upsertOperation([]byte("value"))); err != nil {
		t.Fatal(err)
	}
	if client.setCalls[0].ctx != ctx {
		t.Fatal("client did not receive apply context")
	}
}

func newTestApplier(t *testing.T, client CommandClient, ttl time.Duration, keyFunc KeyFunc) *Applier {
	t.Helper()
	applier, err := NewApplier(ApplierConfig{Client: client, KeyFunc: keyFunc, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	return applier
}

func staticKey(key string) KeyFunc {
	return func(deltaflow.ProjectionIdentity) (string, error) { return key, nil }
}

func upsertOperation(payload []byte) deltaflow.ProjectionOperation {
	return deltaflow.ProjectionOperation{
		Type:       deltaflow.ProjectionOpUpsert,
		Identity:   deltaflow.ProjectionIdentity{Type: "Contact"},
		Projection: &deltaflow.Projection{Payload: payload},
	}
}

type setCall struct {
	ctx   context.Context
	key   string
	value []byte
	ttl   time.Duration
}

type recordingClient struct {
	setCalls     []setCall
	delCalls     [][]string
	setErr       error
	deleteErr    error
	deleteResult int64
}

func (c *recordingClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redisclient.StatusCmd {
	cmd := redisclient.NewStatusCmd(ctx)
	if c.setErr != nil {
		cmd.SetErr(c.setErr)
		return cmd
	}
	payload, ok := value.([]byte)
	if !ok {
		cmd.SetErr(errors.New("test client: value is not []byte"))
		return cmd
	}
	c.setCalls = append(c.setCalls, setCall{
		ctx:   ctx,
		key:   key,
		value: append([]byte(nil), payload...),
		ttl:   expiration,
	})
	cmd.SetVal("OK")
	return cmd
}

func (c *recordingClient) Del(ctx context.Context, keys ...string) *redisclient.IntCmd {
	cmd := redisclient.NewIntCmd(ctx)
	if c.deleteErr != nil {
		cmd.SetErr(c.deleteErr)
		return cmd
	}
	c.delCalls = append(c.delCalls, append([]string(nil), keys...))
	cmd.SetVal(c.deleteResult)
	return cmd
}
