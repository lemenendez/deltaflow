package connectors

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

// DeltaStoreConfig configures shared behavior for durable delta stores.
type DeltaStoreConfig struct {
	Now                 func() time.Time
	MaxEnqueueBatchSize int
}

// DeltaStoreBase contains SQL-agnostic helpers shared by durable delta stores.
type DeltaStoreBase struct {
	DB  *sql.DB
	cfg DeltaStoreConfig
}

func NewDeltaStoreBase(db *sql.DB, cfg DeltaStoreConfig) DeltaStoreBase {
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.MaxEnqueueBatchSize <= 0 {
		cfg.MaxEnqueueBatchSize = deltaflow.DefaultMaxEnqueueBatchSize
	}
	return DeltaStoreBase{DB: db, cfg: cfg}
}

func (b *DeltaStoreBase) now() time.Time {
	return b.cfg.Now().UTC()
}

func (b *DeltaStoreBase) PrepareDeltaForEnqueue(delta deltaflow.Delta) (deltaflow.Delta, []byte, []byte, error) {
	now := b.now()

	if delta.State == "" {
		delta.State = deltaflow.DeltaPending
	}
	if delta.OccurredAt.IsZero() {
		delta.OccurredAt = now
	} else {
		delta.OccurredAt = delta.OccurredAt.UTC()
	}
	if delta.CreatedAt.IsZero() {
		delta.CreatedAt = now
	} else {
		delta.CreatedAt = delta.CreatedAt.UTC()
	}

	hash, err := projectionKeyHash(delta.ProjectionKey)
	if err != nil {
		return deltaflow.Delta{}, nil, nil, err
	}
	delta.ProjectionKeyHash = hash
	if delta.DedupWindow != "" {
		delta.DedupKey = DedupKey(delta.DedupWindow, delta.ProjectionType, hash)
	} else {
		delta.DedupKey = ""
	}

	projectionKeyJSON, err := json.Marshal(delta.ProjectionKey)
	if err != nil {
		return deltaflow.Delta{}, nil, nil, err
	}
	metadataJSON, err := json.Marshal(delta.Metadata)
	if err != nil {
		return deltaflow.Delta{}, nil, nil, err
	}

	return delta, projectionKeyJSON, metadataJSON, nil
}

// DedupKey deterministically identifies one projection identity in a window.
func DedupKey(window deltaflow.DedupWindow, projectionType deltaflow.ProjectionType, keyHash deltaflow.ProjectionKeyHash) deltaflow.DedupKey {
	h := sha256.New()
	for _, value := range []string{string(window), string(projectionType), string(keyHash)} {
		_, _ = h.Write([]byte{byte(len(value) >> 24), byte(len(value) >> 16), byte(len(value) >> 8), byte(len(value))})
		_, _ = h.Write([]byte(value))
	}
	return deltaflow.DedupKey(hex.EncodeToString(h.Sum(nil)))
}

func (b *DeltaStoreBase) ValidateEnqueueBatch(ctx context.Context, deltas []deltaflow.Delta) (deltaflow.DedupWindow, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(deltas) > b.cfg.MaxEnqueueBatchSize {
		return "", deltaflow.ErrEnqueueBatchTooLarge
	}
	if len(deltas) == 0 {
		return "", nil
	}
	window := deltas[0].DedupWindow
	if window == "" {
		return "", deltaflow.ErrDedupWindowRequired
	}
	for _, delta := range deltas[1:] {
		if delta.DedupWindow == "" {
			return "", deltaflow.ErrDedupWindowRequired
		}
		if delta.DedupWindow != window {
			return "", deltaflow.ErrMixedDedupWindows
		}
	}
	return window, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func (b *DeltaStoreBase) ScanDelta(scanner rowScanner) (*deltaflow.Delta, error) {
	var (
		id                string
		syncID            string
		origin            string
		projectionType    string
		projectionKeyHash string
		dedupWindow       sql.NullString
		dedupKey          sql.NullString
		state             string
		projectionKeyJSON []byte
		metadataJSON      []byte
		occurredAt        time.Time
		createdAt         time.Time
		dispatchedAt      sql.NullTime
	)

	err := scanner.Scan(
		&id,
		&syncID,
		&origin,
		&projectionType,
		&projectionKeyJSON,
		&projectionKeyHash,
		&dedupWindow,
		&dedupKey,
		&state,
		&occurredAt,
		&createdAt,
		&dispatchedAt,
		&metadataJSON,
	)
	if err != nil {
		return nil, err
	}

	var projectionKey deltaflow.ProjectionKey
	if len(projectionKeyJSON) > 0 {
		if err := json.Unmarshal(projectionKeyJSON, &projectionKey); err != nil {
			return nil, err
		}
	}

	var metadata map[string]any
	if len(metadataJSON) > 0 && string(metadataJSON) != "null" {
		if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
			return nil, err
		}
	}

	delta := &deltaflow.Delta{
		ID:                deltaflow.DeltaID(id),
		SyncID:            deltaflow.SyncID(syncID),
		Origin:            deltaflow.OriginOperationType(origin),
		ProjectionType:    deltaflow.ProjectionType(projectionType),
		ProjectionKey:     projectionKey,
		ProjectionKeyHash: deltaflow.ProjectionKeyHash(projectionKeyHash),
		DedupWindow:       deltaflow.DedupWindow(dedupWindow.String),
		DedupKey:          deltaflow.DedupKey(dedupKey.String),
		State:             deltaflow.DeltaState(state),
		OccurredAt:        occurredAt.UTC(),
		CreatedAt:         createdAt.UTC(),
		Metadata:          metadata,
	}
	if dispatchedAt.Valid {
		t := dispatchedAt.Time.UTC()
		delta.DispatchedAt = &t
	}

	return cloneDelta(delta), nil
}

func IsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}

	type sqlStateProvider interface {
		SQLState() string
	}
	var provider sqlStateProvider
	if errors.As(err, &provider) {
		return provider.SQLState() == "23505"
	}

	value := reflect.Indirect(reflect.ValueOf(err))
	if value.IsValid() && value.Kind() == reflect.Struct {
		field := value.FieldByName("Code")
		if field.IsValid() && field.Kind() == reflect.String && field.String() == "23505" {
			return true
		}
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "duplicate key") || strings.Contains(message, "unique constraint")
}

func cloneDelta(delta *deltaflow.Delta) *deltaflow.Delta {
	if delta == nil {
		return nil
	}

	copied := *delta
	copied.ProjectionKey = cloneProjectionKey(delta.ProjectionKey)
	copied.DispatchedAt = cloneTimePtr(delta.DispatchedAt)
	copied.Metadata = cloneMetadata(delta.Metadata)

	return &copied
}

func cloneProjectionKey(key deltaflow.ProjectionKey) deltaflow.ProjectionKey {
	if key == nil {
		return nil
	}

	copied := make(deltaflow.ProjectionKey, len(key))
	for k, v := range key {
		if v == nil {
			copied[k] = nil
			continue
		}
		copied[k] = append([]byte(nil), v...)
	}
	return copied
}

func cloneMetadata(metadata map[string]any) map[string]any {
	if metadata == nil {
		return nil
	}

	copied := make(map[string]any, len(metadata))
	for key, value := range metadata {
		copied[key] = cloneMetadataValue(value)
	}
	return copied
}

func cloneMetadataValue(value any) any {
	if value == nil {
		return nil
	}
	return cloneMetadataReflect(reflect.ValueOf(value)).Interface()
}

func cloneMetadataReflect(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}

	switch value.Kind() {
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		copied := reflect.MakeMapWithSize(value.Type(), value.Len())
		for _, key := range value.MapKeys() {
			clonedValue := cloneMetadataReflect(value.MapIndex(key))
			if !clonedValue.IsValid() {
				clonedValue = reflect.Zero(value.Type().Elem())
			}
			copied.SetMapIndex(key, clonedValue)
		}
		return copied
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		copied := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			clonedItem := cloneMetadataReflect(value.Index(i))
			if !clonedItem.IsValid() {
				clonedItem = reflect.Zero(value.Type().Elem())
			}
			copied.Index(i).Set(clonedItem)
		}
		return copied
	case reflect.Array:
		copied := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			clonedItem := cloneMetadataReflect(value.Index(i))
			if !clonedItem.IsValid() {
				clonedItem = reflect.Zero(value.Type().Elem())
			}
			copied.Index(i).Set(clonedItem)
		}
		return copied
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		copied := reflect.New(value.Elem().Type())
		copied.Elem().Set(cloneMetadataReflect(value.Elem()))
		return copied
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneMetadataReflect(value.Elem())
		if !cloned.IsValid() {
			return reflect.Zero(value.Type())
		}
		if cloned.Type().AssignableTo(value.Type()) {
			return cloned
		}
		wrapped := reflect.New(value.Type()).Elem()
		wrapped.Set(cloned)
		return wrapped
	default:
		return value
	}
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func projectionKeyHash(key deltaflow.ProjectionKey) (deltaflow.ProjectionKeyHash, error) {
	encoded, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return deltaflow.ProjectionKeyHash(hex.EncodeToString(sum[:])), nil
}
