package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

var (
	ErrDeltaNotFound      = errors.New("delta not found")
	ErrDeltaAlreadyExists = errors.New("delta already exists")
)

type MemoryDeltaStore struct {
	mu     sync.Mutex
	now    func() time.Time
	nextID int

	order  []string
	deltas map[string]*deltaflow.Delta
}

func NewMemoryDeltaStore() *MemoryDeltaStore {
	return &MemoryDeltaStore{
		now:    func() time.Time { return time.Now().UTC() },
		deltas: make(map[string]*deltaflow.Delta),
	}
}

func (s *MemoryDeltaStore) Insert(ctx context.Context, delta deltaflow.Delta) (*deltaflow.Delta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if delta.ID == "" {
		s.nextID++
		delta.ID = fmt.Sprintf("delta-%d", s.nextID)
	}
	if _, exists := s.deltas[delta.ID]; exists {
		return nil, ErrDeltaAlreadyExists
	}
	if delta.State == "" {
		delta.State = deltaflow.StatePending
	}
	if delta.MaxAttempts == 0 {
		delta.MaxAttempts = 5
	}
	if delta.AvailableAt.IsZero() {
		delta.AvailableAt = now
	}
	if delta.CreatedAt.IsZero() {
		delta.CreatedAt = now
	}
	delta.UpdatedAt = now

	copied := cloneDelta(&delta)
	s.deltas[delta.ID] = copied
	s.order = append(s.order, delta.ID)

	return cloneDelta(copied), nil
}

func (s *MemoryDeltaStore) Get(ctx context.Context, deltaID string) (*deltaflow.Delta, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delta, ok := s.deltas[deltaID]
	if !ok {
		return nil, false, nil
	}
	return cloneDelta(delta), true, nil
}

func (s *MemoryDeltaStore) ClaimNext(ctx context.Context, workerID string, lockFor time.Duration) (*deltaflow.Delta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	for _, id := range s.order {
		delta := s.deltas[id]
		if !claimable(delta, now) {
			continue
		}

		delta.State = deltaflow.StateProcessing
		delta.LockedBy = stringPtr(workerID)
		lockedUntil := now.Add(lockFor)
		delta.LockedUntil = &lockedUntil
		delta.UpdatedAt = now

		return cloneDelta(delta), nil
	}

	return nil, nil
}

func (s *MemoryDeltaStore) MarkSynced(ctx context.Context, deltaID string, ghostDetected bool) error {
	return s.update(ctx, deltaID, func(delta *deltaflow.Delta, now time.Time) {
		delta.State = deltaflow.StateSynced
		delta.GhostDetected = ghostDetected
		delta.SyncedAt = &now
		clearLock(delta)
	})
}

func (s *MemoryDeltaStore) MarkRetrying(ctx context.Context, deltaID string, err error, nextRunAt time.Time) error {
	return s.update(ctx, deltaID, func(delta *deltaflow.Delta, now time.Time) {
		delta.State = deltaflow.StateRetrying
		delta.AttemptCount++
		delta.LastError = stringPtr(errorMessage(err))
		delta.AvailableAt = nextRunAt
		clearLock(delta)
	})
}

func (s *MemoryDeltaStore) MarkDead(ctx context.Context, deltaID string, err error) error {
	return s.update(ctx, deltaID, func(delta *deltaflow.Delta, now time.Time) {
		delta.State = deltaflow.StateDead
		delta.AttemptCount++
		delta.LastError = stringPtr(errorMessage(err))
		delta.DeadAt = &now
		clearLock(delta)
	})
}

func (s *MemoryDeltaStore) update(ctx context.Context, deltaID string, fn func(*deltaflow.Delta, time.Time)) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delta, ok := s.deltas[deltaID]
	if !ok {
		return ErrDeltaNotFound
	}

	now := s.now()
	fn(delta, now)
	delta.UpdatedAt = now

	return nil
}

func claimable(delta *deltaflow.Delta, now time.Time) bool {
	switch delta.State {
	case deltaflow.StatePending, deltaflow.StateRetrying:
		return !delta.AvailableAt.After(now)
	case deltaflow.StateProcessing:
		return delta.LockedUntil != nil && delta.LockedUntil.Before(now)
	default:
		return false
	}
}

func clearLock(delta *deltaflow.Delta) {
	delta.LockedBy = nil
	delta.LockedUntil = nil
}

func cloneDelta(delta *deltaflow.Delta) *deltaflow.Delta {
	if delta == nil {
		return nil
	}

	copied := *delta
	copied.ProjectionKey = cloneProjectionKey(delta.ProjectionKey)
	copied.LastError = cloneStringPtr(delta.LastError)
	copied.LastErrorCode = cloneStringPtr(delta.LastErrorCode)
	copied.LockedBy = cloneStringPtr(delta.LockedBy)
	copied.LockedUntil = cloneTimePtr(delta.LockedUntil)
	copied.SyncedAt = cloneTimePtr(delta.SyncedAt)
	copied.DeadAt = cloneTimePtr(delta.DeadAt)

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

func cloneStringPtr(value *string) *string {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func stringPtr(value string) *string {
	return &value
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
