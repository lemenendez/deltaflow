package internal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

const defaultMaxAttempts = 5

type DeltaMemoryStore struct {
	mu     sync.Mutex
	now    func() time.Time
	nextID int

	deltas map[deltaflow.DeltaID]*deltaflow.Delta
}

func NewDeltaMemoryStore() *DeltaMemoryStore {
	return &DeltaMemoryStore{
		now:    func() time.Time { return time.Now().UTC() },
		deltas: make(map[deltaflow.DeltaID]*deltaflow.Delta),
	}
}

func (s *DeltaMemoryStore) Enqueue(ctx context.Context, delta deltaflow.Delta) (*deltaflow.Delta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if delta.ID != "" {
		return nil, deltaflow.ErrDeltaIDProvided
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	for {
		s.nextID++
		candidate := deltaflow.DeltaID(fmt.Sprintf("delta-%d", s.nextID))
		if _, exists := s.deltas[candidate]; exists {
			continue
		}
		delta.ID = candidate
		break
	}
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
		return nil, err
	}
	delta.ProjectionKeyHash = hash

	copied := cloneDelta(&delta)
	s.deltas[delta.ID] = copied

	return cloneDelta(copied), nil
}

func (s *DeltaMemoryStore) Get(ctx context.Context, deltaID deltaflow.DeltaID) (*deltaflow.Delta, bool, error) {
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

func (s *DeltaMemoryStore) Pull(ctx context.Context, syncID deltaflow.SyncID, limit int) ([]*deltaflow.Delta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ids := s.pendingDeltaIDsLockedForSyncLocked(syncID)
	if len(ids) > limit {
		ids = ids[:limit]
	}

	pulled := make([]*deltaflow.Delta, 0, len(ids))
	for _, id := range ids {
		pulled = append(pulled, cloneDelta(s.deltas[id]))
	}

	return pulled, nil
}

func (s *DeltaMemoryStore) MarkDispatched(ctx context.Context, deltaID deltaflow.DeltaID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delta, ok := s.deltas[deltaID]
	if !ok {
		return deltaflow.ErrDeltaNotFound
	}
	if delta.State == deltaflow.DeltaDispatched {
		return nil
	}

	now := s.now().UTC()
	delta.State = deltaflow.DeltaDispatched
	delta.DispatchedAt = &now

	return nil
}

func (s *DeltaMemoryStore) pendingDeltaIDsLocked() []deltaflow.DeltaID {
	ids := make([]deltaflow.DeltaID, 0, len(s.deltas))
	for id, delta := range s.deltas {
		if delta.State == deltaflow.DeltaPending {
			ids = append(ids, id)
		}
	}

	sort.Slice(ids, func(i, j int) bool {
		left := s.deltas[ids[i]]
		right := s.deltas[ids[j]]
		if !left.OccurredAt.Equal(right.OccurredAt) {
			return left.OccurredAt.Before(right.OccurredAt)
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.ID < right.ID
	})

	return ids
}

func (s *DeltaMemoryStore) pendingDeltaIDsLockedForSyncLocked(syncID deltaflow.SyncID) []deltaflow.DeltaID {
	ids := make([]deltaflow.DeltaID, 0, len(s.deltas))
	for id, delta := range s.deltas {
		if delta.State == deltaflow.DeltaPending && delta.SyncID == syncID {
			ids = append(ids, id)
		}
	}

	sort.Slice(ids, func(i, j int) bool {
		left := s.deltas[ids[i]]
		right := s.deltas[ids[j]]
		if !left.OccurredAt.Equal(right.OccurredAt) {
			return left.OccurredAt.Before(right.OccurredAt)
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.ID < right.ID
	})

	return ids
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

type JobMemoryStore struct {
	mu         sync.Mutex
	now        func() time.Time
	nextID     int
	jobs       map[deltaflow.SyncJobID]*deltaflow.SyncJob
	jobByDelta map[deltaflow.DeltaID]deltaflow.SyncJobID

	LeaseLogger    *slog.Logger
	LeaseTelemetry deltaflow.LeaseTelemetry
}

func NewJobMemoryStore() *JobMemoryStore {
	return &JobMemoryStore{
		now:            func() time.Time { return time.Now().UTC() },
		jobs:           make(map[deltaflow.SyncJobID]*deltaflow.SyncJob),
		jobByDelta:     make(map[deltaflow.DeltaID]deltaflow.SyncJobID),
		LeaseTelemetry: deltaflow.NoopLeaseTelemetry(),
	}
}

func (s *JobMemoryStore) Create(ctx context.Context, job deltaflow.SyncJob) (*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if job.ID != "" {
		return nil, deltaflow.ErrJobIDProvided
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.createLocked(job, s.now().UTC())
}

func (s *JobMemoryStore) createLocked(job deltaflow.SyncJob, now time.Time) (*deltaflow.SyncJob, error) {
	if job.Origin == deltaflow.JobOriginOutbox && job.DeltaID == nil {
		return nil, deltaflow.ErrOutboxJobNeedsDelta
	}
	if job.Origin == deltaflow.JobOriginOutbox && job.DeltaID != nil {
		if _, exists := s.jobByDelta[*job.DeltaID]; exists {
			return nil, deltaflow.ErrDeltaAlreadyMapped
		}
	}
	if job.ID == "" {
		for {
			s.nextID++
			candidate := deltaflow.SyncJobID(fmt.Sprintf("job-%d", s.nextID))
			if _, exists := s.jobs[candidate]; exists {
				continue
			}
			job.ID = candidate
			break
		}
	}
	if job.State == "" {
		job.State = deltaflow.StatePending
	}
	if job.MaxAttempts == 0 {
		job.MaxAttempts = defaultMaxAttempts
	}
	if job.AvailableAt.IsZero() {
		job.AvailableAt = now
	} else {
		job.AvailableAt = job.AvailableAt.UTC()
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	} else {
		job.CreatedAt = job.CreatedAt.UTC()
	}
	job.UpdatedAt = now

	copied := cloneSyncJob(&job)
	s.jobs[job.ID] = copied
	if job.DeltaID != nil && job.Origin == deltaflow.JobOriginOutbox {
		s.jobByDelta[*job.DeltaID] = job.ID
	}

	return cloneSyncJob(copied), nil
}

func (s *JobMemoryStore) Get(ctx context.Context, jobID deltaflow.SyncJobID) (*deltaflow.SyncJob, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return nil, false, nil
	}
	return cloneSyncJob(job), true, nil
}

func (s *JobMemoryStore) ClaimNext(ctx context.Context, syncID deltaflow.SyncID, workerID string, lockFor time.Duration) (*deltaflow.SyncJob, error) {
	telemetry := s.leaseTelemetry()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lockFor <= 0 {
		telemetry.ObserveLeaseClaim(deltaflow.LeaseTelemetryResultInvalidLockFor)
		s.logLease("lease_claim_rejected",
			"sync_id", syncID,
			"worker_id", workerID,
			"reason", "invalid_lock_for",
		)
		return nil, deltaflow.ErrInvalidLockFor
	}

	s.mu.Lock()

	var (
		claimed     *deltaflow.SyncJob
		claimResult string
		emitReclaim bool
		logEvent    string
		logAttrs    []any
	)

	now := s.now().UTC()
	ids := s.claimableJobIDsLocked(now, syncID)
	if len(ids) == 0 {
		claimResult = deltaflow.LeaseTelemetryResultEmpty
		logEvent = "lease_claim_empty"
		logAttrs = []any{
			"sync_id", syncID,
			"worker_id", workerID,
		}
		s.mu.Unlock()

		telemetry.ObserveLeaseClaim(claimResult)
		s.logLease(logEvent, logAttrs...)
		return nil, nil
	}

	job := s.jobs[ids[0]]
	wasExpiredProcessing := job.State == deltaflow.StateProcessing && (job.LockedUntil == nil || !job.LockedUntil.After(now))
	job.State = deltaflow.StateProcessing
	job.LockedBy = stringPtr(workerID)
	lockedUntil := now.Add(lockFor)
	job.LockedUntil = &lockedUntil
	job.UpdatedAt = now

	claimResult = deltaflow.LeaseTelemetryResultSuccess
	if wasExpiredProcessing {
		emitReclaim = true
	}
	reason := "ready"
	if wasExpiredProcessing {
		reason = "expired_reclaimed"
	}
	logEvent = "lease_claimed"
	logAttrs = []any{
		"sync_id", syncID,
		"job_id", job.ID,
		"worker_id", workerID,
		"state", job.State,
		"attempt_count", job.AttemptCount,
		"locked_until", lockedUntil,
		"lease_ms_remaining", int64(lockFor / time.Millisecond),
		"reason", reason,
	}
	claimed = cloneSyncJob(job)

	s.mu.Unlock()

	telemetry.ObserveLeaseClaim(claimResult)
	if emitReclaim {
		telemetry.ObserveLeaseReclaim()
	}
	s.logLease(logEvent, logAttrs...)

	return claimed, nil
}

func (s *JobMemoryStore) RenewLease(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, lockFor time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lockFor <= 0 {
		s.leaseTelemetry().ObserveLeaseRenew(deltaflow.LeaseTelemetryResultInvalidLockFor, 0)
		s.logLease("lease_renew_rejected",
			"job_id", jobID,
			"worker_id", workerID,
			"reason", "invalid_lock_for",
		)
		return deltaflow.ErrInvalidLockFor
	}

	start := time.Now()
	err := s.updateOwned(ctx, jobID, workerID, deltaflow.LeaseTelemetryTransitionRenewLease, func(job *deltaflow.SyncJob, now time.Time) {
		lockedUntil := now.Add(lockFor)
		job.LockedUntil = &lockedUntil
	})
	result := leaseResult(err)
	s.leaseTelemetry().ObserveLeaseRenew(result, time.Since(start))
	if err != nil {
		s.logLease("lease_renew_failed",
			"job_id", jobID,
			"worker_id", workerID,
			"reason", result,
		)
		return err
	}

	s.logLease("lease_renewed",
		"job_id", jobID,
		"worker_id", workerID,
		"lease_ms_remaining", int64(lockFor/time.Millisecond),
	)
	return nil
}

func (s *JobMemoryStore) MarkSynced(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, ghostDetected bool) error {
	return s.updateOwned(ctx, jobID, workerID, deltaflow.LeaseTelemetryTransitionMarkSynced, func(job *deltaflow.SyncJob, now time.Time) {
		job.State = deltaflow.StateSynced
		job.GhostDetected = ghostDetected
		job.SyncedAt = &now
		clearJobLock(job)
	})
}

func (s *JobMemoryStore) MarkRetrying(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error, nextRunAt time.Time) error {
	return s.updateOwned(ctx, jobID, workerID, deltaflow.LeaseTelemetryTransitionMarkRetrying, func(job *deltaflow.SyncJob, now time.Time) {
		job.State = deltaflow.StateRetrying
		job.AttemptCount++
		job.LastError = stringPtr(errorMessage(err))
		job.AvailableAt = nextRunAt.UTC()
		clearJobLock(job)
	})
}

func (s *JobMemoryStore) MarkDead(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error) error {
	return s.updateOwned(ctx, jobID, workerID, deltaflow.LeaseTelemetryTransitionMarkDead, func(job *deltaflow.SyncJob, now time.Time) {
		job.State = deltaflow.StateDead
		job.AttemptCount++
		job.LastError = stringPtr(errorMessage(err))
		job.DeadAt = &now
		clearJobLock(job)
	})
}

func (s *JobMemoryStore) updateOwned(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, transition string, fn func(*deltaflow.SyncJob, time.Time)) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		s.logLease("lease_transition_rejected",
			"transition", transition,
			"job_id", jobID,
			"worker_id", workerID,
			"reason", "job_not_found",
		)
		return deltaflow.ErrJobNotFound
	}
	now := s.now().UTC()
	if !jobLeaseOwned(job, now, workerID) {
		s.leaseTelemetry().ObserveLeaseOwnershipCheck(transition, deltaflow.LeaseTelemetryOwnershipRejected)
		reason := "lease_not_owned"
		s.logLease("lease_transition_rejected",
			"transition", transition,
			"sync_id", job.SyncID,
			"job_id", job.ID,
			"worker_id", workerID,
			"state", job.State,
			"attempt_count", job.AttemptCount,
			"locked_until", job.LockedUntil,
			"lease_ms_remaining", leaseMSRemaining(job.LockedUntil, now),
			"reason", reason,
		)
		return deltaflow.ErrJobLeaseNotOwned
	}
	s.leaseTelemetry().ObserveLeaseOwnershipCheck(transition, deltaflow.LeaseTelemetryOwnershipOwned)

	fn(job, now)
	job.UpdatedAt = now

	s.logLease("lease_transition_applied",
		"transition", transition,
		"sync_id", job.SyncID,
		"job_id", job.ID,
		"worker_id", workerID,
		"state", job.State,
		"attempt_count", job.AttemptCount,
		"locked_until", job.LockedUntil,
		"lease_ms_remaining", leaseMSRemaining(job.LockedUntil, now),
	)

	return nil
}

func (s *JobMemoryStore) leaseTelemetry() deltaflow.LeaseTelemetry {
	if s.LeaseTelemetry == nil {
		return deltaflow.NoopLeaseTelemetry()
	}
	return s.LeaseTelemetry
}

func (s *JobMemoryStore) logLease(event string, attrs ...any) {
	if s.LeaseLogger == nil {
		return
	}
	eventAttrs := make([]any, 0, len(attrs)+2)
	eventAttrs = append(eventAttrs, "event", event)
	eventAttrs = append(eventAttrs, attrs...)
	s.LeaseLogger.Info("lease event", eventAttrs...)
}

func leaseMSRemaining(lockedUntil *time.Time, now time.Time) int64 {
	if lockedUntil == nil {
		return 0
	}
	remaining := lockedUntil.Sub(now)
	if remaining < 0 {
		return 0
	}
	return int64(remaining / time.Millisecond)
}

func leaseResult(err error) string {
	if err == nil {
		return deltaflow.LeaseTelemetryResultSuccess
	}
	if errors.Is(err, deltaflow.ErrJobNotFound) {
		return deltaflow.LeaseTelemetryResultJobNotFound
	}
	if errors.Is(err, deltaflow.ErrJobLeaseNotOwned) {
		return deltaflow.LeaseTelemetryResultLeaseNotOwned
	}
	if errors.Is(err, deltaflow.ErrInvalidLockFor) {
		return deltaflow.LeaseTelemetryResultInvalidLockFor
	}
	return deltaflow.LeaseTelemetryResultError
}

func jobLeaseOwned(job *deltaflow.SyncJob, now time.Time, workerID string) bool {
	if job == nil || job.State != deltaflow.StateProcessing || job.LockedBy == nil || *job.LockedBy != workerID {
		return false
	}
	if job.LockedUntil == nil {
		return false
	}
	return job.LockedUntil.After(now)
}

func (s *JobMemoryStore) claimableJobIDsLocked(now time.Time, syncID deltaflow.SyncID) []deltaflow.SyncJobID {
	ids := make([]deltaflow.SyncJobID, 0, len(s.jobs))
	for id, job := range s.jobs {
		if claimableJob(job, now, syncID) {
			ids = append(ids, id)
		}
	}

	sort.Slice(ids, func(i, j int) bool {
		left := s.jobs[ids[i]]
		right := s.jobs[ids[j]]
		if !left.AvailableAt.Equal(right.AvailableAt) {
			return left.AvailableAt.Before(right.AvailableAt)
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.ID < right.ID
	})

	return ids
}

func claimableJob(job *deltaflow.SyncJob, now time.Time, syncID deltaflow.SyncID) bool {
	if job.SyncID != syncID {
		return false
	}

	switch job.State {
	case deltaflow.StatePending, deltaflow.StateRetrying:
		return !job.AvailableAt.After(now)
	case deltaflow.StateProcessing:
		return job.LockedUntil == nil || !job.LockedUntil.After(now)
	default:
		return false
	}
}

func clearJobLock(job *deltaflow.SyncJob) {
	job.LockedBy = nil
	job.LockedUntil = nil
}

func cloneSyncJob(job *deltaflow.SyncJob) *deltaflow.SyncJob {
	if job == nil {
		return nil
	}

	copied := *job
	copied.DeltaID = cloneDeltaIDPtr(job.DeltaID)
	copied.ProjectionKey = cloneProjectionKey(job.ProjectionKey)
	copied.LastError = cloneStringPtr(job.LastError)
	copied.LastErrorCode = cloneStringPtr(job.LastErrorCode)
	copied.LockedBy = cloneStringPtr(job.LockedBy)
	copied.LockedUntil = cloneTimePtr(job.LockedUntil)
	copied.SyncedAt = cloneTimePtr(job.SyncedAt)
	copied.DeadAt = cloneTimePtr(job.DeadAt)

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

func cloneDeltaIDPtr(value *deltaflow.DeltaID) *deltaflow.DeltaID {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
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

func projectionKeyHash(key deltaflow.ProjectionKey) (deltaflow.ProjectionKeyHash, error) {
	encoded, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return deltaflow.ProjectionKeyHash(hex.EncodeToString(sum[:])), nil
}
