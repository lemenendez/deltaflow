package internal

import (
	"context"
	"fmt"
	"log/slog"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type MemoryDispatchStore struct {
	deltaStore *DeltaMemoryStore
	jobStore   *JobMemoryStore
	logger     *slog.Logger
}

func NewMemoryDispatchStore(deltaStore *DeltaMemoryStore, jobStore *JobMemoryStore, logger *slog.Logger) *MemoryDispatchStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &MemoryDispatchStore{
		deltaStore: deltaStore,
		jobStore:   jobStore,
		logger:     logger,
	}
}

func (s *MemoryDispatchStore) DispatchPending(ctx context.Context, syncID deltaflow.SyncID, limit int) ([]*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}

	s.deltaStore.mu.Lock()
	s.jobStore.mu.Lock()
	defer s.jobStore.mu.Unlock()
	defer s.deltaStore.mu.Unlock()

	now := s.deltaStore.now().UTC()
	ids := s.deltaStore.pendingDeltaIDsLockedForSyncLocked(syncID)
	if len(ids) > limit {
		ids = ids[:limit]
	}

	type stagedJob struct {
		deltaID deltaflow.DeltaID
		job     deltaflow.SyncJob
	}

	staged := make([]stagedJob, 0, len(ids))
	stagedMapped := make([]deltaflow.DeltaID, 0, len(ids))
	nextID := s.jobStore.nextID
	for _, id := range ids {
		delta := s.deltaStore.deltas[id]
		if _, exists := s.jobStore.jobByDelta[id]; exists {
			s.logger.Warn("delta already mapped to job, ignoring dispatch", "delta_id", id)
			stagedMapped = append(stagedMapped, id)
			continue
		}

		var jobID deltaflow.SyncJobID
		for {
			nextID++
			jobID = deltaflow.SyncJobID(fmt.Sprintf("job-%d", nextID))
			if _, exists := s.jobStore.jobs[jobID]; exists {
				continue
			}
			break
		}

		staged = append(staged, stagedJob{
			deltaID: id,
			job: deltaflow.SyncJob{
				ID:                jobID,
				SyncID:            delta.SyncID,
				DeltaID:           cloneDeltaIDPtr(&delta.ID),
				Origin:            deltaflow.JobOriginOutbox,
				ProjectionType:    delta.ProjectionType,
				ProjectionKey:     cloneProjectionKey(delta.ProjectionKey),
				ProjectionKeyHash: delta.ProjectionKeyHash,
				State:             deltaflow.StatePending,
				MaxAttempts:       defaultMaxAttempts,
				AvailableAt:       now,
				CreatedAt:         now,
				UpdatedAt:         now,
			},
		})
	}

	for _, deltaID := range stagedMapped {
		delta := s.deltaStore.deltas[deltaID]
		if delta.State != deltaflow.DeltaDispatched {
			delta.State = deltaflow.DeltaDispatched
			delta.DispatchedAt = &now
		}
	}

	for _, item := range staged {
		job := cloneSyncJob(&item.job)
		s.jobStore.jobs[item.job.ID] = job
		s.jobStore.jobByDelta[item.deltaID] = item.job.ID
		delta := s.deltaStore.deltas[item.deltaID]
		delta.State = deltaflow.DeltaDispatched
		delta.DispatchedAt = &now
	}
	s.jobStore.nextID = nextID

	jobs := make([]*deltaflow.SyncJob, 0, len(staged))
	for _, item := range staged {
		jobs = append(jobs, cloneSyncJob(&item.job))
	}
	return jobs, nil
}
