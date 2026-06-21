package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type benchmarkConfig struct {
	Seed         int64
	Universe     int
	Mutations    int
	GhostEvery   int
	Concurrency  string
	BatchSize    string
	LockFor      time.Duration
	WorkerIDBase string
}

type benchResult struct {
	Concurrency int
	BatchSize   int
	Duration    time.Duration
	JobsPerSec  float64
	WorkerRuns  int
	ClaimCalls  int
	Synced      int
	Dead        int
	Retrying    int
	Ghosts      int
}

type benchmarkScenario struct {
	Name       string
	Seed       int64
	Universe   int
	Mutations  int
	GhostCount int
	Source     *sourceStore
	Target     *targetIndex
	Jobs       []deltaflow.SyncJob
}

func runBenchmark(ctx context.Context, cfg benchmarkConfig) error {
	if cfg.Universe <= 0 {
		return errors.New("universe must be positive")
	}
	if cfg.Mutations <= 0 {
		return errors.New("mutations must be positive")
	}
	if cfg.LockFor <= 0 {
		return errors.New("lock-for must be positive")
	}
	if strings.TrimSpace(cfg.WorkerIDBase) == "" {
		cfg.WorkerIDBase = "bench-worker"
	}

	concurrencyValues, err := parseIntList(cfg.Concurrency)
	if err != nil {
		return fmt.Errorf("parse concurrency: %w", err)
	}
	batchValues, err := parseIntList(cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("parse batch sizes: %w", err)
	}

	scenario := buildBenchmarkScenario(cfg.Seed, cfg.Universe, cfg.Mutations, cfg.GhostEvery)
	fmt.Printf("DeltaFlow playground 01-in-memory benchmark\n")
	fmt.Printf("seed=%d universe=%d mutations=%d ghosts=%d\n", scenario.Seed, scenario.Universe, scenario.Mutations, scenario.GhostCount)
	fmt.Printf("baseline=(concurrency=1,batch=1) candidates=(%s)x(%s)\n\n", cfg.Concurrency, cfg.BatchSize)

	pairs := make([]struct {
		Concurrency int
		BatchSize   int
	}, 0, len(concurrencyValues)*len(batchValues)+1)
	pairs = append(pairs, struct {
		Concurrency int
		BatchSize   int
	}{Concurrency: 1, BatchSize: 1})
	for _, c := range concurrencyValues {
		for _, b := range batchValues {
			if c == 1 && b == 1 {
				continue
			}
			pairs = append(pairs, struct {
				Concurrency int
				BatchSize   int
			}{Concurrency: c, BatchSize: b})
		}
	}

	results := make([]benchResult, 0, len(pairs))
	for _, pair := range pairs {
		fmt.Printf("running case c=%d b=%d ...\n", pair.Concurrency, pair.BatchSize)
		result, err := runBenchmarkCase(ctx, scenario, pair.Concurrency, pair.BatchSize, cfg.LockFor, cfg.WorkerIDBase)
		if err != nil {
			return err
		}
		fmt.Printf("finished case c=%d b=%d in %s\n", result.Concurrency, result.BatchSize, result.Duration.Round(time.Millisecond))
		results = append(results, result)
	}

	baseline := results[0]
	fmt.Printf("case c=%d b=%d  duration=%s  jobs_per_sec=%.2f  worker_runs=%d  claim_calls=%d  synced=%d dead=%d retrying=%d ghosts=%d\n",
		baseline.Concurrency, baseline.BatchSize, baseline.Duration.Round(time.Millisecond), baseline.JobsPerSec, baseline.WorkerRuns, baseline.ClaimCalls, baseline.Synced, baseline.Dead, baseline.Retrying, baseline.Ghosts)
	for i := 1; i < len(results); i++ {
		r := results[i]
		relative := baseline.Duration.Seconds() / r.Duration.Seconds()
		fmt.Printf("case c=%d b=%d  duration=%s  jobs_per_sec=%.2f  speedup=%.2fx  worker_runs=%d  claim_calls=%d  synced=%d dead=%d retrying=%d ghosts=%d\n",
			r.Concurrency, r.BatchSize, r.Duration.Round(time.Millisecond), r.JobsPerSec, relative, r.WorkerRuns, r.ClaimCalls, r.Synced, r.Dead, r.Retrying, r.Ghosts)
	}

	fmt.Println()
	fmt.Println("Tip: keep seed, universe and mutations fixed while tuning concurrency and batch size.")
	return nil
}

func runBenchmarkCase(ctx context.Context, scenario benchmarkScenario, concurrency, batchSize int, lockFor time.Duration, workerIDBase string) (benchResult, error) {
	jobStore := newBenchJobStore(scenario.Jobs)
	target := scenario.Target.cloneEmpty()

	worker := &deltaflow.SyncWorker{
		JobStore:    jobStore,
		Projector:   deltaflow.ProjectorFunc(scenario.Source.project),
		Applier:     deltaflow.ProjectionApplierFunc(target.apply),
		SyncID:      deltaflow.SyncID("customers-bench"),
		WorkerID:    workerIDBase,
		LockFor:     lockFor,
		PullSize:    1,
		BatchSize:   batchSize,
		Concurrency: concurrency,
	}

	start := time.Now()
	workerRuns, err := drainWorker(ctx, worker, jobStore, len(scenario.Jobs))
	if err != nil {
		return benchResult{}, err
	}
	duration := time.Since(start)

	stats := jobStore.stats()
	jobsPerSec := float64(len(scenario.Jobs)) / duration.Seconds()
	return benchResult{
		Concurrency: concurrency,
		BatchSize:   batchSize,
		Duration:    duration,
		JobsPerSec:  jobsPerSec,
		WorkerRuns:  workerRuns,
		ClaimCalls:  stats.ClaimCalls,
		Synced:      stats.Synced,
		Dead:        stats.Dead,
		Retrying:    stats.Retrying,
		Ghosts:      stats.Ghosts,
	}, nil
}

func drainWorker(ctx context.Context, worker *deltaflow.SyncWorker, store *benchJobStore, totalJobs int) (int, error) {
	maxCycles := totalJobs + 100
	for cycle := 0; cycle < maxCycles; cycle++ {
		if err := worker.RunOnce(ctx); err != nil {
			return cycle + 1, err
		}
		if store.completedCount() >= totalJobs {
			return cycle + 1, nil
		}
	}
	return maxCycles, fmt.Errorf("drain did not complete after %d cycles (completed=%d/%d)", maxCycles, store.completedCount(), totalJobs)
}

func buildBenchmarkScenario(seed int64, universe, mutations, ghostEvery int) benchmarkScenario {
	rng := rand.New(rand.NewSource(seed))
	source := &sourceStore{customers: make(map[string]CustomerProfile, universe)}
	for i := 0; i < universe; i++ {
		id := fmt.Sprintf("cus-%06d", i+1)
		source.customers[id] = CustomerProfile{
			CustomerID:      id,
			FullName:        fmt.Sprintf("Customer %06d", i+1),
			Email:           fmt.Sprintf("customer-%06d@example.com", i+1),
			Country:         []string{"US", "AR", "MX", "BR"}[i%4],
			Segment:         []string{"customer", "vip", "prospect"}[i%3],
			NewsletterOptIn: i%2 == 0,
			UpdatedAt:       fixedTime(i + 1).Format(time.RFC3339),
		}
	}

	jobs := make([]deltaflow.SyncJob, 0, mutations)
	ghostCount := 0
	for i := 0; i < mutations; i++ {
		var customerID string
		if ghostEvery > 0 && (i+1)%ghostEvery == 0 {
			ghostCount++
			customerID = fmt.Sprintf("ghost-%06d", i+1)
		} else {
			pick := rng.Intn(universe) + 1
			customerID = fmt.Sprintf("cus-%06d", pick)
		}
		key := mustCustomerKey(customerID)
		now := fixedTime(i + 1)
		jobs = append(jobs, deltaflow.SyncJob{
			ID:             deltaflow.SyncJobID(fmt.Sprintf("job-%08d", i+1)),
			SyncID:         deltaflow.SyncID("customers-bench"),
			Origin:         deltaflow.JobOriginOutbox,
			ProjectionType: deltaflow.ProjectionType("Customer"),
			ProjectionKey:  key,
			State:          deltaflow.StatePending,
			AttemptCount:   0,
			MaxAttempts:    5,
			AvailableAt:    now,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
	}

	return benchmarkScenario{
		Name:       "in-memory worker throughput benchmark",
		Seed:       seed,
		Universe:   universe,
		Mutations:  mutations,
		GhostCount: ghostCount,
		Source:     source,
		Target:     &targetIndex{docs: make(map[string][]byte, universe)},
		Jobs:       jobs,
	}
}

func mustCustomerKey(customerID string) deltaflow.ProjectionKey {
	raw, err := json.Marshal(customerID)
	if err != nil {
		panic(err)
	}
	return deltaflow.ProjectionKey{"customer_id": raw}
}

func parseIntList(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	out := make([]int, 0, len(parts))
	seen := make(map[int]struct{}, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		n, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("%q is not an integer", trimmed)
		}
		if n <= 0 {
			return nil, fmt.Errorf("%q must be positive", trimmed)
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, errors.New("no values provided")
	}
	sort.Ints(out)
	return out, nil
}

type benchStoreStats struct {
	Synced     int
	Dead       int
	Retrying   int
	Ghosts     int
	ClaimCalls int
}

type benchJobStore struct {
	mu         sync.Mutex
	now        func() time.Time
	jobs       map[deltaflow.SyncJobID]*deltaflow.SyncJob
	orderedIDs []deltaflow.SyncJobID
	nextIndex  int
	completed  int
	claimCalls int
}

func newBenchJobStore(seedJobs []deltaflow.SyncJob) *benchJobStore {
	jobs := make(map[deltaflow.SyncJobID]*deltaflow.SyncJob, len(seedJobs))
	orderedIDs := make([]deltaflow.SyncJobID, 0, len(seedJobs))
	for i := range seedJobs {
		job := seedJobs[i]
		copyJob := job
		jobs[job.ID] = &copyJob
		orderedIDs = append(orderedIDs, job.ID)
	}
	sort.Slice(orderedIDs, func(i, j int) bool {
		left := jobs[orderedIDs[i]]
		right := jobs[orderedIDs[j]]
		if !left.AvailableAt.Equal(right.AvailableAt) {
			return left.AvailableAt.Before(right.AvailableAt)
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.ID < right.ID
	})
	return &benchJobStore{
		now:        func() time.Time { return time.Now().UTC() },
		jobs:       jobs,
		orderedIDs: orderedIDs,
		nextIndex:  0,
		completed:  0,
		claimCalls: 0,
	}
}

func (s *benchJobStore) Create(context.Context, deltaflow.SyncJob) (*deltaflow.SyncJob, error) {
	return nil, errors.New("not implemented in benchmark store")
}

func (s *benchJobStore) Get(ctx context.Context, jobID deltaflow.SyncJobID) (*deltaflow.SyncJob, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, false, nil
	}
	copyJob := *job
	copyJob.ProjectionKey = cloneProjectionKey(job.ProjectionKey)
	return &copyJob, true, nil
}

func (s *benchJobStore) ClaimNext(ctx context.Context, syncID deltaflow.SyncID, workerID string, lockFor time.Duration) (*deltaflow.SyncJob, error) {
	jobs, err := s.ClaimNextBatch(ctx, syncID, workerID, 1, lockFor)
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, nil
	}
	return jobs[0], nil
}

func (s *benchJobStore) ClaimNextBatch(ctx context.Context, syncID deltaflow.SyncID, workerID string, limit int, lockFor time.Duration) ([]*deltaflow.SyncJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lockFor <= 0 {
		return nil, deltaflow.ErrInvalidLockFor
	}
	if limit <= 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++

	now := s.now()
	claimed := make([]*deltaflow.SyncJob, 0, limit)
	claimLocked := func(id deltaflow.SyncJobID) bool {
		job, ok := s.jobs[id]
		if !ok || job.SyncID != syncID {
			return false
		}
		if !claimableJobState(job, now) {
			return false
		}
		job.State = deltaflow.StateProcessing
		job.LockedBy = stringPtr(workerID)
		until := now.Add(lockFor)
		job.LockedUntil = &until
		job.UpdatedAt = now

		copyJob := *job
		copyJob.ProjectionKey = cloneProjectionKey(job.ProjectionKey)
		claimed = append(claimed, &copyJob)
		return true
	}

	for len(claimed) < limit && s.nextIndex < len(s.orderedIDs) {
		id := s.orderedIDs[s.nextIndex]
		s.nextIndex++
		_ = claimLocked(id)
	}

	// Rare fallback path for jobs that can become claimable later (for example retrying).
	if len(claimed) < limit {
		ids := make([]deltaflow.SyncJobID, 0, len(s.jobs))
		for id, job := range s.jobs {
			if job.SyncID == syncID && claimableJobState(job, now) {
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
		for _, id := range ids {
			if len(claimed) >= limit {
				break
			}
			_ = claimLocked(id)
		}
	}

	return claimed, nil
}

func (s *benchJobStore) RenewLease(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, lockFor time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lockFor <= 0 {
		return deltaflow.ErrInvalidLockFor
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return deltaflow.ErrJobNotFound
	}
	if !leaseOwnedBy(job, workerID, s.now()) {
		return deltaflow.ErrJobLeaseNotOwned
	}
	until := s.now().Add(lockFor)
	job.LockedUntil = &until
	job.UpdatedAt = s.now()
	return nil
}

func (s *benchJobStore) MarkSynced(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, ghostDetected bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return deltaflow.ErrJobNotFound
	}
	if !leaseOwnedBy(job, workerID, s.now()) {
		return deltaflow.ErrJobLeaseNotOwned
	}
	now := s.now()
	wasTerminal := job.State == deltaflow.StateSynced || job.State == deltaflow.StateDead
	job.State = deltaflow.StateSynced
	job.GhostDetected = ghostDetected
	job.SyncedAt = &now
	job.LockedBy = nil
	job.LockedUntil = nil
	job.UpdatedAt = now
	if !wasTerminal {
		s.completed++
	}
	return nil
}

func (s *benchJobStore) MarkRetrying(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error, nextRunAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return deltaflow.ErrJobNotFound
	}
	if !leaseOwnedBy(job, workerID, s.now()) {
		return deltaflow.ErrJobLeaseNotOwned
	}
	now := s.now()
	job.State = deltaflow.StateRetrying
	job.AttemptCount++
	job.AvailableAt = nextRunAt.UTC()
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	job.LastError = &msg
	job.LockedBy = nil
	job.LockedUntil = nil
	job.UpdatedAt = now
	return nil
}

func (s *benchJobStore) RequeueClaimed(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, reason error, nextRunAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return deltaflow.ErrJobNotFound
	}
	if !leaseOwnedBy(job, workerID, s.now()) {
		return deltaflow.ErrJobLeaseNotOwned
	}
	now := s.now()
	job.State = deltaflow.StateRetrying
	job.AvailableAt = nextRunAt.UTC()
	msg := ""
	if reason != nil {
		msg = reason.Error()
	}
	job.LastError = &msg
	job.LockedBy = nil
	job.LockedUntil = nil
	job.UpdatedAt = now
	return nil
}

func (s *benchJobStore) MarkDead(ctx context.Context, jobID deltaflow.SyncJobID, workerID string, err error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return deltaflow.ErrJobNotFound
	}
	if !leaseOwnedBy(job, workerID, s.now()) {
		return deltaflow.ErrJobLeaseNotOwned
	}
	now := s.now()
	wasTerminal := job.State == deltaflow.StateSynced || job.State == deltaflow.StateDead
	job.State = deltaflow.StateDead
	job.AttemptCount++
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	job.LastError = &msg
	job.DeadAt = &now
	job.LockedBy = nil
	job.LockedUntil = nil
	job.UpdatedAt = now
	if !wasTerminal {
		s.completed++
	}
	return nil
}

func (s *benchJobStore) completedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completed
}

func (s *benchJobStore) stats() benchStoreStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := benchStoreStats{}
	for _, job := range s.jobs {
		switch job.State {
		case deltaflow.StateSynced:
			stats.Synced++
			if job.GhostDetected {
				stats.Ghosts++
			}
		case deltaflow.StateDead:
			stats.Dead++
		case deltaflow.StateRetrying:
			stats.Retrying++
		}
	}
	stats.ClaimCalls = s.claimCalls
	return stats
}

func claimableJobState(job *deltaflow.SyncJob, now time.Time) bool {
	switch job.State {
	case deltaflow.StatePending, deltaflow.StateRetrying:
		return !job.AvailableAt.After(now)
	case deltaflow.StateProcessing:
		return job.LockedUntil == nil || !job.LockedUntil.After(now)
	default:
		return false
	}
}

func leaseOwnedBy(job *deltaflow.SyncJob, workerID string, now time.Time) bool {
	if job.State != deltaflow.StateProcessing || job.LockedBy == nil || job.LockedUntil == nil {
		return false
	}
	if *job.LockedBy != workerID {
		return false
	}
	return job.LockedUntil.After(now)
}

func stringPtr(value string) *string {
	return &value
}

func cloneProjectionKey(key deltaflow.ProjectionKey) deltaflow.ProjectionKey {
	if key == nil {
		return nil
	}
	out := make(deltaflow.ProjectionKey, len(key))
	for k, v := range key {
		if v == nil {
			out[k] = nil
			continue
		}
		out[k] = append([]byte(nil), v...)
	}
	return out
}
