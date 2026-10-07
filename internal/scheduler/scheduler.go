package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

// Handler executes a task payload. Returning an error makes the task retryable.
type Handler func(context.Context, task.Task) error
type LeaderGate interface{ IsLeader() bool }

var ErrNoTask = errors.New("no task available")
var ErrBackpressure = errors.New("scheduler queue is full")

type Worker = store.Worker

type Metrics struct {
	Submitted, Succeeded, Failed, Retried, Canceled, LeaseExpired uint64
}

var queueLatencyBounds = [...]float64{0.1, 1, 5, 10, 15, 30, 60}

// task.NewID names tasks submitted in process. It carries a per-process tag,
// so ids stay distinct from the API layer's and from another instance sharing
// the same store.

type Scheduler struct {
	store                                           store.Store
	workers                                         int
	interval                                        time.Duration
	handlers                                        map[string]Handler
	queue                                           chan task.Task
	stop                                            chan struct{}
	done                                            chan struct{}
	cancel                                          context.CancelFunc
	mu                                              sync.Mutex
	running                                         map[string]context.CancelFunc
	workersByID                                     map[string]Worker
	leaseTTL                                        time.Duration
	maxPending                                      int
	leaderGate                                      LeaderGate
	stopped                                         bool
	submitted, succeeded, failed, retried, canceled uint64
	leaseExpired                                    uint64
	latencyMu                                       sync.Mutex
	queueLatencyBuckets                             [8]uint64
	queueLatencySum                                 float64
	queueLatencyCount                               uint64
}

func New(s store.Store, workers int) *Scheduler {
	if workers < 1 {
		workers = 1
	}
	return &Scheduler{store: s, workers: workers, interval: 100 * time.Millisecond, handlers: make(map[string]Handler), queue: make(chan task.Task, workers*2), stop: make(chan struct{}), done: make(chan struct{}), running: make(map[string]context.CancelFunc), workersByID: make(map[string]Worker), leaseTTL: 30 * time.Second, maxPending: workers * 100}
}

func (s *Scheduler) SetMaxPending(limit int) {
	if limit > 0 {
		s.maxPending = limit
	}
}

func (s *Scheduler) SetLeaseTTL(ttl time.Duration) {
	if ttl > 0 {
		s.leaseTTL = ttl
	}
}

func (s *Scheduler) SetLeaderGate(g LeaderGate) { s.leaderGate = g }

func (s *Scheduler) RegisterWorker(id string) (Worker, error) {
	if registry, ok := s.store.(store.WorkerRegistry); ok {
		w, err := registry.RegisterWorker(id, s.leaseTTL)
		if err != nil {
			return Worker{}, err
		}
		s.mu.Lock()
		s.workersByID[id] = w
		s.mu.Unlock()
		return w, nil
	}
	if id == "" {
		return Worker{}, errors.New("worker id is required")
	}
	now := time.Now()
	w := Worker{ID: id, LastHeartbeat: now, LeaseUntil: now.Add(s.leaseTTL)}
	s.mu.Lock()
	s.workersByID[id] = w
	s.mu.Unlock()
	return w, nil
}

func (s *Scheduler) Heartbeat(id string) (Worker, error) {
	if registry, ok := s.store.(store.WorkerRegistry); ok {
		w, err := registry.HeartbeatWorker(id, s.leaseTTL)
		if err != nil {
			return Worker{}, err
		}
		s.mu.Lock()
		s.workersByID[id] = w
		s.mu.Unlock()
		_ = registry.RenewWorkerTaskLeases(id, w.LeaseUntil)
		return w, nil
	}
	s.mu.Lock()
	w, ok := s.workersByID[id]
	if !ok {
		s.mu.Unlock()
		return Worker{}, errors.New("worker not registered")
	}
	now := time.Now()
	w.LastHeartbeat, w.LeaseUntil = now, now.Add(s.leaseTTL)
	s.workersByID[id] = w
	s.mu.Unlock()
	// Renew leases for tasks owned by this worker as part of its heartbeat.
	items, _ := s.store.List()
	for _, t := range items {
		if t.Status == task.StatusRunning && t.WorkerID == id {
			t.LeaseUntil = timePtr(w.LeaseUntil)
			if updater, ok := s.store.(store.ConditionalUpdater); ok {
				_ = updater.UpdateIfLease(t.ID, t.LeaseToken, t)
			} else {
				_ = s.store.Update(t)
			}
		}
	}
	return w, nil
}

func (s *Scheduler) Workers() []Worker {
	if registry, ok := s.store.(store.WorkerRegistry); ok {
		workers, err := registry.ListWorkers(time.Now())
		if err == nil {
			return workers
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	result := make([]Worker, 0, len(s.workersByID))
	for _, w := range s.workersByID {
		if w.LeaseUntil.After(now) {
			result = append(result, w)
		}
	}
	return result
}

func (s *Scheduler) Register(name string, h Handler) error {
	if name == "" || h == nil {
		return errors.New("handler name and function are required")
	}
	s.mu.Lock()
	s.handlers[name] = h
	s.mu.Unlock()
	return nil
}

func (s *Scheduler) Submit(t task.Task) error {
	if t.IdempotencyKey != "" {
		if existing, err := s.FindByIdempotencyKey(t.IdempotencyKey); err == nil {
			return fmt.Errorf("duplicate task %s: %w", existing.ID, store.ErrDuplicateIdempotencyKey)
		}
	}
	if s.maxPending > 0 {
		inFlight, err := s.inFlightCount()
		if err != nil {
			return err
		}
		if inFlight >= s.maxPending {
			return ErrBackpressure
		}
	}
	if t.ID == "" {
		t.ID = task.NewID()
	}
	if t.RunAt.IsZero() {
		t.RunAt = time.Now()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	if t.Status == "" {
		t.Status = task.StatusPending
	}
	if t.Retry.MaxAttempts < 1 {
		t.Retry.MaxAttempts = 1
	}
	for _, dep := range t.DependsOn {
		if dep == t.ID {
			return errors.New("task cannot depend on itself")
		}
		if _, err := s.store.Get(dep); err != nil {
			return fmt.Errorf("dependency %s not found", dep)
		}
		if s.dependsOn(dep, t.ID, map[string]bool{}) {
			return fmt.Errorf("dependency cycle detected through %s", dep)
		}
	}
	err := s.store.Create(t)
	if err == nil {
		atomic.AddUint64(&s.submitted, 1)
	}
	return err
}

// FindByIdempotencyKey returns the task previously accepted for a retry key.
func (s *Scheduler) FindByIdempotencyKey(key string) (task.Task, error) {
	if key == "" {
		return task.Task{}, store.ErrNotFound
	}
	if finder, ok := s.store.(interface {
		GetByIdempotencyKey(string) (task.Task, error)
	}); ok {
		return finder.GetByIdempotencyKey(key)
	}
	items, err := s.store.List()
	if err != nil {
		return task.Task{}, err
	}
	for _, item := range items {
		if item.IdempotencyKey == key {
			return item, nil
		}
	}
	return task.Task{}, store.ErrNotFound
}

func (s *Scheduler) dependsOn(id, target string, seen map[string]bool) bool {
	if id == target {
		return true
	}
	if seen[id] {
		return false
	}
	seen[id] = true
	t, err := s.store.Get(id)
	if err != nil {
		return false
	}
	for _, dep := range t.DependsOn {
		if s.dependsOn(dep, target, seen) {
			return true
		}
	}
	return false
}

func (s *Scheduler) Get(id string) (task.Task, error) { return s.store.Get(id) }

func (s *Scheduler) List() ([]task.Task, error) { return s.store.List() }

func (s *Scheduler) Metrics() Metrics {
	return Metrics{Submitted: atomic.LoadUint64(&s.submitted), Succeeded: atomic.LoadUint64(&s.succeeded), Failed: atomic.LoadUint64(&s.failed), Retried: atomic.LoadUint64(&s.retried), Canceled: atomic.LoadUint64(&s.canceled), LeaseExpired: atomic.LoadUint64(&s.leaseExpired)}
}

func (s *Scheduler) QueueLatencyHistogram() ([8]uint64, float64, uint64) {
	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()
	return s.queueLatencyBuckets, s.queueLatencySum, s.queueLatencyCount
}

func (s *Scheduler) observeQueueLatency(t task.Task) {
	seconds := time.Since(t.RunAt).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	s.latencyMu.Lock()
	for i, bound := range queueLatencyBounds {
		if seconds <= bound {
			s.queueLatencyBuckets[i]++
		}
	}
	s.queueLatencyBuckets[len(queueLatencyBounds)]++
	s.queueLatencySum += seconds
	s.queueLatencyCount++
	s.latencyMu.Unlock()
}

// inFlightCount sizes the queue for backpressure. Stores that can answer
// without materialising every record are asked directly; the listing fallback
// keeps custom stores working, at a cost that grows with the queue.
func (s *Scheduler) inFlightCount() (int, error) {
	if counter, ok := s.store.(store.InFlightCounter); ok {
		return counter.CountInFlight()
	}
	items, err := s.store.List()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, item := range items {
		if store.IsInFlight(item.Status) {
			count++
		}
	}
	return count, nil
}

// Claim reserves one eligible task for an external worker. The returned lease
// token must be supplied to CompleteTask.
func (s *Scheduler) Claim(workerID string) (task.Task, error) {
	if s.leaderGate != nil && !s.leaderGate.IsLeader() {
		return task.Task{}, errors.New("scheduler is not leader")
	}
	if !s.workerHealthy(workerID) {
		return task.Task{}, errors.New("worker is not registered or lease expired")
	}
	if claimer, ok := s.store.(store.AtomicClaimer); ok {
		claimed, err := claimer.ClaimDue(workerID, s.leaseTTL)
		if errors.Is(err, store.ErrNoTaskAvailable) {
			return task.Task{}, ErrNoTask
		}
		if err == nil {
			s.observeQueueLatency(claimed)
		}
		return claimed, err
	}
	items, err := s.store.List()
	if err != nil {
		return task.Task{}, err
	}
	now := time.Now()
	// skip records the candidates this claim has already ruled out, so the next
	// pass picks the following one instead of retrying the same task.
	skip := make([]bool, len(items))
	for {
		index, ok := bestDue(items, now, skip)
		if !ok {
			return task.Task{}, ErrNoTask
		}
		skip[index] = true
		t := items[index]
		ready, dependencyErr := s.dependenciesReady(t)
		if dependencyErr != "" {
			s.failUnreadyTask(t, dependencyErr)
			continue
		}
		if !ready {
			continue
		}
		s.mu.Lock()
		_, already := s.running[t.ID]
		if already {
			s.mu.Unlock()
			continue
		}
		s.running[t.ID] = nil
		s.mu.Unlock()
		claimed, err := s.commitClaim(workerID, t.ID)
		if err != nil {
			s.mu.Lock()
			delete(s.running, t.ID)
			s.mu.Unlock()
			if errors.Is(err, errClaimLost) {
				continue
			}
			return task.Task{}, err
		}
		s.observeQueueLatency(claimed)
		return claimed, nil
	}
}

// bestDue returns the index of the next claimable candidate, ordered the way the
// PostgreSQL claim orders its candidates: priority first, then the scheduled
// time, then the id as a deterministic tie-break. It scans once and copies
// nothing, where store.Due would copy and sort the whole queue for every claim.
func bestDue(tasks []task.Task, now time.Time, skip []bool) (int, bool) {
	best := -1
	for i, t := range tasks {
		claimable := t.Status == task.StatusPending || t.Status == task.StatusRetrying
		if skip[i] || !claimable || t.RunAt.After(now) {
			continue
		}
		if best < 0 || dueBefore(t, tasks[best]) {
			best = i
		}
	}
	return best, best >= 0
}

func dueBefore(a, b task.Task) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if !a.RunAt.Equal(b.RunAt) {
		return a.RunAt.Before(b.RunAt)
	}
	return a.ID < b.ID
}

// errClaimLost reports that a candidate stopped being claimable between the
// listing and the commit, so the caller should move on to the next candidate.
var errClaimLost = errors.New("task is no longer claimable")

// commitClaim turns one listed candidate into a running task and returns the
// stored record. The listing Claim walks is a snapshot, so by the time a worker
// commits to a task a concurrent claimant may already have claimed and even
// completed it. Re-reading the record and writing it back under a state guard
// keeps the claim atomic, which is what stops a stale snapshot from resurrecting
// a finished task and handing the same task to several workers.
func (s *Scheduler) commitClaim(workerID, id string) (task.Task, error) {
	current, err := s.store.Get(id)
	if err != nil {
		return task.Task{}, errClaimLost
	}
	if current.Status != task.StatusPending && current.Status != task.StatusRetrying {
		return task.Task{}, errClaimLost
	}
	now := time.Now()
	if current.RunAt.After(now) {
		return task.Task{}, errClaimLost
	}
	previousStatus, previousToken := current.Status, current.LeaseToken
	current.Status, current.Attempts, current.StartedAt = task.StatusRunning, current.Attempts+1, &now
	current.WorkerID, current.LeaseUntil, current.LeaseToken = workerID, timePtr(now.Add(s.leaseTTL)), fmt.Sprintf("%d-%s", now.UnixNano(), id)
	var updateErr error
	if updater, ok := s.store.(store.ConditionalUpdater); ok {
		updateErr = updater.UpdateIfState(id, previousStatus, previousToken, current)
	} else {
		updateErr = s.store.Update(current)
	}
	if updateErr != nil {
		return task.Task{}, errClaimLost
	}
	return current, nil
}

// failUnreadyTask fails a task whose dependency can no longer succeed. The write
// is guarded by the state the task was listed in, so a task that was claimed in
// the meantime is left to run instead of being failed underneath its worker.
func (s *Scheduler) failUnreadyTask(t task.Task, reason string) {
	now := time.Now()
	previousStatus, previousToken := t.Status, t.LeaseToken
	t.Status, t.LastError, t.FinishedAt = task.StatusFailed, reason, &now
	if updater, ok := s.store.(store.ConditionalUpdater); ok {
		_ = updater.UpdateIfState(t.ID, previousStatus, previousToken, t)
		return
	}
	_ = s.store.Update(t)
}

func (s *Scheduler) CompleteTask(workerID, id, token, failure string) error {
	return s.CompleteTaskWithResult(workerID, id, token, failure, nil)
}

// RenewTaskLease extends one task lease after validating its owner and token.
func (s *Scheduler) RenewTaskLease(workerID, id, token string) (task.Task, error) {
	t, err := s.store.Get(id)
	if err != nil {
		return task.Task{}, err
	}
	if !validLease(t, workerID, token, time.Now()) {
		return task.Task{}, errors.New("invalid task lease")
	}
	until := time.Now().Add(s.leaseTTL)
	t.LeaseUntil = &until
	var updateErr error
	if updater, ok := s.store.(store.ConditionalUpdater); ok {
		updateErr = updater.UpdateIfLease(t.ID, token, t)
	} else {
		updateErr = s.store.Update(t)
	}
	if updateErr != nil {
		return task.Task{}, updateErr
	}
	return t, nil
}

func (s *Scheduler) CompleteTaskWithResult(workerID, id, token, failure string, result []byte) error {
	t, err := s.store.Get(id)
	if err != nil {
		return err
	}
	if !validLease(t, workerID, token, time.Now()) {
		return errors.New("invalid task lease")
	}
	if failure != "" {
		err = errors.New(failure)
	} else {
		err = nil
	}
	if err == nil {
		t.Result = append([]byte(nil), result...)
	}
	if err := s.finish(t, err); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.running, id)
	s.mu.Unlock()
	return nil
}

func validLease(t task.Task, workerID, token string, now time.Time) bool {
	return t.Status == task.StatusRunning && t.WorkerID == workerID && t.LeaseToken == token && t.LeaseUntil != nil && t.LeaseUntil.After(now)
}

func (s *Scheduler) workerHealthy(id string) bool {
	if registry, ok := s.store.(store.WorkerRegistry); ok {
		w, err := registry.GetWorker(id)
		return err == nil && w.LeaseUntil.After(time.Now())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workersByID[id]
	return ok && w.LeaseUntil.After(time.Now())
}

func (s *Scheduler) Cancel(id string) error {
	t, err := s.store.Get(id)
	if err != nil {
		return err
	}
	if t.IsTerminal() {
		return nil
	}
	s.mu.Lock()
	if cancel, ok := s.running[id]; ok {
		if cancel != nil {
			cancel()
		}
		delete(s.running, id)
	}
	s.mu.Unlock()
	now := time.Now()
	expectedStatus, expectedToken := t.Status, t.LeaseToken
	t.Status, t.LastError, t.FinishedAt = task.StatusCanceled, task.ErrCanceled.Error(), &now
	if updater, ok := s.store.(store.ConditionalUpdater); ok {
		err = updater.UpdateIfState(id, expectedStatus, expectedToken, t)
	} else {
		err = s.store.Update(t)
	}
	if err == nil {
		atomic.AddUint64(&s.canceled, 1)
	}
	return err
}

// Purge removes a task record entirely. Unlike Cancel it leaves no trace, so it
// refuses to touch a task that may still be executing: a worker holding the
// lease would keep running and then fail to complete against a missing record.
func (s *Scheduler) Purge(id string) error {
	t, err := s.store.Get(id)
	if err != nil {
		return err
	}
	if t.Status == task.StatusRunning {
		return fmt.Errorf("task %s is running: cancel it and wait for the lease to expire first", id)
	}
	return s.store.Delete(id)
}

func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.cancel != nil && !s.stopped {
		s.mu.Unlock()
		return
	}
	s.done = make(chan struct{})
	s.stopped = false
	ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()
	workerPrefix := fmt.Sprintf("local-%d", time.Now().UnixNano())
	for i := 0; i < s.workers; i++ {
		_, _ = s.RegisterWorker(fmt.Sprintf("%s-%d", workerPrefix, i))
	}
	if _, shared := s.store.(store.AtomicClaimer); !shared {
		s.recoverRunning()
	}
	go s.loop(ctx)
}

func (s *Scheduler) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	if cancel == nil || s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()
	cancel()
	<-s.done
}

func (s *Scheduler) requeueRunningOnStop() {
	items, err := s.store.List()
	if err != nil {
		return
	}
	now := time.Now()
	for _, t := range items {
		if t.Status != task.StatusRunning || !s.isLocalWorker(t.WorkerID) {
			continue
		}
		expectedToken := t.LeaseToken
		t.Status = task.StatusRetrying
		t.RunAt = now
		t.LastError = "scheduler stopped while task was running"
		t.WorkerID, t.LeaseUntil, t.LeaseToken = "", nil, ""
		if updater, ok := s.store.(store.ConditionalUpdater); ok {
			_ = updater.UpdateIfState(t.ID, task.StatusRunning, expectedToken, t)
		} else {
			_ = s.store.Update(t)
		}
	}
}

func (s *Scheduler) isLocalWorker(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.workersByID[id]
	return ok && strings.HasPrefix(id, "local-")
}

func (s *Scheduler) loop(ctx context.Context) {
	defer func() {
		// Context cancellation can happen without an explicit Stop call. Keep
		// lifecycle state consistent and make interrupted work retryable.
		s.requeueRunningOnStop()
		s.mu.Lock()
		s.stopped = true
		s.cancel = nil
		done := s.done
		s.mu.Unlock()
		close(done)
	}()
	for i := 0; i < s.workers; i++ {
		go s.worker(ctx)
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	heartbeatInterval := s.leaseTTL / 3
	if heartbeatInterval <= 0 {
		heartbeatInterval = time.Millisecond
	}
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.dispatch()
		case <-heartbeatTicker.C:
			s.mu.Lock()
			localWorkers := make([]string, 0, len(s.workersByID))
			for id := range s.workersByID {
				if strings.HasPrefix(id, "local-") {
					localWorkers = append(localWorkers, id)
				}
			}
			s.mu.Unlock()
			for _, id := range localWorkers {
				_, _ = s.Heartbeat(id)
			}
		}
	}
}

// dueTasks returns the tasks this tick should consider dispatching. A store
// that can answer with an index-backed query does, because listing the whole
// table costs time proportional to the queue on every tick, including all the
// finished work a running deployment accumulates.
func (s *Scheduler) dueTasks() ([]task.Task, error) {
	now := time.Now()
	if scanner, ok := s.store.(store.TaskScanner); ok {
		return scanner.ListDue(now)
	}
	items, err := s.store.List()
	if err != nil {
		return nil, err
	}
	return store.Due(items, now), nil
}

// expiredCandidates feeds reapExpired. Stores that can name the lapsed tasks
// directly do; the others get a full listing, and reapExpired filters whatever
// it receives, so both paths reap exactly the same tasks.
func (s *Scheduler) expiredCandidates() ([]task.Task, error) {
	if scanner, ok := s.store.(store.TaskScanner); ok {
		return scanner.ListExpired(time.Now())
	}
	return s.store.List()
}

func (s *Scheduler) dispatch() {
	if s.leaderGate != nil && !s.leaderGate.IsLeader() {
		return
	}
	s.reapExpired()
	items, err := s.dueTasks()
	if err != nil {
		return
	}
	for _, t := range items {
		ready, dependencyErr := s.dependenciesReady(t)
		if dependencyErr != "" {
			s.failUnreadyTask(t, dependencyErr)
			continue
		}
		if !ready {
			continue
		}
		s.mu.Lock()
		_, already := s.running[t.ID]
		hasHandler := s.handlers[t.Name] != nil
		if !already && hasHandler {
			s.running[t.ID] = nil // reserve the task before enqueueing
		}
		s.mu.Unlock()
		if already || !hasHandler {
			continue
		}
		now := time.Now()
		expectedStatus, expectedToken := t.Status, t.LeaseToken
		t.Status = task.StatusRunning
		t.Attempts++
		t.StartedAt = &now
		t.LeaseUntil = timePtr(now.Add(s.leaseTTL))
		t.LeaseToken = fmt.Sprintf("%d-%s", now.UnixNano(), t.ID)
		t.WorkerID = s.pickWorker()
		var updateErr error
		if updater, ok := s.store.(store.ConditionalUpdater); ok {
			updateErr = updater.UpdateIfState(t.ID, expectedStatus, expectedToken, t)
		} else {
			updateErr = s.store.Update(t)
		}
		if updateErr == nil {
			s.observeQueueLatency(t)
			select {
			case s.queue <- t:
			default:
				// Leave the task retryable when all workers are busy. Guard the
				// rollback exactly like the claim above it: the task can be
				// canceled or reaped while it waits for a free worker, and a
				// blind write would put it back in the queue behind our back.
				t.Status = task.StatusPending
				t.Attempts--
				if updater, ok := s.store.(store.ConditionalUpdater); ok {
					_ = updater.UpdateIfState(t.ID, task.StatusRunning, t.LeaseToken, t)
				} else {
					_ = s.store.Update(t)
				}
				s.mu.Lock()
				delete(s.running, t.ID)
				s.mu.Unlock()
			}
		} else {
			s.mu.Lock()
			delete(s.running, t.ID)
			s.mu.Unlock()
		}
	}
}

func (s *Scheduler) dependenciesReady(t task.Task) (bool, string) {
	for _, id := range t.DependsOn {
		dep, err := s.store.Get(id)
		if err != nil {
			return false, fmt.Sprintf("dependency %s not found", id)
		}
		if dep.Status == task.StatusFailed || dep.Status == task.StatusCanceled {
			return false, fmt.Sprintf("dependency %s did not succeed", id)
		}
		if dep.Status != task.StatusSuccess {
			return false, ""
		}
	}
	return true, ""
}

func (s *Scheduler) worker(parent context.Context) {
	for {
		select {
		case <-parent.Done():
			return
		case t := <-s.queue:
			s.execute(parent, t)
		}
	}
}

func (s *Scheduler) execute(parent context.Context, t task.Task) {
	current, err := s.store.Get(t.ID)
	if err != nil || current.Status != task.StatusRunning || current.LeaseToken != t.LeaseToken {
		s.mu.Lock()
		delete(s.running, t.ID)
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	h := s.handlers[t.Name]
	ctx, cancel := context.WithCancel(parent)
	s.running[t.ID] = cancel
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.running, t.ID)
		s.mu.Unlock()
		cancel()
	}()
	if t.Timeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, t.Timeout)
		defer timeoutCancel()
	}
	err = h(ctx, t)
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	current, getErr := s.store.Get(t.ID)
	if getErr != nil || current.Status != task.StatusRunning || current.LeaseToken != t.LeaseToken {
		return
	}
	s.finish(current, err)
}

func (s *Scheduler) finish(current task.Task, runErr error) error {
	expectedToken := current.LeaseToken
	metric := ""
	now := time.Now()
	current.RunCount++
	current.FinishedAt = &now
	current.LeaseUntil, current.WorkerID, current.LeaseToken = nil, "", ""
	if runErr == nil {
		current.LastError = ""
		if current.Schedule > 0 {
			current.Status, current.RunAt, current.FinishedAt = task.StatusPending, now.Add(current.Schedule), nil
			current.Attempts = 0
			current.StartedAt = nil
		} else {
			current.Status = task.StatusSuccess
		}
		metric = "succeeded"
	} else if current.Attempts < current.Retry.MaxAttempts {
		current.Status = task.StatusRetrying
		current.LastError = runErr.Error()
		current.RunAt = now.Add(current.Retry.Backoff)
		current.FinishedAt = nil
		metric = "retried"
	} else {
		current.Status, current.LastError = task.StatusFailed, runErr.Error()
		metric = "failed"
	}
	var updateErr error
	if updater, ok := s.store.(store.ConditionalUpdater); ok {
		updateErr = updater.UpdateIfState(current.ID, task.StatusRunning, expectedToken, current)
	} else {
		updateErr = s.store.Update(current)
	}
	if updateErr != nil {
		return updateErr
	}
	switch metric {
	case "succeeded":
		atomic.AddUint64(&s.succeeded, 1)
	case "retried":
		atomic.AddUint64(&s.retried, 1)
	case "failed":
		atomic.AddUint64(&s.failed, 1)
	}
	return nil
}

func (s *Scheduler) pickWorker() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, w := range s.workersByID {
		if w.LeaseUntil.After(now) {
			return id
		}
	}
	return ""
}

func (s *Scheduler) reapExpired() {
	items, err := s.expiredCandidates()
	if err != nil {
		return
	}
	now := time.Now()
	for _, t := range items {
		if t.Status != task.StatusRunning || t.LeaseUntil == nil || t.LeaseUntil.After(now) {
			continue
		}
		if t.IsTerminal() {
			continue
		}
		s.mu.Lock()
		if cancel, ok := s.running[t.ID]; ok && cancel != nil {
			cancel()
		}
		delete(s.running, t.ID)
		s.mu.Unlock()
		expectedToken := t.LeaseToken
		t.Status, t.RunAt, t.WorkerID, t.LeaseUntil, t.LeaseToken = task.StatusRetrying, now, "", nil, ""
		t.LastError = "worker lease expired"
		// The items above are a snapshot taken before this loop started, so the
		// worker may have completed the task while we were iterating. Writing the
		// snapshot back unconditionally would overwrite a finished task with
		// "retrying" and let it run a second time. Guarding the write on "still
		// running with the same lease token" makes the reap a no-op once the worker
		// has finished, and counts only a real expiry.
		if updater, ok := s.store.(store.ConditionalUpdater); ok {
			if updater.UpdateIfState(t.ID, task.StatusRunning, expectedToken, t) == nil {
				atomic.AddUint64(&s.leaseExpired, 1)
			}
		} else {
			current, getErr := s.store.Get(t.ID)
			if getErr != nil || current.Status != task.StatusRunning || current.LeaseToken != expectedToken {
				continue
			}
			if s.store.Update(t) == nil {
				atomic.AddUint64(&s.leaseExpired, 1)
			}
		}
	}
}

// recoverRunning converts in-flight tasks from a previous process into
// retryable work. Lease tokens are intentionally process-local and therefore
// cannot survive a restart.
func (s *Scheduler) recoverRunning() {
	items, err := s.store.List()
	if err != nil {
		return
	}
	now := time.Now()
	for _, t := range items {
		if t.Status != task.StatusRunning {
			continue
		}
		t.Status, t.RunAt = task.StatusRetrying, now
		t.WorkerID, t.LeaseUntil, t.LeaseToken = "", nil, ""
		t.LastError = "scheduler restarted while task was running"
		_ = s.store.Update(t)
	}
}

func timePtr(t time.Time) *time.Time { return &t }
