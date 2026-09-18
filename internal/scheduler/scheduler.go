package scheduler

import (
	"context"
	"errors"
	"fmt"
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

type Worker struct {
	ID            string    `json:"id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	LeaseUntil    time.Time `json:"lease_until"`
}

type Metrics struct{ Submitted, Succeeded, Failed, Retried, Canceled uint64 }

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
			_ = s.store.Update(t)
		}
	}
	return w, nil
}

func (s *Scheduler) Workers() []Worker {
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
	if s.maxPending > 0 {
		items, err := s.store.List()
		if err != nil {
			return err
		}
		inFlight := 0
		for _, item := range items {
			if item.Status == task.StatusPending || item.Status == task.StatusRetrying || item.Status == task.StatusRunning {
				inFlight++
			}
		}
		if inFlight >= s.maxPending {
			return ErrBackpressure
		}
	}
	if t.ID == "" {
		t.ID = fmt.Sprintf("task-%d", time.Now().UnixNano())
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
	return Metrics{Submitted: atomic.LoadUint64(&s.submitted), Succeeded: atomic.LoadUint64(&s.succeeded), Failed: atomic.LoadUint64(&s.failed), Retried: atomic.LoadUint64(&s.retried), Canceled: atomic.LoadUint64(&s.canceled)}
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
	items, err := s.store.List()
	if err != nil {
		return task.Task{}, err
	}
	for _, t := range store.Due(items, time.Now()) {
		ready, dependencyErr := s.dependenciesReady(t)
		if dependencyErr != "" {
			now := time.Now()
			t.Status, t.LastError, t.FinishedAt = task.StatusFailed, dependencyErr, &now
			_ = s.store.Update(t)
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
		now := time.Now()
		t.Status, t.Attempts, t.StartedAt = task.StatusRunning, t.Attempts+1, &now
		t.WorkerID, t.LeaseUntil, t.LeaseToken = workerID, timePtr(now.Add(s.leaseTTL)), fmt.Sprintf("%d-%s", now.UnixNano(), t.ID)
		if err := s.store.Update(t); err != nil {
			s.mu.Lock()
			delete(s.running, t.ID)
			s.mu.Unlock()
			return task.Task{}, err
		}
		return t, nil
	}
	return task.Task{}, ErrNoTask
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
	if err := s.store.Update(t); err != nil {
		return task.Task{}, err
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
	s.finish(t, err)
	s.mu.Lock()
	delete(s.running, id)
	s.mu.Unlock()
	return nil
}

func validLease(t task.Task, workerID, token string, now time.Time) bool {
	return t.Status == task.StatusRunning && t.WorkerID == workerID && t.LeaseToken == token && t.LeaseUntil != nil && t.LeaseUntil.After(now)
}

func (s *Scheduler) workerHealthy(id string) bool {
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
	t.Status, t.LastError, t.FinishedAt = task.StatusCanceled, task.ErrCanceled.Error(), &now
	err = s.store.Update(t)
	if err == nil {
		atomic.AddUint64(&s.canceled, 1)
	}
	return err
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
	for i := 0; i < s.workers; i++ {
		_, _ = s.RegisterWorker(fmt.Sprintf("local-%d", i))
	}
	s.recoverRunning()
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
		if t.Status != task.StatusRunning {
			continue
		}
		t.Status = task.StatusRetrying
		t.RunAt = now
		t.LastError = "scheduler stopped while task was running"
		t.WorkerID, t.LeaseUntil, t.LeaseToken = "", nil, ""
		_ = s.store.Update(t)
	}
}

func (s *Scheduler) loop(ctx context.Context) {
	defer func() {
		// Context cancellation can happen without an explicit Stop call. Keep
		// lifecycle state consistent and make interrupted work retryable.
		s.requeueRunningOnStop()
		s.mu.Lock()
		s.stopped = true
		s.cancel = nil
		s.mu.Unlock()
		close(s.done)
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
			for i := 0; i < s.workers; i++ {
				_, _ = s.Heartbeat(fmt.Sprintf("local-%d", i))
			}
		}
	}
}

func (s *Scheduler) dispatch() {
	if s.leaderGate != nil && !s.leaderGate.IsLeader() {
		return
	}
	s.reapExpired()
	items, err := s.store.List()
	if err != nil {
		return
	}
	for _, t := range store.Due(items, time.Now()) {
		ready, dependencyErr := s.dependenciesReady(t)
		if dependencyErr != "" {
			now := time.Now()
			t.Status, t.LastError, t.FinishedAt = task.StatusFailed, dependencyErr, &now
			_ = s.store.Update(t)
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
		t.Status = task.StatusRunning
		t.Attempts++
		t.StartedAt = &now
		t.LeaseUntil = timePtr(now.Add(s.leaseTTL))
		t.LeaseToken = fmt.Sprintf("%d-%s", now.UnixNano(), t.ID)
		t.WorkerID = s.pickWorker()
		if s.store.Update(t) == nil {
			select {
			case s.queue <- t:
			default:
				// Leave the task retryable when all workers are busy.
				t.Status = task.StatusPending
				t.Attempts--
				_ = s.store.Update(t)
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

func (s *Scheduler) finish(current task.Task, err error) {
	now := time.Now()
	current.RunCount++
	current.FinishedAt = &now
	current.LeaseUntil, current.WorkerID, current.LeaseToken = nil, "", ""
	if err == nil {
		current.LastError = ""
		if current.Schedule > 0 {
			current.Status, current.RunAt, current.FinishedAt = task.StatusPending, now.Add(current.Schedule), nil
			current.Attempts = 0
			current.StartedAt = nil
		} else {
			current.Status = task.StatusSuccess
		}
		atomic.AddUint64(&s.succeeded, 1)
	} else if current.Attempts < current.Retry.MaxAttempts {
		current.Status = task.StatusRetrying
		current.LastError = err.Error()
		current.RunAt = now.Add(current.Retry.Backoff)
		current.FinishedAt = nil
		atomic.AddUint64(&s.retried, 1)
	} else {
		current.Status, current.LastError = task.StatusFailed, err.Error()
		atomic.AddUint64(&s.failed, 1)
	}
	_ = s.store.Update(current)
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
	items, err := s.store.List()
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
		t.Status, t.RunAt, t.WorkerID, t.LeaseUntil, t.LeaseToken = task.StatusRetrying, now, "", nil, ""
		t.LastError = "worker lease expired"
		_ = s.store.Update(t)
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
