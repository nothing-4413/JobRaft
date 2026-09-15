package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

// Handler executes a task payload. Returning an error makes the task retryable.
type Handler func(context.Context, task.Task) error

type Worker struct {
	ID            string    `json:"id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	LeaseUntil    time.Time `json:"lease_until"`
}

type Scheduler struct {
	store       store.Store
	workers     int
	interval    time.Duration
	handlers    map[string]Handler
	queue       chan task.Task
	stop        chan struct{}
	done        chan struct{}
	cancel      context.CancelFunc
	mu          sync.Mutex
	running     map[string]context.CancelFunc
	workersByID map[string]Worker
	leaseTTL    time.Duration
}

func New(s store.Store, workers int) *Scheduler {
	if workers < 1 {
		workers = 1
	}
	return &Scheduler{store: s, workers: workers, interval: 100 * time.Millisecond, handlers: make(map[string]Handler), queue: make(chan task.Task, workers*2), stop: make(chan struct{}), done: make(chan struct{}), running: make(map[string]context.CancelFunc), workersByID: make(map[string]Worker), leaseTTL: 30 * time.Second}
}

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
	defer s.mu.Unlock()
	w, ok := s.workersByID[id]
	if !ok {
		return Worker{}, errors.New("worker not registered")
	}
	now := time.Now()
	w.LastHeartbeat, w.LeaseUntil = now, now.Add(s.leaseTTL)
	s.workersByID[id] = w
	return w, nil
}

func (s *Scheduler) Workers() []Worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Worker, 0, len(s.workersByID))
	for _, w := range s.workersByID {
		result = append(result, w)
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
	}
	return s.store.Create(t)
}

func (s *Scheduler) Get(id string) (task.Task, error) { return s.store.Get(id) }

func (s *Scheduler) List() ([]task.Task, error) { return s.store.List() }

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
		cancel()
	}
	s.mu.Unlock()
	now := time.Now()
	t.Status, t.LastError, t.FinishedAt = task.StatusCanceled, task.ErrCanceled.Error(), &now
	return s.store.Update(t)
}

func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return
	}
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
	s.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-s.done
}

func (s *Scheduler) loop(ctx context.Context) {
	defer close(s.done)
	for i := 0; i < s.workers; i++ {
		go s.worker(ctx)
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	heartbeatTicker := time.NewTicker(s.leaseTTL / 3)
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
	now := time.Now()
	current.FinishedAt = &now
	current.LeaseUntil = nil
	current.WorkerID = ""
	current.LeaseToken = ""
	if err == nil {
		current.Status, current.LastError = task.StatusSuccess, ""
	} else if current.Attempts < current.Retry.MaxAttempts {
		current.Status = task.StatusRetrying
		current.LastError = err.Error()
		current.RunAt = now.Add(current.Retry.Backoff)
		current.FinishedAt = nil
	} else {
		current.Status, current.LastError = task.StatusFailed, err.Error()
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
