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

type Scheduler struct {
	store    store.Store
	workers  int
	interval time.Duration
	handlers map[string]Handler
	queue    chan task.Task
	stop     chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
	mu       sync.Mutex
	running  map[string]context.CancelFunc
}

func New(s store.Store, workers int) *Scheduler {
	if workers < 1 {
		workers = 1
	}
	return &Scheduler{store: s, workers: workers, interval: 100 * time.Millisecond, handlers: make(map[string]Handler), queue: make(chan task.Task, workers*2), stop: make(chan struct{}), done: make(chan struct{}), running: make(map[string]context.CancelFunc)}
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
	return s.store.Create(t)
}

func (s *Scheduler) Get(id string) (task.Task, error) { return s.store.Get(id) }

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
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.dispatch()
		}
	}
}

func (s *Scheduler) dispatch() {
	items, err := s.store.List()
	if err != nil {
		return
	}
	for _, t := range store.Due(items, time.Now()) {
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
	if err != nil || current.Status == task.StatusCanceled {
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
	if getErr != nil || current.Status == task.StatusCanceled {
		return
	}
	now := time.Now()
	current.FinishedAt = &now
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
