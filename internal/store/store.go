package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/nothing-4413/JobRaft/internal/task"
)

var ErrNotFound = errors.New("task not found")
var ErrConflict = errors.New("task changed since it was read")
var ErrDuplicateIdempotencyKey = errors.New("idempotency key already exists")
var ErrNoTaskAvailable = errors.New("no task available")

// Store persists task metadata. Implementations must be safe for concurrent use.
type Store interface {
	Create(task.Task) error
	Get(string) (task.Task, error)
	List() ([]task.Task, error)
	Update(task.Task) error
	// Delete removes a task record. It returns ErrNotFound when the task does
	// not exist, so callers can distinguish a purged record from a missing one.
	Delete(string) error
}

// ConditionalUpdater atomically updates a task only when its lease token still
// matches the expected value.
type ConditionalUpdater interface {
	UpdateIfLease(string, string, task.Task) error
	UpdateIfState(string, task.Status, string, task.Task) error
}

// AtomicClaimer lets a shared store reserve due work without the read/update
// race that exists when several scheduler processes use the same queue.
type AtomicClaimer interface {
	ClaimDue(workerID string, leaseTTL time.Duration) (task.Task, error)
}

// Worker is a worker liveness lease. Persistent implementations make workers
// visible to every API instance behind a load balancer.
type Worker struct {
	ID            string    `json:"id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	LeaseUntil    time.Time `json:"lease_until"`
}

type WorkerRegistry interface {
	RegisterWorker(string, time.Duration) (Worker, error)
	HeartbeatWorker(string, time.Duration) (Worker, error)
	GetWorker(string) (Worker, error)
	ListWorkers(time.Time) ([]Worker, error)
	RenewWorkerTaskLeases(string, time.Time) error
}

// MemoryStore is a simple in-process store useful for development and tests.
type MemoryStore struct {
	mu      sync.RWMutex
	tasks   map[string]task.Task
	workers map[string]Worker
}

func NewMemory() *MemoryStore {
	return &MemoryStore{tasks: make(map[string]task.Task), workers: make(map[string]Worker)}
}

func (s *MemoryStore) Create(t task.Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.IdempotencyKey != "" {
		for _, existing := range s.tasks {
			if existing.IdempotencyKey == t.IdempotencyKey {
				return ErrDuplicateIdempotencyKey
			}
		}
	}
	if _, ok := s.tasks[t.ID]; ok {
		return errors.New("task already exists")
	}
	s.tasks[t.ID] = clone(t)
	return nil
}

func (s *MemoryStore) GetByIdempotencyKey(key string) (task.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, t := range s.tasks {
		if t.IdempotencyKey == key {
			return clone(t), nil
		}
	}
	return task.Task{}, ErrNotFound
}

func (s *MemoryStore) Get(id string) (task.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return task.Task{}, ErrNotFound
	}
	return clone(t), nil
}

func (s *MemoryStore) List() ([]task.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]task.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		result = append(result, clone(t))
	}
	return result, nil
}

func (s *MemoryStore) Update(t task.Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[t.ID]; !ok {
		return ErrNotFound
	}
	s.tasks[t.ID] = clone(t)
	return nil
}

// Delete removes the task record entirely, unlike Cancel which keeps the task
// visible in its terminal canceled state.
func (s *MemoryStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[id]; !ok {
		return ErrNotFound
	}
	delete(s.tasks, id)
	return nil
}

func (s *MemoryStore) UpdateIfLease(id, token string, t task.Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.tasks[id]
	if !ok {
		return ErrNotFound
	}
	if current.Status != task.StatusRunning || current.LeaseToken != token {
		return ErrConflict
	}
	s.tasks[id] = clone(t)
	return nil
}

func (s *MemoryStore) UpdateIfState(id string, status task.Status, token string, t task.Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.tasks[id]
	if !ok {
		return ErrNotFound
	}
	if current.Status != status || current.LeaseToken != token {
		return ErrConflict
	}
	s.tasks[id] = clone(t)
	return nil
}

func (s *MemoryStore) RegisterWorker(id string, ttl time.Duration) (Worker, error) {
	if id == "" {
		return Worker{}, errors.New("worker id is required")
	}
	now := time.Now()
	w := Worker{ID: id, LastHeartbeat: now, LeaseUntil: now.Add(ttl)}
	s.mu.Lock()
	s.workers[id] = w
	s.mu.Unlock()
	return w, nil
}

func (s *MemoryStore) HeartbeatWorker(id string, ttl time.Duration) (Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workers[id]; !ok {
		return Worker{}, errors.New("worker not registered")
	}
	now := time.Now()
	w := Worker{ID: id, LastHeartbeat: now, LeaseUntil: now.Add(ttl)}
	s.workers[id] = w
	return w, nil
}

func (s *MemoryStore) GetWorker(id string) (Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.workers[id]
	if !ok {
		return Worker{}, ErrNotFound
	}
	return w, nil
}

func (s *MemoryStore) ListWorkers(now time.Time) ([]Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Worker, 0, len(s.workers))
	for _, w := range s.workers {
		if w.LeaseUntil.After(now) {
			result = append(result, w)
		}
	}
	return result, nil
}

func (s *MemoryStore) RenewWorkerTaskLeases(workerID string, until time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.tasks {
		if t.Status == task.StatusRunning && t.WorkerID == workerID {
			t.LeaseUntil = timePtr(until)
			s.tasks[id] = t
		}
	}
	return nil
}

func clone(t task.Task) task.Task {
	if t.Payload != nil {
		t.Payload = append([]byte(nil), t.Payload...)
	}
	if t.Result != nil {
		t.Result = append([]byte(nil), t.Result...)
	}
	if t.DependsOn != nil {
		t.DependsOn = append([]string(nil), t.DependsOn...)
	}
	return t
}

func timePtr(value time.Time) *time.Time { return &value }

// Due returns pending/retrying tasks whose scheduled time has arrived.
func Due(tasks []task.Task, now time.Time) []task.Task {
	result := make([]task.Task, 0)
	for _, t := range tasks {
		if (t.Status == task.StatusPending || t.Status == task.StatusRetrying) && !t.RunAt.After(now) {
			result = append(result, t)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Priority != result[j].Priority {
			return result[i].Priority > result[j].Priority
		}
		return result[i].RunAt.Before(result[j].RunAt)
	})
	return result
}
