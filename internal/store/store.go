package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/nothing-4413/JobRaft/internal/task"
)

var ErrNotFound = errors.New("task not found")

// Store persists task metadata. Implementations must be safe for concurrent use.
type Store interface {
	Create(task.Task) error
	Get(string) (task.Task, error)
	List() ([]task.Task, error)
	Update(task.Task) error
}

// MemoryStore is a simple in-process store useful for development and tests.
type MemoryStore struct {
	mu    sync.RWMutex
	tasks map[string]task.Task
}

func NewMemory() *MemoryStore { return &MemoryStore{tasks: make(map[string]task.Task)} }

func (s *MemoryStore) Create(t task.Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[t.ID]; ok {
		return errors.New("task already exists")
	}
	s.tasks[t.ID] = clone(t)
	return nil
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
