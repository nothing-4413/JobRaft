package store

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"

	"github.com/nothing-4413/JobRaft/internal/fileutil"
	"github.com/nothing-4413/JobRaft/internal/task"
)

// FileStore persists the complete task set as a JSON document. It is intended
// for a single scheduler process and provides restart recovery without external
// services.
type FileStore struct {
	mu    sync.RWMutex
	path  string
	tasks map[string]task.Task
}

func NewFile(path string) (*FileStore, error) {
	if path == "" {
		return nil, os.ErrInvalid
	}
	s := &FileStore{path: path, tasks: make(map[string]task.Task)}
	b, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &s.tasks); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *FileStore) Create(t task.Task) error {
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
		return os.ErrExist
	}
	s.tasks[t.ID] = clone(t)
	return s.saveLocked()
}

func (s *FileStore) GetByIdempotencyKey(key string) (task.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, t := range s.tasks {
		if t.IdempotencyKey == key {
			return clone(t), nil
		}
	}
	return task.Task{}, ErrNotFound
}

func (s *FileStore) Get(id string) (task.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return task.Task{}, ErrNotFound
	}
	return clone(t), nil
}

func (s *FileStore) List() ([]task.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]task.Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		result = append(result, clone(t))
	}
	return result, nil
}

func (s *FileStore) Update(t task.Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[t.ID]; !ok {
		return ErrNotFound
	}
	s.tasks[t.ID] = clone(t)
	return s.saveLocked()
}

func (s *FileStore) UpdateIfLease(id, token string, t task.Task) error {
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
	return s.saveLocked()
}

func (s *FileStore) UpdateIfState(id string, status task.Status, token string, t task.Task) error {
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
	return s.saveLocked()
}

func (s *FileStore) saveLocked() error {
	b, err := json.MarshalIndent(s.tasks, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := ioutil.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	return fileutil.Replace(tmp, s.path)
}
