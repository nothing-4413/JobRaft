package scheduler

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

// staleListingStore finishes a task from under the scheduler the first time a
// listing is taken. That models what happens in practice: a claim walks a
// snapshot of the queue, and by the time it commits to a candidate another
// worker may already have claimed and completed it.
type staleListingStore struct {
	*store.MemoryStore

	once   sync.Once
	onList func()
}

func (s *staleListingStore) List() ([]task.Task, error) {
	items, err := s.MemoryStore.List()
	if err != nil {
		return nil, err
	}
	if s.onList != nil {
		s.once.Do(s.onList)
	}
	return items, nil
}

// TestClaimSkipsTaskFinishedDuringListing pins the stale-snapshot bug: a claim
// used to write the candidate it had listed straight back as "running", so a task
// that had already succeeded was handed to a second worker. The store recorded a
// single run for a task the client completed twice, and the duplicate completions
// consumed the budget of tasks that were never delivered at all.
func TestClaimSkipsTaskFinishedDuringListing(t *testing.T) {
	memory := store.NewMemory()
	now := time.Now().UTC()
	item := task.Task{ID: "stale", Name: "benchmark", Status: task.StatusPending, RunAt: now, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}}
	if err := memory.Create(item); err != nil {
		t.Fatal(err)
	}
	finish := func() {
		current, err := memory.Get("stale")
		if err != nil {
			t.Errorf("get task: %v", err)
			return
		}
		finished := time.Now().UTC()
		current.Status, current.RunCount, current.FinishedAt = task.StatusSuccess, current.RunCount+1, &finished
		if err := memory.Update(current); err != nil {
			t.Errorf("finish task: %v", err)
		}
	}
	s := New(&staleListingStore{MemoryStore: memory, onList: finish}, 1)
	if _, err := s.RegisterWorker("worker"); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim("worker")
	if !errors.Is(err, ErrNoTask) {
		t.Fatalf("Claim delivered %q with err=%v, want no task: it was already completed", claimed.ID, err)
	}
	current, err := memory.Get("stale")
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != task.StatusSuccess || current.RunCount != 1 {
		t.Fatalf("completed task was rewritten: status=%s run_count=%d", current.Status, current.RunCount)
	}
}
