package store

import (
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/task"
)

func TestMemoryStoreCopiesPayload(t *testing.T) {
	s := NewMemory()
	original := []byte("hello")
	item := task.Task{ID: "1", Name: "demo", Payload: original, RunAt: time.Now(), Retry: task.RetryPolicy{MaxAttempts: 1}}
	if err := s.Create(item); err != nil {
		t.Fatal(err)
	}
	original[0] = 'x'
	got, err := s.Get("1")
	if err != nil || string(got.Payload) != "hello" {
		t.Fatalf("store did not copy payload: %+v, %v", got, err)
	}
}

func TestMemoryStoreConditionalLeaseUpdate(t *testing.T) {
	s := NewMemory()
	item := task.Task{ID: "lease", Name: "demo", RunAt: time.Now(), Retry: task.RetryPolicy{MaxAttempts: 1}, Status: task.StatusRunning, LeaseToken: "token"}
	if err := s.Create(item); err != nil {
		t.Fatal(err)
	}
	item.LastError = "renewed"
	if err := s.UpdateIfLease(item.ID, "wrong", item); err != ErrConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
	if err := s.UpdateIfLease(item.ID, item.LeaseToken, item); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(item.ID)
	if got.LastError != "renewed" {
		t.Fatalf("conditional update did not apply: %+v", got)
	}
	item.Status = task.StatusSuccess
	if err := s.UpdateIfLease(item.ID, item.LeaseToken, item); err != nil {
		t.Fatal(err)
	}
	item.LastError = "stale"
	if err := s.UpdateIfLease(item.ID, item.LeaseToken, item); err != ErrConflict {
		t.Fatalf("expected completed-task conflict, got %v", err)
	}
}

func TestMemoryStoreConditionalStateUpdate(t *testing.T) {
	s := NewMemory()
	item := task.Task{ID: "state", Name: "demo", RunAt: time.Now(), Retry: task.RetryPolicy{MaxAttempts: 1}, Status: task.StatusPending}
	if err := s.Create(item); err != nil {
		t.Fatal(err)
	}
	item.Status = task.StatusCanceled
	if err := s.UpdateIfState(item.ID, task.StatusRunning, "", item); err != ErrConflict {
		t.Fatalf("expected state conflict, got %v", err)
	}
	if err := s.UpdateIfState(item.ID, task.StatusPending, "", item); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(item.ID)
	if got.Status != task.StatusCanceled {
		t.Fatalf("conditional state update did not apply: %s", got.Status)
	}
}

func TestMemoryStoreDeleteRemovesRecord(t *testing.T) {
	s := NewMemory()
	item := task.Task{ID: "gone", Name: "demo", RunAt: time.Now(), Retry: task.RetryPolicy{MaxAttempts: 1}}
	if err := s.Create(item); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(item.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(item.ID); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if err := s.Delete(item.ID); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound on repeated delete, got %v", err)
	}
	items, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("expected empty store, got %d tasks", len(items))
	}
}

func TestFileStoreDeletePersists(t *testing.T) {
	path := t.TempDir() + "/tasks.json"
	s, err := NewFile(path)
	if err != nil {
		t.Fatal(err)
	}
	keep := task.Task{ID: "keep", Name: "demo", RunAt: time.Now(), Retry: task.RetryPolicy{MaxAttempts: 1}}
	drop := task.Task{ID: "drop", Name: "demo", RunAt: time.Now(), Retry: task.RetryPolicy{MaxAttempts: 1}}
	if err := s.Create(keep); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(drop); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(drop.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(drop.ID); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound on repeated delete, got %v", err)
	}
	reopened, err := NewFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Get(drop.ID); err != ErrNotFound {
		t.Fatalf("delete was not persisted, got %v", err)
	}
	if _, err := reopened.Get(keep.ID); err != nil {
		t.Fatalf("unrelated task was lost: %v", err)
	}
}

// backpressureCounts pins which statuses occupy a queue slot, since the
// submission limit is enforced from this count alone now.
func backpressureCounts() map[task.Status]bool {
	return map[task.Status]bool{
		task.StatusPending:  true,
		task.StatusRetrying: true,
		task.StatusRunning:  true,
		task.StatusSuccess:  false,
		task.StatusFailed:   false,
		task.StatusCanceled: false,
	}
}

func TestMemoryStoreCountInFlight(t *testing.T) {
	s := NewMemory()
	want := 0
	for status, counts := range backpressureCounts() {
		item := task.Task{ID: string(status), Name: "demo", Status: status, RunAt: time.Now(), Retry: task.RetryPolicy{MaxAttempts: 1}}
		if err := s.Create(item); err != nil {
			t.Fatal(err)
		}
		if counts {
			want++
		}
	}
	got, err := s.CountInFlight()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("CountInFlight = %d, want %d", got, want)
	}
}

func TestFileStoreCountInFlight(t *testing.T) {
	s, err := NewFile(t.TempDir() + "/tasks.json")
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	for status, counts := range backpressureCounts() {
		item := task.Task{ID: string(status), Name: "demo", Status: status, RunAt: time.Now(), Retry: task.RetryPolicy{MaxAttempts: 1}}
		if err := s.Create(item); err != nil {
			t.Fatal(err)
		}
		if counts {
			want++
		}
	}
	got, err := s.CountInFlight()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("CountInFlight = %d, want %d", got, want)
	}
}
