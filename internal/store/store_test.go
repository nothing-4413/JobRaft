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
