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
