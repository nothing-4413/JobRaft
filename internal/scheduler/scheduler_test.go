package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

func TestSchedulerRetriesAndSucceeds(t *testing.T) {
	s := New(store.NewMemory(), 1)
	attempts := 0
	if err := s.Register("demo", func(context.Context, task.Task) error {
		attempts++
		if attempts == 1 {
			return errors.New("temporary")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(task.Task{ID: "r1", Name: "demo", Retry: task.RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	defer s.Stop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.Get("r1")
		if got.Status == task.StatusSuccess {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task did not succeed, attempts=%d", attempts)
}
