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

func TestSchedulerTimeoutMarksFailure(t *testing.T) {
	s := New(store.NewMemory(), 1)
	_ = s.Register("slow", func(context.Context, task.Task) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	})
	if err := s.Submit(task.Task{ID: "timeout-1", Name: "slow", Timeout: 5 * time.Millisecond, Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	defer s.Stop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.Get("timeout-1")
		if got.Status == task.StatusFailed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out task was not marked failed")
}

func TestDueSortsByPriority(t *testing.T) {
	now := time.Now()
	items := store.Due([]task.Task{
		{ID: "low", Name: "x", Priority: 1, Status: task.StatusPending, RunAt: now},
		{ID: "high", Name: "x", Priority: 10, Status: task.StatusPending, RunAt: now},
	}, now)
	if len(items) != 2 || items[0].ID != "high" {
		t.Fatalf("priority order incorrect: %+v", items)
	}
}

func TestSchedulerHonorsDependencies(t *testing.T) {
	s := New(store.NewMemory(), 1)
	order := make([]string, 0, 2)
	_ = s.Register("step", func(_ context.Context, t task.Task) error { order = append(order, t.ID); return nil })
	if err := s.Submit(task.Task{ID: "first", Name: "step", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(task.Task{ID: "second", Name: "step", DependsOn: []string{"first"}, Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	defer s.Stop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && len(order) < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("dependency order incorrect: %v", order)
	}
}

func TestExternalWorkerClaimAndComplete(t *testing.T) {
	s := New(store.NewMemory(), 1)
	if err := s.Submit(task.Task{ID: "remote-1", Name: "remote", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	worker, err := s.RegisterWorker("remote-worker")
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Status != task.StatusRunning || claimed.WorkerID != worker.ID || claimed.LeaseToken == "" {
		t.Fatalf("invalid claim: %+v", claimed)
	}
	if err := s.CompleteTask(worker.ID, claimed.ID, claimed.LeaseToken, ""); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(claimed.ID)
	if got.Status != task.StatusSuccess {
		t.Fatalf("expected success, got %s", got.Status)
	}
}

func TestExternalWorkerStoresResult(t *testing.T) {
	s := New(store.NewMemory(), 1)
	if err := s.Submit(task.Task{ID: "result-1", Name: "remote", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	w, _ := s.RegisterWorker("worker-result")
	claimed, err := s.Claim(w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteTaskWithResult(w.ID, claimed.ID, claimed.LeaseToken, "", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(claimed.ID)
	if string(got.Result) != `{"ok":true}` {
		t.Fatalf("result=%s", got.Result)
	}
}

func TestExternalWorkerRenewsLease(t *testing.T) {
	s := New(store.NewMemory(), 1)
	if err := s.Submit(task.Task{ID: "renew-1", Name: "remote", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	w, _ := s.RegisterWorker("worker-renew")
	claimed, err := s.Claim(w.ID)
	if err != nil {
		t.Fatal(err)
	}
	old := claimed.LeaseUntil
	time.Sleep(time.Millisecond)
	renewed, err := s.RenewTaskLease(w.ID, claimed.ID, claimed.LeaseToken)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.LeaseUntil == nil || old == nil || !renewed.LeaseUntil.After(*old) {
		t.Fatalf("lease did not advance: old=%v new=%v", old, renewed.LeaseUntil)
	}
	if _, err := s.RenewTaskLease(w.ID, claimed.ID, "wrong"); err == nil {
		t.Fatal("expected invalid token error")
	}
}

func TestScheduledTaskRunsAgain(t *testing.T) {
	s := New(store.NewMemory(), 1)
	count := 0
	_ = s.Register("periodic", func(context.Context, task.Task) error { count++; return nil })
	if err := s.Submit(task.Task{ID: "periodic-1", Name: "periodic", Schedule: 10 * time.Millisecond, Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	defer s.Stop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && count < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if count < 2 {
		t.Fatalf("expected recurring task to run twice, got %d", count)
	}
}

func TestScheduledTaskResetsAttempts(t *testing.T) {
	s := New(store.NewMemory(), 1)
	count := 0
	_ = s.Register("periodic-reset", func(context.Context, task.Task) error { count++; return nil })
	if err := s.Submit(task.Task{ID: "periodic-reset-1", Name: "periodic-reset", Schedule: 10 * time.Millisecond, Retry: task.RetryPolicy{MaxAttempts: 2}}); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	defer s.Stop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && count < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.Get("periodic-reset-1")
	if count < 2 || got.Attempts > 1 {
		t.Fatalf("attempts were not reset: count=%d attempts=%d", count, got.Attempts)
	}
}

func TestSchedulerBackpressure(t *testing.T) {
	s := New(store.NewMemory(), 1)
	s.SetMaxPending(1)
	if err := s.Submit(task.Task{ID: "queued", Name: "noop", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(task.Task{ID: "rejected", Name: "noop", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != ErrBackpressure {
		t.Fatalf("expected backpressure, got %v", err)
	}
}

func TestDispatchReservesTaskBeforeWorkerExecution(t *testing.T) {
	s := New(store.NewMemory(), 1)
	_ = s.Register("noop", func(context.Context, task.Task) error { return nil })
	if err := s.Submit(task.Task{ID: "reservation-1", Name: "noop", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	s.dispatch()
	if _, ok := s.running["reservation-1"]; !ok {
		t.Fatal("expected task reservation")
	}
}

func TestStopRequeuesRunningTasks(t *testing.T) {
	s := New(store.NewMemory(), 1)
	started := make(chan struct{})
	_ = s.Register("blocking", func(ctx context.Context, t task.Task) error { close(started); <-ctx.Done(); return ctx.Err() })
	if err := s.Submit(task.Task{ID: "stop-1", Name: "blocking", Retry: task.RetryPolicy{MaxAttempts: 2}}); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("task did not start")
	}
	s.Stop()
	got, _ := s.Get("stop-1")
	if got.Status != task.StatusRetrying {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestSchedulerCanRestartAfterStop(t *testing.T) {
	s := New(store.NewMemory(), 1)
	runs := 0
	_ = s.Register("restart", func(context.Context, task.Task) error { runs++; return nil })
	if err := s.Submit(task.Task{ID: "restart-1", Name: "restart", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	deadline := time.Now().Add(time.Second)
	for runs == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	s.Stop()
	if runs == 0 {
		t.Fatal("task did not run before stop")
	}
	if err := s.Submit(task.Task{ID: "restart-2", Name: "restart", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	defer s.Stop()
	deadline = time.Now().Add(time.Second)
	for runs < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runs < 2 {
		t.Fatalf("scheduler did not restart, runs=%d", runs)
	}
}

func TestSubmitRejectsMissingDependency(t *testing.T) {
	s := New(store.NewMemory(), 1)
	err := s.Submit(task.Task{ID: "child", Name: "x", DependsOn: []string{"missing"}, Retry: task.RetryPolicy{MaxAttempts: 1}})
	if err == nil {
		t.Fatal("expected missing dependency error")
	}
}
