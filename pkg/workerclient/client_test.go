package workerclient

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/api"
	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

func TestClientClaimComplete(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	_ = s.Register("job", func(context.Context, task.Task) error { return nil })
	_ = s.Submit(task.Task{ID: "client-1", Name: "job", Retry: task.RetryPolicy{MaxAttempts: 1}})
	ts := httptest.NewServer(api.New(s).Handler())
	defer ts.Close()
	c := &Client{BaseURL: ts.URL, WorkerID: "w1"}
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err := c.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(ctx, claimed, nil); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("client-1")
	if got.Status != task.StatusSuccess {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestClientEscapesWorkerAndTaskIDs(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	workerID := "worker/one"
	taskID := "task/one"
	if err := s.Submit(task.Task{ID: taskID, Name: "job", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(s).Handler())
	defer ts.Close()
	c := &Client{BaseURL: ts.URL, WorkerID: workerID}
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err := c.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != taskID {
		t.Fatalf("claimed wrong task: %+v", claimed)
	}
	if _, err := c.RenewLease(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(ctx, claimed, nil); err != nil {
		t.Fatal(err)
	}
}

func TestClientRenewsLease(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	_ = s.Register("job", func(context.Context, task.Task) error { return nil })
	_ = s.Submit(task.Task{ID: "renew-client", Name: "job", Retry: task.RetryPolicy{MaxAttempts: 1}})
	ts := httptest.NewServer(api.New(s).Handler())
	defer ts.Close()
	c := &Client{BaseURL: ts.URL, WorkerID: "w-renew"}
	ctx := context.Background()
	if err := c.Register(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err := c.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RenewLease(ctx, claimed); err != nil {
		t.Fatal(err)
	}
}

// renewalCountingStore reports the first lease renewal that reaches the store.
// It lets the test below wait for a renewal instead of racing a fixed sleep
// against the lease TTL, which made the test fail whenever the machine was
// slow enough that the client missed a renewal before the lease lapsed.
type renewalCountingStore struct {
	*store.MemoryStore
	renewed chan struct{}
	once    sync.Once
}

func newRenewalCountingStore() *renewalCountingStore {
	return &renewalCountingStore{MemoryStore: store.NewMemory(), renewed: make(chan struct{})}
}

func (s *renewalCountingStore) UpdateIfLease(id string, token string, t task.Task) error {
	err := s.MemoryStore.UpdateIfLease(id, token, t)
	if err == nil {
		s.once.Do(func() { close(s.renewed) })
	}
	return err
}

func TestClientRunRenewsLongTaskLease(t *testing.T) {
	st := newRenewalCountingStore()
	s := scheduler.New(st, 1)
	s.SetLeaseTTL(300 * time.Millisecond)
	if err := s.Submit(task.Task{ID: "long-client", Name: "job", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(api.New(s).Handler())
	defer ts.Close()
	c := &Client{BaseURL: ts.URL, WorkerID: "long-worker", PollInterval: 5 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	err := c.Run(ctx, func(handlerCtx context.Context, _ task.Task) error {
		// Keep working until the client has actually renewed the lease, so a
		// client that never renews fails this test instead of passing by luck.
		select {
		case <-st.renewed:
			return nil
		case <-handlerCtx.Done():
			return handlerCtx.Err()
		}
	})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected run to stop on context deadline, got %v", err)
	}
	got, err := s.Get("long-client")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != task.StatusSuccess {
		t.Fatalf("long task was not completed: %+v", got)
	}
}
