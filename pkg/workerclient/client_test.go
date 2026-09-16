package workerclient

import (
	"context"
	"net/http/httptest"
	"testing"

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
