package store

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/task"
)

func newPostgresTestStore(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("JOBRAFT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set JOBRAFT_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	s, err := NewPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.db.Exec(`DELETE FROM jobraft_tasks; DELETE FROM jobraft_workers`); _ = s.Close() })
	if _, err := s.db.Exec(`DELETE FROM jobraft_tasks; DELETE FROM jobraft_workers`); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPostgresCountInFlight(t *testing.T) {
	s := newPostgresTestStore(t)
	now := time.Now().UTC()
	want := 0
	for status, counts := range backpressureCounts() {
		item := task.Task{ID: "inflight-" + string(status), Name: "demo", Status: status, RunAt: now, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}}
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

// TestPostgresTargetedScans covers the two index-backed reads a scheduler tick
// uses instead of listing the table: the work that is due now, and the running
// tasks whose lease has lapsed.
func TestPostgresTargetedScans(t *testing.T) {
	s := newPostgresTestStore(t)
	now := time.Now().UTC()
	past, future := now.Add(-time.Minute), now.Add(time.Hour)
	lapsed, fresh := now.Add(-time.Minute), now.Add(time.Minute)
	fixtures := []task.Task{
		{ID: "due-pending", Name: "demo", Status: task.StatusPending, RunAt: past, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}},
		{ID: "due-retrying", Name: "demo", Priority: 5, Status: task.StatusRetrying, RunAt: past, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 2}},
		{ID: "future", Name: "demo", Status: task.StatusPending, RunAt: future, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}},
		{ID: "done", Name: "demo", Status: task.StatusSuccess, RunAt: past, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}},
		{ID: "running-fresh", Name: "demo", Status: task.StatusRunning, RunAt: past, CreatedAt: now, WorkerID: "worker-a", LeaseUntil: &fresh, LeaseToken: "tok-fresh", Retry: task.RetryPolicy{MaxAttempts: 1}},
		{ID: "running-lapsed", Name: "demo", Status: task.StatusRunning, RunAt: past, CreatedAt: now, WorkerID: "worker-a", LeaseUntil: &lapsed, LeaseToken: "tok-lapsed", Retry: task.RetryPolicy{MaxAttempts: 1}},
	}
	for _, item := range fixtures {
		if err := s.Create(item); err != nil {
			t.Fatalf("create %s: %v", item.ID, err)
		}
	}

	due, err := s.ListDue(now)
	if err != nil {
		t.Fatal(err)
	}
	if got := taskIDs(due); !slices.Equal(got, []string{"due-retrying", "due-pending"}) {
		t.Fatalf("ListDue = %v, want [due-retrying due-pending]", got)
	}

	expired, err := s.ListExpired(now)
	if err != nil {
		t.Fatal(err)
	}
	if got := taskIDs(expired); !slices.Equal(got, []string{"running-lapsed"}) {
		t.Fatalf("ListExpired = %v, want [running-lapsed]", got)
	}
}

func taskIDs(items []task.Task) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}

func TestPostgresClaimDueIsAtomic(t *testing.T) {
	s := newPostgresTestStore(t)
	now := time.Now().UTC()
	for _, id := range []string{"claim-1", "claim-2"} {
		if err := s.Create(task.Task{ID: id, Name: "demo", Status: task.StatusPending, RunAt: now, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"worker-a", "worker-b"} {
		if _, err := s.RegisterWorker(id, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	claimed, errs := make(chan task.Task, 2), make(chan error, 2)
	var wg sync.WaitGroup
	for _, workerID := range []string{"worker-a", "worker-b"} {
		workerID := workerID
		wg.Add(1)
		go func() {
			defer wg.Done()
			item, err := s.ClaimDue(workerID, time.Minute)
			if err != nil {
				errs <- err
				return
			}
			claimed <- item
		}()
	}
	wg.Wait()
	close(claimed)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for item := range claimed {
		if item.LeaseToken == "" || seen[item.ID] {
			t.Fatalf("duplicate or invalid claim: %+v", item)
		}
		seen[item.ID] = true
	}
	if len(seen) != 2 {
		t.Fatalf("expected two unique claims, got %v", seen)
	}
}

func TestPostgresClaimFailsDependentTask(t *testing.T) {
	s := newPostgresTestStore(t)
	now := time.Now().UTC()
	failed := task.Task{ID: "failed", Name: "demo", Status: task.StatusFailed, Attempts: 1, RunAt: now, CreatedAt: now, FinishedAt: &now, Retry: task.RetryPolicy{MaxAttempts: 1}}
	child := task.Task{ID: "child", Name: "demo", Status: task.StatusPending, DependsOn: []string{"failed"}, RunAt: now, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}}
	if err := s.Create(failed); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(child); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterWorker("worker", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimDue("worker", time.Minute); !errors.Is(err, ErrNoTaskAvailable) {
		t.Fatalf("claim error = %v", err)
	}
	got, err := s.Get("child")
	if err != nil || got.Status != task.StatusFailed {
		t.Fatalf("child = %+v, %v", got, err)
	}
}

func TestPostgresDeleteRemovesRecord(t *testing.T) {
	s := newPostgresTestStore(t)
	now := time.Now().UTC()
	keep := task.Task{ID: "delete-keep", Name: "demo", Status: task.StatusPending, RunAt: now, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}}
	drop := task.Task{ID: "delete-drop", Name: "demo", Status: task.StatusPending, RunAt: now, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}}
	if err := s.Create(keep); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(drop); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(drop.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(drop.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if err := s.Delete(drop.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound on repeated delete, got %v", err)
	}
	if _, err := s.Get(keep.ID); err != nil {
		t.Fatalf("unrelated task was lost: %v", err)
	}
}
