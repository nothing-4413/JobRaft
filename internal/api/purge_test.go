package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

func deleteTasks(t *testing.T, s *scheduler.Scheduler, target string, ids ...string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string][]string{"ids": ids})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("DELETE", target, strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	New(s).Handler().ServeHTTP(w, r)
	return w
}

// A plain DELETE cancels, which is the documented behavior and keeps the record
// visible for auditing. Regression guard for the benchmark cleanup that relied
// on this endpoint to actually empty the queue.
func TestDeleteTasksCancelsByDefault(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	if err := s.Submit(task.Task{ID: "cancel-me", Name: "benchmark", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	w := deleteTasks(t, s, "/tasks", "cancel-me")
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	got, err := s.Get("cancel-me")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != task.StatusCanceled {
		t.Fatalf("expected canceled status, got %s", got.Status)
	}
}

// purge=true removes the record so a rerun starts from an empty queue.
func TestDeleteTasksPurgeRemovesRecord(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	if err := s.Submit(task.Task{ID: "purge-me", Name: "benchmark", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	w := deleteTasks(t, s, "/tasks?purge=true", "purge-me")
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var result []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0]["purged"] != true {
		t.Fatalf("response=%s", w.Body.String())
	}
	if _, err := s.Get("purge-me"); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound after purge, got %v", err)
	}
}

// Purging a running task would strand the worker holding the lease, so the
// scheduler refuses and reports the failure per id instead of dropping it.
func TestDeleteTasksPurgeRefusesRunningTask(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	if err := s.Submit(task.Task{ID: "running", Name: "benchmark", Retry: task.RetryPolicy{MaxAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterWorker("worker"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim("worker"); err != nil {
		t.Fatal(err)
	}
	w := deleteTasks(t, s, "/tasks?purge=true", "running")
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var result []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0]["purged"] != false {
		t.Fatalf("response=%s", w.Body.String())
	}
	if _, err := s.Get("running"); err != nil {
		t.Fatalf("running task should survive a refused purge: %v", err)
	}
}
