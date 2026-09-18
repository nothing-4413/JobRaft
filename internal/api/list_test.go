package api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

func TestTaskListFilters(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	_ = s.Submit(task.Task{ID: "a", Name: "email", Retry: task.RetryPolicy{MaxAttempts: 1}})
	_ = s.Submit(task.Task{ID: "b", Name: "cleanup", Retry: task.RetryPolicy{MaxAttempts: 1}})
	r := httptest.NewRequest("GET", "/tasks?name=email&limit=1", nil)
	w := httptest.NewRecorder()
	New(s).Handler().ServeHTTP(w, r)
	var items []task.Task
	if err := json.Unmarshal(w.Body.Bytes(), &items); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(items) != 1 || items[0].Name != "email" {
		t.Fatalf("response=%s", w.Body.String())
	}
}
