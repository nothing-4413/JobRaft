package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

func TestBulkCancel(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	_ = s.Submit(task.Task{ID: "bulk-a", Name: "job", Retry: task.RetryPolicy{MaxAttempts: 1}})
	r := httptest.NewRequest("DELETE", "/tasks", strings.NewReader(`{"ids":["bulk-a","missing"]}`))
	w := httptest.NewRecorder()
	New(s).Handler().ServeHTTP(w, r)
	var result []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(result) != 2 || result[0]["canceled"] != true || result[1]["canceled"] != false {
		t.Fatalf("response=%s", w.Body.String())
	}
}
