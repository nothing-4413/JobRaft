package api

import (
	"net/http/httptest"
	"testing"

	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
)

func TestReadinessProbe(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/readyz", nil)
	New(s).Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("ready status=%d", w.Code)
	}
}
