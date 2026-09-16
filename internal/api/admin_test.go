package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
)

func TestAdminPage(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	r := httptest.NewRequest("GET", "/admin", nil)
	w := httptest.NewRecorder()
	New(s).Handler().ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "JobRaft") {
		t.Fatalf("unexpected admin response: %d", w.Code)
	}
}
