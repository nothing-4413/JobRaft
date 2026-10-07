package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
)

// A client that pins an id which is already taken must be told the id is taken.
// It used to be reported as "task changed since it was read", which describes a
// lost race the caller never entered and reads like a bug in the caller.
func TestSubmitRejectsDuplicateID(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	handler := New(s).Handler()
	body := `{"id":"fixed-id","name":"benchmark","retry":{"max_attempts":1}}`
	cases := []struct {
		attempt int
		want    int
	}{{1, http.StatusCreated}, {2, http.StatusConflict}}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != c.want {
			t.Fatalf("attempt %d status=%d want=%d body=%s", c.attempt, w.Code, c.want, w.Body.String())
		}
		if c.want == http.StatusConflict && !strings.Contains(w.Body.String(), "task id already exists") {
			t.Fatalf("conflict body does not explain the duplicate id: %s", w.Body.String())
		}
	}
}
