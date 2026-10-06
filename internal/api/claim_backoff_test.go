package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

// countingStore counts how often the claim path lists the queue. A claim
// attempt on a store that is not an AtomicClaimer is exactly one List.
type countingStore struct {
	*store.MemoryStore
	lists int64
}

func (s *countingStore) List() ([]task.Task, error) {
	atomic.AddInt64(&s.lists, 1)
	return s.MemoryStore.List()
}

func (s *countingStore) listCalls() int { return int(atomic.LoadInt64(&s.lists)) }

// TestWorkerClaimBacksOffWhileIdle pins the retry cadence of an idle long poll.
// A fixed interval fits ten claim attempts into a one second wait, which means
// every idle worker keeps opening transactions that only take locks. Backing off
// from 50 ms fits about six.
func TestWorkerClaimBacksOffWhileIdle(t *testing.T) {
	st := &countingStore{MemoryStore: store.NewMemory()}
	if _, err := st.RegisterWorker("w1", time.Minute); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	ts := httptest.NewServer(New(scheduler.New(st, 1)).Handler())
	defer ts.Close()

	start := time.Now()
	resp, err := http.Post(ts.URL+"/workers/w1/claim?wait=1s", "application/json", nil)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	defer resp.Body.Close()
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("idle claim returned %d (%s), want 204", resp.StatusCode, body)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("idle claim returned after %s, want it to wait out the requested 1s", elapsed)
	}

	attempts := st.listCalls()
	if attempts > 8 {
		t.Fatalf("idle long poll made %d claim attempts in %s; the retry interval is not backing off", attempts, elapsed)
	}
	if attempts < 2 {
		t.Fatalf("idle long poll made %d claim attempt(s) in %s; it stopped polling too early", attempts, elapsed)
	}
}
