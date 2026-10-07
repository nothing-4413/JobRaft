package store

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/task"
)

// TestPostgresClaimThroughput measures how many tasks per second the store can
// claim, and checks that concurrent claims stay correct while doing it.
//
// It exists because the end-to-end benchmark in docs/benchmark-results.md is
// dominated by claim cost: every claim locks the worker row, locks up to 64
// candidate task rows, and locks the whole dependency set of each candidate it
// walks past. This test isolates that cost from the HTTP API and the scheduler's
// dispatch loop, so a regression shows up as a number instead of a hunch.
//
// It is opt-in because it is a measurement, not a correctness gate, and the
// sweep is far slower than the rest of the suite:
//
//	JOBRAFT_TEST_DATABASE_URL=... JOBRAFT_BENCH_CLAIM=1 \
//	  go test ./internal/store -run ClaimThroughput -v
func TestPostgresClaimThroughput(t *testing.T) {
	if os.Getenv("JOBRAFT_BENCH_CLAIM") == "" {
		t.Skip("set JOBRAFT_BENCH_CLAIM=1 to measure claim throughput")
	}
	const (
		tasks     = 1000
		lease     = time.Minute
		candidate = 64
	)
	for _, workers := range []int{1, 2, 4, 8, 16} {
		workers := workers
		t.Run(fmt.Sprintf("%dworkers", workers), func(t *testing.T) {
			s := newPostgresTestStore(t)
			now := time.Now().UTC()
			for index := 0; index < tasks; index++ {
				id := fmt.Sprintf("throughput-%d", index)
				item := task.Task{ID: id, Name: "bench", Status: task.StatusPending, RunAt: now, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}}
				if err := s.Create(item); err != nil {
					t.Fatal(err)
				}
			}

			type result struct {
				workerID string
				claimed  int
				err      error
			}
			results := make(chan result, workers)
			var heartbeat, complete int64
			start := time.Now()
			var wg sync.WaitGroup
			for index := 0; index < workers; index++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					workerID := fmt.Sprintf("throughput-worker-%d", index)
					if _, err := s.RegisterWorker(workerID, lease); err != nil {
						results <- result{workerID: workerID, err: err}
						return
					}
					claimed, empty := 0, 0
					for {
						// Mirror the benchmark worker loop: heartbeat, then claim,
						// then complete with the lease token.
						if _, err := s.HeartbeatWorker(workerID, lease); err != nil {
							results <- result{workerID: workerID, claimed: claimed, err: err}
							return
						}
						atomic.AddInt64(&heartbeat, 1)
						item, err := s.ClaimDue(workerID, lease)
						if err == ErrNoTaskAvailable {
							// An empty result can mean the queue is drained, or
							// just that another transaction still holds the rows
							// it walked. Retry before concluding the queue is empty.
							if empty++; empty >= 3 {
								results <- result{workerID: workerID, claimed: claimed}
								return
							}
							time.Sleep(time.Millisecond)
							continue
						}
						if err != nil {
							results <- result{workerID: workerID, claimed: claimed, err: err}
							return
						}
						claimed++
						empty = 0
						item.Status = task.StatusSuccess
						finished := time.Now().UTC()
						item.FinishedAt = &finished
						if err := s.UpdateIfLease(item.ID, item.LeaseToken, item); err != nil {
							results <- result{workerID: workerID, claimed: claimed, err: err}
							return
						}
						atomic.AddInt64(&complete, 1)
					}
				}(index)
			}
			wg.Wait()
			close(results)

			elapsed := time.Since(start)
			seen := make(map[string]string, tasks)
			total := 0
			for item := range results {
				if item.err != nil {
					t.Fatalf("worker %s: %v", item.workerID, item.err)
				}
				total += item.claimed
			}
			items, err := s.List()
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range items {
				if item.Status != task.StatusSuccess {
					t.Fatalf("task %s ended in status %s", item.ID, item.Status)
				}
				seen[item.ID] = item.WorkerID
			}
			if len(seen) != tasks {
				t.Fatalf("completed %d distinct tasks, want %d", len(seen), tasks)
			}
			t.Logf("%d workers: %d claims in %s = %.0f claims/s (%d heartbeats, %d candidate rows locked per claim)",
				workers, total, elapsed.Round(time.Millisecond),
				float64(total)/elapsed.Seconds(), atomic.LoadInt64(&heartbeat), candidate)
		})
	}
}

// TestPostgresClaimIsSingleUse guards the property the throughput test depends
// on: however many workers race, each task is delivered exactly once.
func TestPostgresClaimIsSingleUse(t *testing.T) {
	s := newPostgresTestStore(t)
	now := time.Now().UTC()
	const tasks = 50
	for index := 0; index < tasks; index++ {
		id := fmt.Sprintf("single-use-%d", index)
		item := task.Task{ID: id, Name: "bench", Status: task.StatusPending, RunAt: now, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}}
		if err := s.Create(item); err != nil {
			t.Fatal(err)
		}
	}
	const workers = 8
	var mu sync.Mutex
	claims := make(map[string]int, tasks)
	var wg sync.WaitGroup
	for index := 0; index < workers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			workerID := fmt.Sprintf("single-use-worker-%d", index)
			if _, err := s.RegisterWorker(workerID, time.Minute); err != nil {
				t.Errorf("register %s: %v", workerID, err)
				return
			}
			for {
				item, err := s.ClaimDue(workerID, time.Minute)
				if err == ErrNoTaskAvailable {
					return
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				mu.Lock()
				claims[item.ID]++
				mu.Unlock()
			}
		}(index)
	}
	wg.Wait()
	if len(claims) != tasks {
		t.Fatalf("claimed %d distinct tasks, want %d", len(claims), tasks)
	}
	for id, count := range claims {
		if count != 1 {
			t.Fatalf("task %s claimed %d times", id, count)
		}
	}
}
