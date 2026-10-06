package store

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/nothing-4413/JobRaft/internal/task"
)

// TestPostgresOperationCost measures what one task costs the store at different
// queue depths: the per-submit backpressure count, the claim transaction, the
// completion write, and a worker heartbeat plus its lease renewal.
//
// It exists because the reference benchmark in docs/benchmark-results.md is
// dominated by these per-task operations, and a cost that grows with the queue
// is invisible until the queue is deep. Run it against a real server:
//
//	JOBRAFT_TEST_DATABASE_URL=... JOBRAFT_BENCH_CLAIM=1 \
//	  go test ./internal/store -run OperationCost -v
//
// It is opt-in because it is a measurement, not a correctness gate.
func TestPostgresOperationCost(t *testing.T) {
	if os.Getenv("JOBRAFT_BENCH_CLAIM") == "" {
		t.Skip("set JOBRAFT_BENCH_CLAIM=1 to measure operation cost")
	}
	const (
		samples     = 200
		listSamples = 20
		lease       = time.Minute
	)

	for _, depth := range []int{0, 1000, 10000} {
		depth := depth
		t.Run(fmt.Sprintf("depth%d", depth), func(t *testing.T) {
			s := newPostgresTestStore(t)
			seedDueTasks(t, s, depth, "", 0)
			workerID := fmt.Sprintf("cost-worker-%d", depth)
			if _, err := s.RegisterWorker(workerID, lease); err != nil {
				t.Fatal(err)
			}

			measure(t, "list", listSamples, func() error {
				_, err := s.List()
				return err
			})
			measure(t, "count-in-flight", samples, func() error {
				_, err := s.CountInFlight()
				return err
			})
			measure(t, "create", samples, func() error {
				now := time.Now().UTC()
				return s.Create(task.Task{ID: fmt.Sprintf("cost-%d-%d", depth, time.Now().UnixNano()), Name: "cost", Status: task.StatusPending, RunAt: now, CreatedAt: now, Retry: task.RetryPolicy{MaxAttempts: 1}})
			})
			measure(t, "heartbeat+renew", samples, func() error {
				if _, err := s.HeartbeatWorker(workerID, lease); err != nil {
					return err
				}
				return s.RenewWorkerTaskLeases(workerID, time.Now().UTC().Add(lease))
			})
			measure(t, "claim", samples, func() error {
				_, err := s.ClaimDue(workerID, lease)
				if err == ErrNoTaskAvailable {
					return nil
				}
				return err
			})
			if depth > 0 {
				measure(t, "complete", samples, func() error {
					item, err := s.ClaimDue(workerID, lease)
					if err != nil {
						return err
					}
					item.Status = task.StatusSuccess
					finished := time.Now().UTC()
					item.FinishedAt = &finished
					return s.UpdateIfLease(item.ID, item.LeaseToken, item)
				})
			}
		})
	}

	t.Run("dependencies", func(t *testing.T) {
		s := newPostgresTestStore(t)
		const deps = 5
		seedDepTasks(t, s, deps)
		seedDueTasks(t, s, 1000, "dep", deps)
		workerID := "cost-worker-deps"
		if _, err := s.RegisterWorker(workerID, lease); err != nil {
			t.Fatal(err)
		}
		measure(t, "claim+5deps", samples, func() error {
			item, err := s.ClaimDue(workerID, lease)
			if err != nil {
				return err
			}
			item.Status = task.StatusSuccess
			return s.UpdateIfLease(item.ID, item.LeaseToken, item)
		})
	})
}

// seedDueTasks inserts count pending tasks that are already due. When deps is
// non-zero every task also depends on <depPrefix>-0 .. <depPrefix>-<deps-1>.
func seedDueTasks(t *testing.T, s *PostgresStore, count int, depPrefix string, deps int) {
	t.Helper()
	if count == 0 {
		return
	}
	runAt := time.Now().UTC().Add(-time.Minute)
	now := time.Now().UTC()
	if deps == 0 {
		_, err := s.db.Exec(`INSERT INTO jobraft_tasks (id, name, priority, depends_on, status, max_attempts, run_at, created_at, worker_id, lease_token)
			SELECT 'seed-' || g, 'cost', 0, '{}', 'pending', 1, $1, $2, '', '' FROM generate_series(1, $3) AS g`, runAt, now, count)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	dependsOn := make([]string, 0, deps)
	for index := 0; index < deps; index++ {
		dependsOn = append(dependsOn, fmt.Sprintf("%s-%d", depPrefix, index))
	}
	_, err := s.db.Exec(`INSERT INTO jobraft_tasks (id, name, priority, depends_on, status, max_attempts, run_at, created_at, worker_id, lease_token)
		SELECT 'dep-seed-' || g, 'cost', 0, $1, 'pending', 1, $2, $3, '', '' FROM generate_series(1, $4) AS g`, pq.Array(dependsOn), runAt, now, count)
	if err != nil {
		t.Fatal(err)
	}
}

// seedDepTasks inserts deps tasks that already succeeded, named dep-0..dep-n.
func seedDepTasks(t *testing.T, s *PostgresStore, deps int) {
	t.Helper()
	runAt := time.Now().UTC().Add(-time.Minute)
	now := time.Now().UTC()
	_, err := s.db.Exec(`INSERT INTO jobraft_tasks (id, name, priority, depends_on, status, max_attempts, run_at, created_at, worker_id, lease_token)
		SELECT 'dep-' || (g - 1), 'cost', 0, '{}', 'success', 1, $1, $2, '', '' FROM generate_series(1, $3) AS g`, runAt, now, deps)
	if err != nil {
		t.Fatal(err)
	}
}

// measure runs one operation samples times and reports its latency spread. The
// numbers are means because the interesting signal is a cost that grows with
// queue depth, not a tail.
func measure(t *testing.T, label string, samples int, fn func() error) {
	t.Helper()
	durations := make([]time.Duration, 0, samples)
	var total time.Duration
	for index := 0; index < samples; index++ {
		start := time.Now()
		if err := fn(); err != nil {
			t.Fatalf("%s sample %d: %v", label, index, err)
		}
		elapsed := time.Since(start)
		durations = append(durations, elapsed)
		total += elapsed
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	t.Logf("%-16s n=%-5d mean=%-10s p50=%-10s p95=%-10s",
		label, samples, (total / time.Duration(samples)).Round(time.Microsecond),
		durations[len(durations)/2].Round(time.Microsecond),
		durations[len(durations)*95/100].Round(time.Microsecond))
}
