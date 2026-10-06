package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

// The single-node paths walk the whole queue: Submit counts in-flight tasks to
// enforce backpressure and Claim sorts every due candidate, both from List().
// The cost per operation therefore grows with the backlog. These benchmarks pin
// that shape down, so a fix shows up as a flat ns/op across the queue depths
// instead of a line through the origin.

var benchBacklogs = []int{100, 1000, 10000}

func benchTask(id string, now time.Time) task.Task {
	return task.Task{
		ID:     id,
		Name:   "bench",
		Status: task.StatusPending,
		RunAt:  now,
		Retry:  task.RetryPolicy{MaxAttempts: 1},
	}
}

// seedQueue fills a store with backlog immediately-due pending tasks.
func seedQueue(tb testing.TB, backlog int) *store.MemoryStore {
	tb.Helper()
	st := store.NewMemory()
	now := time.Now()
	for i := 0; i < backlog; i++ {
		if err := st.Create(benchTask(fmt.Sprintf("seed-%07d", i), now)); err != nil {
			tb.Fatalf("seed %d: %v", i, err)
		}
	}
	return st
}

// BenchmarkList is the floor for both paths above: they cannot get cheaper than
// materialising every record, so this shows how much of their cost is the
// listing itself.
func BenchmarkList(b *testing.B) {
	for _, backlog := range benchBacklogs {
		b.Run(fmt.Sprintf("backlog=%d", backlog), func(b *testing.B) {
			st := seedQueue(b, backlog)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := st.List(); err != nil {
					b.Fatalf("list: %v", err)
				}
			}
		})
	}
}

// BenchmarkSubmit measures accepting one task while the queue already holds
// backlog tasks, which is the state the submission phase of a benchmark run
// spends most of its time in.
func BenchmarkSubmit(b *testing.B) {
	for _, backlog := range benchBacklogs {
		b.Run(fmt.Sprintf("backlog=%d", backlog), func(b *testing.B) {
			st := seedQueue(b, backlog)
			sch := New(st, 1)
			// Keep the limit out of the measurement: what is timed is the
			// in-flight count, not the rejection.
			sch.SetMaxPending(backlog + b.N + 1000)
			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := sch.Submit(benchTask("", now)); err != nil {
					b.Fatalf("submit: %v", err)
				}
			}
		})
	}
}

// BenchmarkClaim measures reserving one task from a queue of backlog due
// candidates. The queue is refilled through the store directly so that the
// refill does not pay for Submit's scan and the depth stays constant.
func BenchmarkClaim(b *testing.B) {
	for _, backlog := range benchBacklogs {
		b.Run(fmt.Sprintf("backlog=%d", backlog), func(b *testing.B) {
			st := seedQueue(b, backlog)
			sch := New(st, 1)
			sch.SetLeaseTTL(time.Hour)
			if _, err := sch.RegisterWorker("bench-worker"); err != nil {
				b.Fatalf("register worker: %v", err)
			}
			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := sch.Claim("bench-worker"); err != nil {
					b.Fatalf("claim: %v", err)
				}
				if err := st.Create(benchTask(fmt.Sprintf("refill-%07d", i), now)); err != nil {
					b.Fatalf("refill: %v", err)
				}
			}
		})
	}
}
