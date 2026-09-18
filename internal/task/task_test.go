package task

import (
	"testing"
	"time"
)

func TestTaskValidationAndTerminalState(t *testing.T) {
	task := Task{ID: "t1", Name: "email", RunAt: time.Now(), Retry: RetryPolicy{MaxAttempts: 2}}
	if err := task.Validate(); err != nil {
		t.Fatalf("expected valid task: %v", err)
	}
	task.Status = StatusFailed
	if task.IsTerminal() {
		t.Fatalf("failed task should be retryable before max attempts")
	}
	task.Attempts = 2
	if !task.IsTerminal() {
		t.Fatalf("failed task should be terminal at max attempts")
	}
}

func TestTaskRejectsNegativeTimingAndCounters(t *testing.T) {
	base := Task{ID: "t1", Name: "job", RunAt: time.Now(), Retry: RetryPolicy{MaxAttempts: 1}}
	tests := []Task{
		func() Task { v := base; v.Retry.Backoff = -time.Second; return v }(),
		func() Task { v := base; v.Timeout = -time.Second; return v }(),
		func() Task { v := base; v.Schedule = -time.Second; return v }(),
		func() Task { v := base; v.Attempts = -1; return v }(),
		func() Task { v := base; v.RunCount = -1; return v }(),
	}
	for i, item := range tests {
		if err := item.Validate(); err == nil {
			t.Fatalf("case %d unexpectedly validated", i)
		}
	}
}
