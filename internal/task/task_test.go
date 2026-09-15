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
