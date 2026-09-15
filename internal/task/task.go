// Package task contains the domain model used by JobRaft.
package task

import (
	"errors"
	"time"
)

// Status describes the lifecycle state of a task.
type Status string

const (
	StatusPending  Status = "pending"
	StatusRunning  Status = "running"
	StatusSuccess  Status = "success"
	StatusFailed   Status = "failed"
	StatusRetrying Status = "retrying"
	StatusCanceled Status = "canceled"
)

// RetryPolicy controls how failed executions are retried.
type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"`
	Backoff     time.Duration `json:"backoff"`
}

// Task is a unit of work submitted to JobRaft.
type Task struct {
	ID         string        `json:"id"`
	Name       string        `json:"name"`
	Payload    []byte        `json:"payload,omitempty"`
	Status     Status        `json:"status"`
	Attempts   int           `json:"attempts"`
	Retry      RetryPolicy   `json:"retry"`
	RunAt      time.Time     `json:"run_at"`
	Timeout    time.Duration `json:"timeout"`
	LastError  string        `json:"last_error,omitempty"`
	CreatedAt  time.Time     `json:"created_at"`
	StartedAt  *time.Time    `json:"started_at,omitempty"`
	FinishedAt *time.Time    `json:"finished_at,omitempty"`
	WorkerID   string        `json:"worker_id,omitempty"`
	LeaseUntil *time.Time    `json:"lease_until,omitempty"`
	LeaseToken string        `json:"-"`
}

var (
	ErrInvalidTask = errors.New("invalid task")
	ErrCanceled    = errors.New("task canceled")
)

// Validate checks the fields required by the scheduler.
func (t Task) Validate() error {
	if t.ID == "" || t.Name == "" {
		return ErrInvalidTask
	}
	if t.Retry.MaxAttempts < 1 {
		return ErrInvalidTask
	}
	if t.RunAt.IsZero() {
		return ErrInvalidTask
	}
	return nil
}

// IsTerminal reports whether no further state transitions are expected.
func (t Task) IsTerminal() bool {
	return t.Status == StatusSuccess || t.Status == StatusCanceled || (t.Status == StatusFailed && t.Attempts >= t.Retry.MaxAttempts)
}
