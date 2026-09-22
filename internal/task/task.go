// Package task contains the domain model used by JobRaft.
package task

import (
	"encoding/json"
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
	ID string `json:"id"`
	// IdempotencyKey lets clients safely retry a submission without creating
	// another logical task.
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Name           string          `json:"name"`
	Priority       int             `json:"priority"`
	DependsOn      []string        `json:"depends_on,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	Status         Status          `json:"status"`
	Attempts       int             `json:"attempts"`
	Retry          RetryPolicy     `json:"retry"`
	RunAt          time.Time       `json:"run_at"`
	Schedule       time.Duration   `json:"schedule"`
	RunCount       int             `json:"run_count"`
	Timeout        time.Duration   `json:"timeout"`
	LastError      string          `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	WorkerID       string          `json:"worker_id,omitempty"`
	LeaseUntil     *time.Time      `json:"lease_until,omitempty"`
	LeaseToken     string          `json:"lease_token,omitempty"`
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
	if t.Retry.Backoff < 0 || t.Timeout < 0 || t.Schedule < 0 || t.Attempts < 0 || t.RunCount < 0 {
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
