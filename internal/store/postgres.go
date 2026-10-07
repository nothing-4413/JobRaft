package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/nothing-4413/JobRaft/internal/task"
)

// PostgresStore is the shared production store. Schema creation is
// idempotent so a fresh deployment has no separate bootstrap step.
type PostgresStore struct{ db *sql.DB }

func NewPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	if dsn == "" {
		return nil, errors.New("database URL is required")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	s := &PostgresStore{db: db}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *PostgresStore) Close() error { return s.db.Close() }

func (s *PostgresStore) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS jobraft_tasks (
  id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE, name TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0, depends_on TEXT[] NOT NULL DEFAULT '{}',
  payload JSONB, result JSONB, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL, retry_backoff_ns BIGINT NOT NULL DEFAULT 0,
  run_at TIMESTAMPTZ NOT NULL, schedule_ns BIGINT NOT NULL DEFAULT 0,
  run_count INTEGER NOT NULL DEFAULT 0, timeout_ns BIGINT NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, worker_id TEXT NOT NULL DEFAULT '',
  lease_until TIMESTAMPTZ, lease_token TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS jobraft_tasks_due_idx ON jobraft_tasks (status, run_at, priority DESC);
-- A due task is picked by priority and then by arrival, so a claim has to read
-- the queue in exactly this order and stop as soon as it has its batch. Without
-- a matching index the planner sorts every due row on every claim: on a 10,000
-- row backlog it quicksorted the whole set (15.6 ms) to return 64 candidates,
-- and with this index it walks the ordered rows instead (0.1 ms). The status
-- predicate is part of the index, so the scan covers only claimable tasks.
CREATE INDEX IF NOT EXISTS jobraft_tasks_claim_idx ON jobraft_tasks (priority DESC, run_at, id) WHERE status IN ('pending', 'retrying');
CREATE INDEX IF NOT EXISTS jobraft_tasks_running_lease_idx ON jobraft_tasks (lease_until) WHERE status = 'running';
CREATE TABLE IF NOT EXISTS jobraft_workers (
  id TEXT PRIMARY KEY, last_heartbeat TIMESTAMPTZ NOT NULL, lease_until TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS jobraft_workers_lease_idx ON jobraft_workers (lease_until);`)
	return err
}

const taskColumns = `id, idempotency_key, name, priority, depends_on, payload, result, status, attempts, max_attempts, retry_backoff_ns, run_at, schedule_ns, run_count, timeout_ns, last_error, created_at, started_at, finished_at, worker_id, lease_until, lease_token`
const taskSelectColumns = `id, COALESCE(idempotency_key, ''), name, priority, depends_on, payload, result, status, attempts, max_attempts, retry_backoff_ns, run_at, schedule_ns, run_count, timeout_ns, last_error, created_at, started_at, finished_at, worker_id, lease_until, lease_token`

func (s *PostgresStore) Create(t task.Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	_, err := s.db.Exec(`INSERT INTO jobraft_tasks (`+taskColumns+`) VALUES ($1, NULLIF($2, ''), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`, taskArgs(t)...)
	return mapPostgresError(err)
}

func (s *PostgresStore) Get(id string) (task.Task, error) {
	return scanOne(s.db.QueryRow(`SELECT `+taskSelectColumns+` FROM jobraft_tasks WHERE id = $1`, id))
}

func (s *PostgresStore) GetByIdempotencyKey(key string) (task.Task, error) {
	return scanOne(s.db.QueryRow(`SELECT `+taskSelectColumns+` FROM jobraft_tasks WHERE idempotency_key = $1`, key))
}

func (s *PostgresStore) List() ([]task.Task, error) {
	return s.queryTasks(`ORDER BY created_at, id`)
}

// ListDue returns the pending and retrying tasks whose scheduled time has
// arrived, in the order a scheduler dispatches them. The due index answers this
// directly, so a queue full of finished or future work costs no more than an
// empty one.
func (s *PostgresStore) ListDue(now time.Time) ([]task.Task, error) {
	return s.queryTasks(`WHERE status IN ('pending', 'retrying') AND run_at <= $1 ORDER BY priority DESC, run_at, id`, now)
}

// ListExpired returns running tasks whose lease has lapsed: the work a
// scheduler has to make retryable again. A partial index keeps this
// proportional to the running tasks rather than to the whole table.
func (s *PostgresStore) ListExpired(now time.Time) ([]task.Task, error) {
	return s.queryTasks(`WHERE status = 'running' AND lease_until IS NOT NULL AND lease_until <= $1 ORDER BY lease_until, id`, now)
}

func (s *PostgresStore) queryTasks(where string, args ...any) ([]task.Task, error) {
	rows, err := s.db.Query(`SELECT `+taskSelectColumns+` FROM jobraft_tasks `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]task.Task, 0)
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, t)
	}
	return items, rows.Err()
}

// CountInFlight asks the database for the queue depth instead of transferring
// and decoding every row, which is what a List() based count would cost.
func (s *PostgresStore) CountInFlight() (int, error) {
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobraft_tasks WHERE status IN ('pending', 'retrying', 'running')`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PostgresStore) Update(t task.Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	result, err := s.db.Exec(updateTaskSQL, taskArgs(t)...)
	if err != nil {
		return mapPostgresError(err)
	}
	return requireAffected(result)
}

// Delete removes the task record entirely, unlike Cancel which keeps the task
// visible in its terminal canceled state.
func (s *PostgresStore) Delete(id string) error {
	result, err := s.db.Exec(`DELETE FROM jobraft_tasks WHERE id = $1`, id)
	if err != nil {
		return mapPostgresError(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

const updateTaskSQL = `UPDATE jobraft_tasks SET idempotency_key = NULLIF($2, ''), name = $3, priority = $4, depends_on = $5, payload = $6, result = $7, status = $8, attempts = $9, max_attempts = $10, retry_backoff_ns = $11, run_at = $12, schedule_ns = $13, run_count = $14, timeout_ns = $15, last_error = $16, created_at = $17, started_at = $18, finished_at = $19, worker_id = $20, lease_until = $21, lease_token = $22 WHERE id = $1`

func (s *PostgresStore) UpdateIfLease(id, token string, t task.Task) error {
	return s.updateIf(id, task.StatusRunning, token, t)
}

func (s *PostgresStore) UpdateIfState(id string, status task.Status, token string, t task.Task) error {
	return s.updateIf(id, status, token, t)
}

func (s *PostgresStore) updateIf(id string, status task.Status, token string, t task.Task) error {
	if err := t.Validate(); err != nil {
		return err
	}
	args := append(taskArgs(t), status, token)
	result, err := s.db.Exec(updateTaskSQL+` AND status = $23 AND lease_token = $24`, args...)
	if err != nil {
		return mapPostgresError(err)
	}
	return requireAffected(result)
}

// ClaimDue reserves exactly one eligible task inside one SQL transaction.
// SKIP LOCKED lets competing API processes progress without duplicate claims.
func (s *PostgresStore) ClaimDue(workerID string, ttl time.Duration) (task.Task, error) {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return task.Task{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var alive bool
	if err := tx.QueryRow(`SELECT lease_until > NOW() FROM jobraft_workers WHERE id = $1 FOR UPDATE`, workerID).Scan(&alive); err != nil || !alive {
		if err == sql.ErrNoRows {
			return task.Task{}, ErrConflict
		}
		return task.Task{}, err
	}
	rows, err := tx.Query(`SELECT ` + taskSelectColumns + ` FROM jobraft_tasks WHERE status IN ('pending', 'retrying') AND run_at <= NOW() ORDER BY priority DESC, run_at, id FOR UPDATE SKIP LOCKED LIMIT 64`)
	if err != nil {
		return task.Task{}, err
	}
	candidates := make([]task.Task, 0, 64)
	for rows.Next() {
		candidate, err := scanTask(rows)
		if err != nil {
			return task.Task{}, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Close(); err != nil {
		return task.Task{}, err
	}
	if err := rows.Err(); err != nil {
		return task.Task{}, err
	}
	for _, candidate := range candidates {
		ready, dependencyError, err := dependenciesInTx(tx, candidate)
		if err != nil {
			return task.Task{}, err
		}
		if dependencyError != "" {
			if _, err := tx.Exec(`UPDATE jobraft_tasks SET status = $2, last_error = $3, finished_at = NOW() WHERE id = $1`, candidate.ID, task.StatusFailed, dependencyError); err != nil {
				return task.Task{}, err
			}
			continue
		}
		if !ready {
			continue
		}
		now := time.Now().UTC()
		candidate.Status, candidate.Attempts, candidate.StartedAt = task.StatusRunning, candidate.Attempts+1, &now
		candidate.WorkerID = workerID
		leaseUntil := now.Add(ttl)
		candidate.LeaseUntil = &leaseUntil
		candidate.LeaseToken, err = newLeaseToken()
		if err != nil {
			return task.Task{}, err
		}
		if _, err := tx.Exec(`UPDATE jobraft_tasks SET status = $2, attempts = $3, started_at = $4, worker_id = $5, lease_until = $6, lease_token = $7 WHERE id = $1`, candidate.ID, candidate.Status, candidate.Attempts, candidate.StartedAt, candidate.WorkerID, candidate.LeaseUntil, candidate.LeaseToken); err != nil {
			return task.Task{}, err
		}
		if err := tx.Commit(); err != nil {
			return task.Task{}, err
		}
		return candidate, nil
	}
	if err := rows.Err(); err != nil {
		return task.Task{}, err
	}
	if err := tx.Commit(); err != nil {
		return task.Task{}, err
	}
	return task.Task{}, ErrNoTaskAvailable
}

func dependenciesInTx(tx *sql.Tx, t task.Task) (bool, string, error) {
	if len(t.DependsOn) == 0 {
		return true, "", nil
	}
	// One query for the whole dependency set: a claim is on the hot path, and a
	// round trip per dependency made a task with five of them cost several
	// times what the claim itself costs.
	rows, err := tx.Query(`SELECT id, status FROM jobraft_tasks WHERE id = ANY($1) FOR KEY SHARE`, pq.Array(t.DependsOn))
	if err != nil {
		return false, "", err
	}
	statuses := make(map[string]task.Status, len(t.DependsOn))
	for rows.Next() {
		var id string
		var status task.Status
		if err := rows.Scan(&id, &status); err != nil {
			rows.Close()
			return false, "", err
		}
		statuses[id] = status
	}
	if err := rows.Close(); err != nil {
		return false, "", err
	}
	if err := rows.Err(); err != nil {
		return false, "", err
	}
	// Report the first problem in the order the task lists its dependencies, so
	// the error a caller sees does not depend on how the rows came back.
	for _, id := range t.DependsOn {
		status, ok := statuses[id]
		if !ok {
			return false, fmt.Sprintf("dependency %s not found", id), nil
		}
		if status == task.StatusFailed || status == task.StatusCanceled {
			return false, fmt.Sprintf("dependency %s did not succeed", id), nil
		}
		if status != task.StatusSuccess {
			return false, "", nil
		}
	}
	return true, "", nil
}

func (s *PostgresStore) RegisterWorker(id string, ttl time.Duration) (Worker, error) {
	if id == "" {
		return Worker{}, errors.New("worker id is required")
	}
	now := time.Now().UTC()
	w := Worker{ID: id, LastHeartbeat: now, LeaseUntil: now.Add(ttl)}
	_, err := s.db.Exec(`INSERT INTO jobraft_workers (id, last_heartbeat, lease_until) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET last_heartbeat = EXCLUDED.last_heartbeat, lease_until = EXCLUDED.lease_until`, w.ID, w.LastHeartbeat, w.LeaseUntil)
	return w, err
}

func (s *PostgresStore) HeartbeatWorker(id string, ttl time.Duration) (Worker, error) {
	now := time.Now().UTC()
	w := Worker{ID: id, LastHeartbeat: now, LeaseUntil: now.Add(ttl)}
	result, err := s.db.Exec(`UPDATE jobraft_workers SET last_heartbeat = $2, lease_until = $3 WHERE id = $1`, w.ID, w.LastHeartbeat, w.LeaseUntil)
	if err != nil {
		return Worker{}, err
	}
	if err := requireAffected(result); err != nil {
		return Worker{}, err
	}
	return w, nil
}

func (s *PostgresStore) GetWorker(id string) (Worker, error) {
	var w Worker
	err := s.db.QueryRow(`SELECT id, last_heartbeat, lease_until FROM jobraft_workers WHERE id = $1`, id).Scan(&w.ID, &w.LastHeartbeat, &w.LeaseUntil)
	if err == sql.ErrNoRows {
		return Worker{}, ErrNotFound
	}
	return w, err
}

func (s *PostgresStore) ListWorkers(now time.Time) ([]Worker, error) {
	rows, err := s.db.Query(`SELECT id, last_heartbeat, lease_until FROM jobraft_workers WHERE lease_until > $1 ORDER BY id`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	workers := make([]Worker, 0)
	for rows.Next() {
		var w Worker
		if err := rows.Scan(&w.ID, &w.LastHeartbeat, &w.LeaseUntil); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

func (s *PostgresStore) RenewWorkerTaskLeases(workerID string, until time.Time) error {
	_, err := s.db.Exec(`UPDATE jobraft_tasks SET lease_until = $2 WHERE worker_id = $1 AND status = 'running'`, workerID, until)
	return err
}

func taskArgs(t task.Task) []interface{} {
	dependsOn := t.DependsOn
	if dependsOn == nil {
		dependsOn = []string{}
	}
	return []interface{}{t.ID, t.IdempotencyKey, t.Name, t.Priority, pq.Array(dependsOn), nullableJSON(t.Payload), nullableJSON(t.Result), t.Status, t.Attempts, t.Retry.MaxAttempts, int64(t.Retry.Backoff), t.RunAt, int64(t.Schedule), t.RunCount, int64(t.Timeout), t.LastError, t.CreatedAt, t.StartedAt, t.FinishedAt, t.WorkerID, t.LeaseUntil, t.LeaseToken}
}

func nullableJSON(value []byte) interface{} {
	if len(value) == 0 {
		return nil
	}
	return value
}

type taskScanner interface{ Scan(...interface{}) error }

func scanOne(row *sql.Row) (task.Task, error) { return scanTask(row) }

func scanTask(scanner taskScanner) (task.Task, error) {
	var t task.Task
	var payload, result []byte
	var startedAt, finishedAt, leaseUntil sql.NullTime
	err := scanner.Scan(&t.ID, &t.IdempotencyKey, &t.Name, &t.Priority, pq.Array(&t.DependsOn), &payload, &result, &t.Status, &t.Attempts, &t.Retry.MaxAttempts, &t.Retry.Backoff, &t.RunAt, &t.Schedule, &t.RunCount, &t.Timeout, &t.LastError, &t.CreatedAt, &startedAt, &finishedAt, &t.WorkerID, &leaseUntil, &t.LeaseToken)
	if err == sql.ErrNoRows {
		return task.Task{}, ErrNotFound
	}
	if err != nil {
		return task.Task{}, err
	}
	t.Payload, t.Result = payload, result
	if startedAt.Valid {
		t.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		t.FinishedAt = &finishedAt.Time
	}
	if leaseUntil.Valid {
		t.LeaseUntil = &leaseUntil.Time
	}
	return t, nil
}

func newLeaseToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func requireAffected(result sql.Result) error {
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrConflict
	}
	return nil
}

func mapPostgresError(err error) error {
	if err == nil {
		return nil
	}
	if pg, ok := err.(*pq.Error); ok && pg.Code == "23505" {
		if pg.Constraint == "jobraft_tasks_idempotency_key_key" {
			return ErrDuplicateIdempotencyKey
		}
		return ErrConflict
	}
	return err
}
