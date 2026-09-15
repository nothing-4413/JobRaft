package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/task"
)

type Server struct{ scheduler *scheduler.Scheduler }

func New(s *scheduler.Scheduler) *Server { return &Server{scheduler: s} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/tasks", s.tasks)
	mux.HandleFunc("/tasks/", s.taskByID)
	mux.HandleFunc("/workers", s.workers)
	mux.HandleFunc("/workers/", s.workerHeartbeat)
	return mux
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	m := s.scheduler.Metrics()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "jobraft_tasks_submitted_total %d\njobraft_tasks_succeeded_total %d\njobraft_tasks_failed_total %d\njobraft_tasks_retried_total %d\njobraft_tasks_canceled_total %d\n", m.Submitted, m.Succeeded, m.Failed, m.Retried, m.Canceled)
}

func (s *Server) workers(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, s.scheduler.Workers())
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	wkr, err := s.scheduler.RegisterWorker(req.ID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, wkr)
}

func (s *Server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/claim") {
		s.workerClaim(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/workers/")
	wkr, err := s.scheduler.Heartbeat(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, wkr)
}

func (s *Server) workerClaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/workers/"), "/claim")
	t, err := s.scheduler.Claim(path)
	if err == scheduler.ErrNoTask {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) tasks(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		items, err := s.scheduler.List()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, items)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, "+http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID        string           `json:"id"`
		Name      string           `json:"name"`
		Priority  int              `json:"priority"`
		DependsOn []string         `json:"depends_on"`
		Payload   json.RawMessage  `json:"payload"`
		RunAt     *time.Time       `json:"run_at"`
		Delay     time.Duration    `json:"delay"`
		Timeout   time.Duration    `json:"timeout"`
		Schedule  time.Duration    `json:"schedule"`
		Retry     task.RetryPolicy `json:"retry"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	runAt := time.Now()
	if req.RunAt != nil {
		runAt = req.RunAt.UTC()
	} else if req.Delay > 0 {
		runAt = runAt.Add(req.Delay)
	}
	t := task.Task{ID: req.ID, Name: req.Name, Priority: req.Priority, DependsOn: req.DependsOn, Payload: append([]byte(nil), req.Payload...), RunAt: runAt, Timeout: req.Timeout, Schedule: req.Schedule, Retry: req.Retry}
	if t.ID == "" {
		t.ID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}
	if err := s.scheduler.Submit(t); err != nil {
		if err == scheduler.ErrBackpressure {
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	created, _ := s.scheduler.Get(t.ID)
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) taskByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/tasks/")
	if id == "" {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodDelete {
		if err := s.scheduler.Cancel(id); err != nil {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method == http.MethodPost && r.URL.Query().Get("complete") == "true" {
		var req struct {
			WorkerID   string `json:"worker_id"`
			LeaseToken string `json:"lease_token"`
			Error      string `json:"error"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := s.scheduler.CompleteTask(req.WorkerID, id, req.LeaseToken, req.Error); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	t, err := s.scheduler.Get(id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
