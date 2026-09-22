package api

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nothing-4413/JobRaft/internal/cluster"
	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/task"
)

type Server struct {
	scheduler *scheduler.Scheduler
	registry  cluster.Registry
	token     string
}

func New(s *scheduler.Scheduler) *Server { return &Server{scheduler: s} }
func NewWithCluster(s *scheduler.Scheduler, r cluster.Registry) *Server {
	return &Server{scheduler: s, registry: r}
}

// NewWithToken enables bearer/API-key authentication for management and worker
// endpoints. An empty token keeps the local development mode unauthenticated.
func NewWithToken(s *scheduler.Scheduler, token string) *Server {
	return &Server{scheduler: s, token: token}
}

func NewWithClusterToken(s *scheduler.Scheduler, r cluster.Registry, token string) *Server {
	return &Server{scheduler: s, registry: r, token: token}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", s.ready)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/cluster", s.cluster)
	mux.HandleFunc("/admin", s.admin)
	mux.HandleFunc("/tasks", s.tasks)
	mux.HandleFunc("/tasks/", s.taskByID)
	mux.HandleFunc("/workers", s.workers)
	mux.HandleFunc("/workers/", s.workerHeartbeat)
	if s.token == "" {
		return mux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health probes remain public so a load balancer can determine whether
		// the process is alive without holding application credentials.
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			mux.ServeHTTP(w, r)
			return
		}
		provided := r.Header.Get("X-API-Key")
		if provided == "" {
			provided = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		}
		if subtle.ConstantTimeCompare([]byte(provided), []byte(s.token)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if _, err := s.scheduler.List(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) admin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<!doctype html><html><head><meta charset="utf-8"><title>JobRaft Admin</title><style>body{font:14px system-ui;margin:2rem;background:#f6f7f9;color:#17202a}h1{margin-bottom:.25rem}section{background:white;padding:1rem;margin:1rem 0;border-radius:8px;box-shadow:0 1px 4px #ccd}pre{white-space:pre-wrap;overflow:auto}</style></head><body><h1>JobRaft</h1><p>Live scheduler overview (auto-refreshes every 2 seconds)</p><section><h2>Cluster</h2><pre id="cluster">loading...</pre></section><section><h2>Workers</h2><pre id="workers">loading...</pre></section><section><h2>Tasks</h2><pre id="tasks">loading...</pre></section><section><h2>Metrics</h2><pre id="metrics">loading...</pre></section><script>async function load(){for(const [id,url] of [['cluster','/cluster'],['workers','/workers'],['tasks','/tasks']]){try{document.getElementById(id).textContent=JSON.stringify(await (await fetch(url)).json(),null,2)}catch(e){document.getElementById(id).textContent=e}}try{document.getElementById('metrics').textContent=await (await fetch('/metrics')).text()}catch(e){document.getElementById('metrics').textContent=e}}load();setInterval(load,2000)</script></body></html>`))
}

func (s *Server) cluster(w http.ResponseWriter, r *http.Request) {
	if s.registry == nil {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "cluster election is not configured"})
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"leader": func() interface{} {
		n, ok := s.registry.Leader()
		if !ok {
			return nil
		}
		return n
	}(), "nodes": s.registry.List()})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	m := s.scheduler.Metrics()
	items, _ := s.scheduler.List()
	pending, running, retrying := 0, 0, 0
	for _, item := range items {
		switch item.Status {
		case task.StatusPending:
			pending++
		case task.StatusRunning:
			running++
		case task.StatusRetrying:
			retrying++
		}
	}
	workers := len(s.scheduler.Workers())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# TYPE jobraft_tasks_submitted_total counter\njobraft_tasks_submitted_total %d\n# TYPE jobraft_tasks_succeeded_total counter\njobraft_tasks_succeeded_total %d\n# TYPE jobraft_tasks_failed_total counter\njobraft_tasks_failed_total %d\n# TYPE jobraft_tasks_retried_total counter\njobraft_tasks_retried_total %d\n# TYPE jobraft_tasks_canceled_total counter\njobraft_tasks_canceled_total %d\n# TYPE jobraft_tasks_pending gauge\njobraft_tasks_pending %d\n# TYPE jobraft_tasks_running gauge\njobraft_tasks_running %d\n# TYPE jobraft_tasks_retrying gauge\njobraft_tasks_retrying %d\n# TYPE jobraft_workers_online gauge\njobraft_workers_online %d\n", m.Submitted, m.Succeeded, m.Failed, m.Retried, m.Canceled, pending, running, retrying, workers)
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
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
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
	escapedPath := r.URL.EscapedPath()
	if strings.HasSuffix(escapedPath, "/claim") {
		s.workerClaim(w, r)
		return
	}
	if strings.HasSuffix(escapedPath, "/renew") {
		s.workerRenew(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	id, err := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/workers/"))
	if err != nil || id == "" {
		http.NotFound(w, r)
		return
	}
	wkr, err := s.scheduler.Heartbeat(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, wkr)
}

func (s *Server) workerRenew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	parts := strings.Split(strings.TrimPrefix(r.URL.EscapedPath(), "/workers/"), "/")
	if len(parts) != 3 || parts[1] != "tasks" || parts[2] != "renew" {
		http.NotFound(w, r)
		return
	}
	workerID, err := url.PathUnescape(parts[0])
	if err != nil || workerID == "" {
		http.NotFound(w, r)
		return
	}
	var req struct {
		LeaseToken string `json:"lease_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	t, err := s.scheduler.RenewTaskLease(workerID, r.URL.Query().Get("task"), req.LeaseToken)
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) workerClaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path, err := url.PathUnescape(strings.TrimSuffix(strings.TrimPrefix(r.URL.EscapedPath(), "/workers/"), "/claim"))
	if err != nil || path == "" {
		http.NotFound(w, r)
		return
	}
	wait := r.URL.Query().Get("wait")
	deadline := time.Now()
	if wait != "" {
		if d, err := time.ParseDuration(wait); err == nil && d > 0 && d <= 30*time.Second {
			deadline = deadline.Add(d)
		}
	}
	var t task.Task
	for {
		t, err = s.scheduler.Claim(path)
		if err == nil {
			writeJSON(w, http.StatusOK, t)
			return
		}
		if err != scheduler.ErrNoTask || !deadline.After(time.Now()) {
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
	}
	if err == scheduler.ErrNoTask {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
}

func (s *Server) tasks(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var req struct {
			IDs []string `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.IDs) == 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ids must be a non-empty array"})
			return
		}
		result := make([]map[string]interface{}, 0, len(req.IDs))
		for _, id := range req.IDs {
			err := s.scheduler.Cancel(id)
			item := map[string]interface{}{"id": id, "canceled": err == nil}
			if err != nil {
				item["error"] = err.Error()
			}
			result = append(result, item)
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if r.Method == http.MethodGet {
		items, err := s.scheduler.List()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		statusFilter, nameFilter := r.URL.Query().Get("status"), r.URL.Query().Get("name")
		if statusFilter != "" && !validStatus(statusFilter) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid status filter"})
			return
		}
		filtered := items[:0]
		for _, item := range items {
			if statusFilter != "" && string(item.Status) != statusFilter {
				continue
			}
			if nameFilter != "" && item.Name != nameFilter {
				continue
			}
			filtered = append(filtered, item)
		}
		limit := 0
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil || parsed < 1 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "limit must be a positive integer"})
				return
			}
			limit = parsed
		}
		if limit > 0 && len(filtered) > limit {
			filtered = filtered[:limit]
		}
		writeJSON(w, http.StatusOK, filtered)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
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

func validStatus(value string) bool {
	switch task.Status(value) {
	case task.StatusPending, task.StatusRunning, task.StatusSuccess, task.StatusFailed, task.StatusRetrying, task.StatusCanceled:
		return true
	default:
		return false
	}
}

func (s *Server) taskByID(w http.ResponseWriter, r *http.Request) {
	id, err := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/tasks/"))
	if err != nil || id == "" {
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
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var req struct {
			WorkerID   string          `json:"worker_id"`
			LeaseToken string          `json:"lease_token"`
			Error      string          `json:"error"`
			Result     json.RawMessage `json:"result"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := s.scheduler.CompleteTaskWithResult(req.WorkerID, id, req.LeaseToken, req.Error, req.Result); err != nil {
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
