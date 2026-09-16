package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/nothing-4413/JobRaft/internal/cluster"
	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/task"
)

type Server struct {
	scheduler *scheduler.Scheduler
	registry  cluster.Registry
}

func New(s *scheduler.Scheduler) *Server { return &Server{scheduler: s} }
func NewWithCluster(s *scheduler.Scheduler, r cluster.Registry) *Server {
	return &Server{scheduler: s, registry: r}
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
	return mux
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
	wait := r.URL.Query().Get("wait")
	deadline := time.Now()
	if wait != "" {
		if d, err := time.ParseDuration(wait); err == nil && d > 0 && d <= 30*time.Second {
			deadline = deadline.Add(d)
		}
	}
	var t task.Task
	var err error
	for {
		t, err = s.scheduler.Claim(path)
		if err == nil {
			writeJSON(w, http.StatusOK, t)
			return
		}
		if err != scheduler.ErrNoTask || !deadline.After(time.Now()) {
			break
		}
		time.Sleep(100 * time.Millisecond)
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
