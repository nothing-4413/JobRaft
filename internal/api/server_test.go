package api

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

func TestTaskHTTPFlow(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	_ = s.Register("echo", func(context.Context, task.Task) error { return nil })
	s.Start(context.Background())
	defer s.Stop()
	tsrv := httptest.NewServer(New(s).Handler())
	defer tsrv.Close()

	body := `{"id":"api-1","name":"echo","retry":{"max_attempts":1}}`
	resp, err := http.Post(tsrv.URL+"/tasks", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: %d", resp.StatusCode)
	}
	resp.Body.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, err := http.Get(tsrv.URL + "/tasks/api-1")
		if err != nil {
			t.Fatal(err)
		}
		var item task.Task
		_ = json.NewDecoder(got.Body).Decode(&item)
		got.Body.Close()
		if item.Status == task.StatusSuccess {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	metrics, err := http.Get(tsrv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := ioutil.ReadAll(metrics.Body)
	metrics.Body.Close()
	if !strings.Contains(string(b), "jobraft_tasks_submitted_total 1") {
		t.Fatalf("metrics missing submission: %s", b)
	}
}

func TestTaskPayloadIsJSONNotBase64(t *testing.T) {
	s := scheduler.New(store.NewMemory(), 1)
	ts := httptest.NewServer(New(s).Handler())
	defer ts.Close()
	resp, err := http.Post(ts.URL+"/tasks", "application/json", strings.NewReader(`{"id":"json-1","name":"remote","payload":{"message":"hello"},"retry":{"max_attempts":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `"payload":{"message":"hello"}`) {
		t.Fatalf("payload was not emitted as JSON: %s", b)
	}
}
