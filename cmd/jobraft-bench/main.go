// jobraft-bench submits and completes tasks through one or more JobRaft APIs.
// It is intentionally dependency-free so benchmark results are reproducible.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nothing-4413/JobRaft/internal/task"
)

func main() {
	var endpointList, token string
	var total, workers, submitParallelism int
	var timeout time.Duration
	flag.StringVar(&endpointList, "endpoints", "http://localhost:8080,http://localhost:8081", "comma-separated JobRaft API URLs")
	flag.StringVar(&token, "token", "local-dev-token", "bearer token")
	flag.IntVar(&total, "tasks", 1000, "number of tasks to submit")
	flag.IntVar(&workers, "workers", 8, "number of concurrent external workers")
	flag.IntVar(&submitParallelism, "submit-parallelism", 16, "number of concurrent submitters")
	flag.DurationVar(&timeout, "timeout", 2*time.Minute, "end-to-end timeout")
	flag.Parse()
	if total < 1 || workers < 1 || submitParallelism < 1 || timeout <= 0 {
		fmt.Fprintln(os.Stderr, "tasks, workers, submit-parallelism, and timeout must be positive")
		os.Exit(2)
	}
	endpoints := parseEndpoints(endpointList)
	if len(endpoints) == 0 {
		fmt.Fprintln(os.Stderr, "at least one endpoint is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}
	start := time.Now()
	if err := cleanupBenchmarkTasks(ctx, client, endpoints[0], token); err != nil {
		fatal(err)
	}
	if err := submit(ctx, client, endpoints, token, total, submitParallelism); err != nil {
		fatal(err)
	}
	submitFinished := time.Now()
	var completed int64
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for index := 0; index < workers; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runWorker(ctx, client, endpoints, token, fmt.Sprintf("bench-%d-%d", start.UnixNano(), index), total, &completed, index); err != nil {
				errCh <- err
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		fatal(fmt.Errorf("benchmark timed out after %s", timeout))
	}
	select {
	case err := <-errCh:
		fatal(err)
	default:
	}
	if got := atomic.LoadInt64(&completed); got != int64(total) {
		fatal(fmt.Errorf("completed %d of %d tasks", got, total))
	}
	elapsed := time.Since(start)
	fmt.Printf("submitted=%d submit_duration=%s completed=%d elapsed=%s throughput=%.2f tasks/s\n", total, submitFinished.Sub(start).Round(time.Millisecond), completed, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
	for _, endpoint := range endpoints {
		if metrics, err := get(ctx, client, endpoint+"/metrics", token); err == nil {
			fmt.Printf("metrics[%s]:\n%s", endpoint, metrics)
		}
	}
}

func cleanupBenchmarkTasks(ctx context.Context, client *http.Client, endpoint, token string) error {
	var items []task.Task
	if err := getJSON(ctx, client, endpoint+"/tasks?name=benchmark&limit=100000", token, &items); err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return requestJSON(ctx, client, http.MethodDelete, endpoint+"/tasks", token, map[string]interface{}{"ids": ids}, http.StatusOK, nil)
}

func parseEndpoints(value string) []string {
	var endpoints []string
	for _, raw := range strings.Split(value, ",") {
		endpoint := strings.TrimRight(strings.TrimSpace(raw), "/")
		if endpoint != "" {
			endpoints = append(endpoints, endpoint)
		}
	}
	return endpoints
}

func submit(ctx context.Context, client *http.Client, endpoints []string, token string, total, parallelism int) error {
	jobs := make(chan int)
	errCh := make(chan error, parallelism)
	var wg sync.WaitGroup
	for worker := 0; worker < parallelism; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sequence := range jobs {
				body := map[string]interface{}{"name": "benchmark", "payload": map[string]int{"sequence": sequence}, "retry": map[string]int{"max_attempts": 1}}
				if err := postJSON(ctx, client, endpoints[(worker+sequence)%len(endpoints)]+"/tasks", token, body, http.StatusCreated, nil); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	for sequence := 0; sequence < total; sequence++ {
		select {
		case jobs <- sequence:
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func runWorker(ctx context.Context, client *http.Client, endpoints []string, token, workerID string, total int, completed *int64, offset int) error {
	if err := postJSON(ctx, client, endpoints[offset%len(endpoints)]+"/workers", token, map[string]string{"id": workerID}, http.StatusCreated, nil); err != nil {
		return err
	}
	for request := 0; atomic.LoadInt64(completed) < int64(total); request++ {
		endpoint := endpoints[(offset+request)%len(endpoints)]
		if err := postJSON(ctx, client, endpoint+"/workers/"+url.PathEscape(workerID), token, nil, http.StatusOK, nil); err != nil {
			return err
		}
		var claimed task.Task
		status, err := postJSONStatus(ctx, client, endpoint+"/workers/"+url.PathEscape(workerID)+"/claim?wait=1s", token, nil, &claimed)
		if err != nil {
			return err
		}
		if status == http.StatusNoContent {
			continue
		}
		if status != http.StatusOK {
			return fmt.Errorf("claim returned HTTP %d", status)
		}
		body := map[string]string{"worker_id": workerID, "lease_token": claimed.LeaseToken}
		if err := postJSON(ctx, client, endpoints[(offset+request+1)%len(endpoints)]+"/tasks/"+url.PathEscape(claimed.ID)+"?complete=true", token, body, http.StatusNoContent, nil); err != nil {
			return err
		}
		atomic.AddInt64(completed, 1)
	}
	return nil
}

func postJSON(ctx context.Context, client *http.Client, endpoint, token string, body interface{}, expected int, out interface{}) error {
	return requestJSON(ctx, client, http.MethodPost, endpoint, token, body, expected, out)
}

func requestJSON(ctx context.Context, client *http.Client, method, endpoint, token string, body interface{}, expected int, out interface{}) error {
	status, err := requestJSONStatus(ctx, client, method, endpoint, token, body, out)
	if err != nil {
		return err
	}
	if status != expected {
		return fmt.Errorf("%s returned HTTP %d", endpoint, status)
	}
	return nil
}

func postJSONStatus(ctx context.Context, client *http.Client, endpoint, token string, body, out interface{}) (int, error) {
	return requestJSONStatus(ctx, client, http.MethodPost, endpoint, token, body, out)
}

func requestJSONStatus(ctx context.Context, client *http.Client, method, endpoint, token string, body, out interface{}) (int, error) {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return 0, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(response.Body)
		return response.StatusCode, fmt.Errorf("%s: HTTP %d: %s", endpoint, response.StatusCode, strings.TrimSpace(string(message)))
	}
	if out != nil && response.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(response.Body).Decode(out); err != nil {
			return response.StatusCode, err
		}
	}
	return response.StatusCode, nil
}

func getJSON(ctx context.Context, client *http.Client, endpoint, token string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP %d", endpoint, response.StatusCode)
	}
	return json.NewDecoder(response.Body).Decode(out)
}

func get(ctx context.Context, client *http.Client, endpoint, token string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return string(data), err
}

func fatal(err error) { fmt.Fprintln(os.Stderr, "jobraft-bench:", err); os.Exit(1) }
