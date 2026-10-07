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
	// Reuse connections. The default idle pool keeps only two per host, so a run
	// of a few thousand requests closes nearly every connection and parks the
	// host's ephemeral ports in TIME_WAIT. That both skews the measured latency
	// and, once the local port range fills up, makes back-to-back runs fail with
	// "only one usage of each socket address".
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 256
	client := &http.Client{Timeout: 15 * time.Second, Transport: transport}
	// Cleanup deletes rows an earlier run left behind. That work scales with the
	// leftovers, not with this run, so it stays outside the measured window and
	// is reported on its own: counting it made a run that started on a populated
	// store look several times slower than the same run on an empty one.
	cleanupStart := time.Now()
	for _, endpoint := range endpoints {
		if err := cleanupBenchmarkTasks(ctx, client, endpoint, token); err != nil {
			fatal(err)
		}
	}
	cleanupDuration := time.Since(cleanupStart)
	start := time.Now()
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
	workDone := make(chan struct{})
	go func() { wg.Wait(); close(workDone) }()
	select {
	case <-workDone:
	case <-ctx.Done():
		fatal(fmt.Errorf("benchmark timed out after %s", timeout))
	}
	if err := firstError(errCh); err != nil {
		fatal(err)
	}
	// Every worker checks the completion counter before claiming, so the run can
	// overshoot the target by up to workers-1 tasks: several workers pass the check
	// on the same tick and each finishes one more task. Only a shortfall is a real
	// problem, and duplicate_ids reports the other failure mode, a server that
	// handed the same task to more than one worker.
	if got := atomic.LoadInt64(&completed); got < int64(total) {
		fatal(fmt.Errorf("completed %d of %d tasks", got, total))
	}
	elapsed := time.Since(start)
	reportDuplicateCompletions()
	fmt.Printf("cleanup_duration=%s submitted=%d submit_duration=%s completed=%d elapsed=%s throughput=%.2f tasks/s\n", cleanupDuration.Round(time.Millisecond), total, submitFinished.Sub(start).Round(time.Millisecond), completed, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
	for _, endpoint := range endpoints {
		if metrics, err := get(ctx, client, endpoint+"/metrics", token); err == nil {
			fmt.Printf("metrics[%s]:\n%s", endpoint, metrics)
		}
	}
}

// cleanupBenchmarkTasks removes records from an earlier run so this run's counts
// are not polluted by leftovers. purge=true deletes them outright: a plain DELETE
// only cancels, which leaves the records behind and lets the next run count them
// a second time. Every endpoint is cleaned because two APIs with separate stores
// (the in-memory configuration) do not share a queue.
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
	return requestJSON(ctx, client, http.MethodDelete, endpoint+"/tasks?purge=true", token, map[string]interface{}{"ids": ids}, http.StatusOK, nil)
}

// firstError drains every recorded worker failure so no error is silently lost
// when more than one worker fails before the run shuts down.
func firstError(errCh <-chan error) error {
	var first error
	for {
		select {
		case err := <-errCh:
			if first == nil {
				first = err
			}
		default:
			return first
		}
	}
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

// runWorker drives one external worker until the run has delivered every task.
// The completion request goes back to the endpoint that granted the claim: with
// per-instance stores the lease only exists on that instance, and even with a
// shared database keeping the pair on one endpoint measures a realistic client.
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
		// Complete every claim in hand: a dropped running task times out and gets
		// retried, which corrupts the throughput number.
		body := map[string]string{"worker_id": workerID, "lease_token": claimed.LeaseToken}
		if err := postJSON(ctx, client, endpoint+"/tasks/"+url.PathEscape(claimed.ID)+"?complete=true", token, body, http.StatusNoContent, nil); err != nil {
			return err
		}
		recordCompletion(claimed.ID)
		atomic.AddInt64(completed, 1)
	}
	return nil
}

var (
	completionMu    sync.Mutex
	completionCount = map[string]int{}
)

func recordCompletion(id string) {
	completionMu.Lock()
	completionCount[id]++
	completionMu.Unlock()
}

func reportDuplicateCompletions() {
	completionMu.Lock()
	defer completionMu.Unlock()
	duplicates := 0
	worst := 0
	for _, count := range completionCount {
		if count > 1 {
			duplicates++
		}
		if count > worst {
			worst = count
		}
	}
	fmt.Printf("unique_tasks_completed=%d duplicate_ids=%d max_completions_per_id=%d\n", len(completionCount), duplicates, worst)
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
