package workerclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/nothing-4413/JobRaft/internal/task"
)

type Handler func(context.Context, task.Task) error

var ErrNoTask = errors.New("no task available")

type Client struct {
	BaseURL, WorkerID string
	HTTPClient        *http.Client
	PollInterval      time.Duration
}

func (c *Client) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}
func (c *Client) Register(ctx context.Context) error {
	return c.postJSON(ctx, "/workers", map[string]string{"id": c.WorkerID}, nil)
}
func (c *Client) Heartbeat(ctx context.Context) error {
	return c.postJSON(ctx, "/workers/"+c.WorkerID, nil, nil)
}

func (c *Client) Claim(ctx context.Context) (task.Task, error) {
	return c.ClaimWait(ctx, 0)
}

func (c *Client) ClaimWait(ctx context.Context, wait time.Duration) (task.Task, error) {
	var t task.Task
	path := "/workers/" + c.WorkerID + "/claim"
	if wait > 0 {
		path += "?wait=" + wait.String()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, nil)
	if err != nil {
		return t, err
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return t, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return t, ErrNoTask
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		return t, errors.New(string(b))
	}
	return t, json.NewDecoder(resp.Body).Decode(&t)
}

func (c *Client) Complete(ctx context.Context, t task.Task, err error) error {
	return c.CompleteWithResult(ctx, t, err, nil)
}

func (c *Client) RenewLease(ctx context.Context, t task.Task) (task.Task, error) {
	var renewed task.Task
	path := "/workers/" + c.WorkerID + "/tasks/renew?task=" + t.ID
	if err := c.postJSON(ctx, path, map[string]string{"lease_token": t.LeaseToken}, &renewed); err != nil {
		return renewed, err
	}
	return renewed, nil
}

func (c *Client) CompleteWithResult(ctx context.Context, t task.Task, err error, result []byte) error {
	payload := map[string]interface{}{"worker_id": c.WorkerID, "lease_token": t.LeaseToken}
	if err != nil {
		payload["error"] = err.Error()
	}
	if result != nil {
		payload["result"] = json.RawMessage(result)
	}
	return c.postJSON(ctx, "/tasks/"+t.ID+"?complete=true", payload, nil)
}

func (c *Client) Run(ctx context.Context, handler Handler) error {
	if c.WorkerID == "" || c.BaseURL == "" || handler == nil {
		return errors.New("worker id, base URL and handler are required")
	}
	if err := c.Register(ctx); err != nil {
		return err
	}
	interval := c.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := c.Heartbeat(ctx); err != nil {
			return err
		}
		t, err := c.ClaimWait(ctx, interval)
		if err == nil {
			runErr := handler(ctx, t)
			if completeErr := c.Complete(ctx, t, runErr); completeErr != nil {
				return completeErr
			}
			continue
		}
		if !errors.Is(err, ErrNoTask) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Client) postJSON(ctx context.Context, path string, body interface{}, out interface{}) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := ioutil.ReadAll(resp.Body)
		return errors.New(string(b))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
