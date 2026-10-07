// Command demo-worker is the small external Worker used by the walkthrough in
// docs/demo.md. It exercises the whole protocol through pkg/workerclient:
// register, heartbeat, long-poll claim, renew the lease while the handler runs,
// then complete with an error or a success.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nothing-4413/JobRaft/internal/task"
	"github.com/nothing-4413/JobRaft/pkg/workerclient"
)

func main() {
	var (
		api      = flag.String("api", "http://localhost:8080", "API base URL")
		workerID = flag.String("worker", "demo-1", "worker id reported to the API")
		token    = flag.String("token", "local-dev-token", "bearer token, empty disables authentication")
		work     = flag.Duration("work", 2*time.Second, "how long the handler pretends to work")
		name     = flag.String("name", "", "only handle tasks with this name, empty handles every task")
		failOnce = flag.Bool("fail-first", false, "fail the first task on purpose to show the retry path")
	)
	flag.Parse()

	client := &workerclient.Client{
		BaseURL:      *api,
		WorkerID:     *workerID,
		APIToken:     *token,
		PollInterval: 500 * time.Millisecond,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	failed := false
	handler := func(ctx context.Context, t task.Task) error {
		lease := "none"
		if t.LeaseUntil != nil {
			lease = t.LeaseUntil.Format(time.RFC3339)
		}
		fmt.Printf("[%s] claimed %s name=%q attempt=%d lease_until=%s\n", *workerID, t.ID, t.Name, t.Attempts, lease)
		if *name != "" && t.Name != *name {
			return fmt.Errorf("task name %q is not handled by this worker", t.Name)
		}
		select {
		case <-time.After(*work):
		case <-ctx.Done():
			// The client cancels this context when it can no longer renew the
			// lease, so the task is left for scheduler lease recovery instead of
			// being acknowledged.
			fmt.Printf("[%s] %s interrupted after %s: %v\n", *workerID, t.ID, *work, ctx.Err())
			return ctx.Err()
		}
		if *failOnce && !failed {
			failed = true
			fmt.Printf("[%s] failing %s on purpose\n", *workerID, t.ID)
			return errors.New("deliberate failure")
		}
		fmt.Printf("[%s] completed %s\n", *workerID, t.ID)
		return nil
	}

	if err := client.Run(ctx, handler); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("worker %s stopped: %v", *workerID, err)
	}
}
