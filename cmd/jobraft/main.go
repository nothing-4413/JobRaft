package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/nothing-4413/JobRaft/internal/api"
	"github.com/nothing-4413/JobRaft/internal/cluster"
	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

func main() {
	var st store.Store = store.NewMemory()
	if path := os.Getenv("JOBRAFT_STORE"); path != "" {
		if persisted, err := store.NewFile(filepath.Clean(path)); err == nil {
			st = persisted
		} else {
			log.Fatalf("file store unavailable: %v", err)
		}
	}
	workers := 4
	if value := os.Getenv("JOBRAFT_WORKERS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			log.Fatalf("JOBRAFT_WORKERS must be a positive integer")
		}
		workers = parsed
	}
	sch := scheduler.New(st, workers)
	if value := os.Getenv("JOBRAFT_MAX_PENDING"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			sch.SetMaxPending(parsed)
		} else {
			log.Fatalf("JOBRAFT_MAX_PENDING must be a positive integer")
		}
	}
	if value := os.Getenv("JOBRAFT_LEASE_TTL"); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			sch.SetLeaseTTL(parsed)
		} else {
			log.Fatalf("JOBRAFT_LEASE_TTL must be a valid duration: %v", err)
		}
	}
	_ = sch.Register("echo", func(ctx context.Context, t task.Task) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	sch.Start(ctx)
	apiToken := os.Getenv("JOBRAFT_API_TOKEN")
	if apiToken == "" {
		log.Printf("warning: JOBRAFT_API_TOKEN is unset; API authentication is disabled")
	}
	apiServer := api.NewWithToken(sch, apiToken)
	var elector *cluster.Elector
	if nodeID := os.Getenv("JOBRAFT_NODE_ID"); nodeID != "" {
		var registry cluster.Registry = cluster.NewMemoryRegistry()
		if path := os.Getenv("JOBRAFT_CLUSTER_FILE"); path != "" {
			if shared, err := cluster.NewFileRegistry(filepath.Clean(path)); err == nil {
				registry = shared
			} else {
				log.Fatalf("cluster file unavailable: %v", err)
			}
		}
		var err error
		elector, err = cluster.NewElector(registry, nodeID, 10*time.Second)
		if err != nil {
			log.Fatalf("cluster election setup failed: %v", err)
		}
		elector.Start()
		defer elector.Stop()
		sch.SetLeaderGate(elector)
		apiServer = api.NewWithClusterToken(sch, registry, apiToken)
	}
	addr := os.Getenv("JOBRAFT_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	server := &http.Server{Addr: addr, Handler: apiServer.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	go func() {
		log.Printf("JobRaft listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	<-signals
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	sch.Stop()
}
