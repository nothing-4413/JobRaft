package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
			log.Printf("file store unavailable, using memory: %v", err)
		}
	}
	sch := scheduler.New(st, 4)
	_ = sch.Register("echo", func(ctx context.Context, t task.Task) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	sch.Start(ctx)
	apiServer := api.New(sch)
	var elector *cluster.Elector
	if nodeID := os.Getenv("JOBRAFT_NODE_ID"); nodeID != "" {
		registry := cluster.NewMemoryRegistry()
		elector, _ = cluster.NewElector(registry, nodeID, 10*time.Second)
		elector.Start()
		defer elector.Stop()
		sch.SetLeaderGate(elector)
		apiServer = api.NewWithCluster(sch, registry)
	}
	server := &http.Server{Addr: ":8080", Handler: apiServer.Handler(), ReadHeaderTimeout: 5 * time.Second}
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
