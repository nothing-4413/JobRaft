package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nothing-4413/JobRaft/internal/api"
	"github.com/nothing-4413/JobRaft/internal/scheduler"
	"github.com/nothing-4413/JobRaft/internal/store"
	"github.com/nothing-4413/JobRaft/internal/task"
)

func main() {
	st := store.NewMemory()
	sch := scheduler.New(st, 4)
	_ = sch.Register("echo", func(ctx context.Context, t task.Task) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	sch.Start(ctx)
	server := &http.Server{Addr: ":8080", Handler: api.New(sch).Handler(), ReadHeaderTimeout: 5 * time.Second}
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
