# pkg/workerclient

An SDK for building external JobRaft workers. Unlike the `internal/...` packages,
this one is importable by other Go programs.

## Quick start

```go
client := workerclient.Client{
    BaseURL:  "http://localhost:8080",
    WorkerID: "worker-1",
    APIToken: "local-dev-token",
}
err := client.Run(context.Background(), func(ctx context.Context, t task.Task) error {
    // process t.Payload; return an error to fail the task
    return nil
})
```

`Run` handles the register → heartbeat → long-poll claim → run → complete loop.
While a handler runs, `runHandler` renews the task lease at roughly one third of
its remaining TTL; if renewal fails it cancels the handler context and leaves
the task for scheduler lease recovery rather than acknowledging optimistically.

For finer control use `Register`, `Heartbeat`, `Claim`, `Complete`,
`CompleteWithResult`, and `RenewLease` directly.
