# pkg/workerclient

用于构建外部 JobRaft Worker 的 SDK。与 `internal/...` 包不同，它可被其他 Go 程序导入。

## 快速上手

```go
client := workerclient.Client{
    BaseURL:  "http://localhost:8080",
    WorkerID: "worker-1",
    APIToken: "local-dev-token",
}
err := client.Run(context.Background(), func(ctx context.Context, t task.Task) error {
    // 处理 t.Payload；返回 error 即让任务失败
    return nil
})
```

`Run` 处理 register → heartbeat → 长轮询 claim → run → complete 循环。处理函数运行期间，`runHandler` 会按剩余 TTL 的约三分之一续期任务租约；若续期失败，它会取消处理函数上下文，并把任务留给调度器租约恢复，而不是乐观确认。

需要更细粒度控制时，可直接使用 `Register`、`Heartbeat`、`Claim`、`Complete`、`CompleteWithResult` 与 `RenewLease`。
