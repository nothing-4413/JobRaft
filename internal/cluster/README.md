# internal/cluster

面向调度器节点的领导选举，以及内嵌的复制日志共识模型。

## 领导选举

- `Registry` 是最小接口（`Renew`、`Leader`、`List`）。
- `MemoryRegistry` 是确定性的进程内注册表，用于测试与本地集群。
- `FileRegistry` 通过共享 JSON 文件协调进程，并用排他锁文件串行化更新；续期时会移除过期租约，从而释放崩溃节点占用的席位。
- `Elector` 运行一个续期循环（TTL / 3），并暴露 `IsLeader`，调度器将其用作领导门控。

`Registry` 接口是生产环境中接入 Raft/etcd 后端的边界。

## 共识模型

`ConsensusGroup` 与 `ConsensusRegistry` 在 `ReplicatedLog` 接口之上提供 quorum 状态机，用于确定性测试与本地集成。它们刻意不宣称能在不可信网络上替代完整 Raft；`ReplicatedLog` 是后续接入真实传输层的接缝。
