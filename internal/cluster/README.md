# internal/cluster

Leader election and an embedded replicated-log consensus model for scheduler
nodes.

## Leader election

- `Registry` is the minimal interface (`Renew`, `Leader`, `List`).
- `MemoryRegistry` is a deterministic in-process registry for tests and local
  clusters.
- `FileRegistry` coordinates processes through a shared JSON file guarded by an
  exclusive lock file; expired leases are removed on renewal so a crashed node's
  seat is released.
- `Elector` runs a renewal loop (at TTL / 3) and exposes `IsLeader`, which the
  scheduler uses as a leader gate.

The `Registry` interface is the boundary for a Raft/etcd-backed implementation in
production.

## Consensus model

`ConsensusGroup` and `ConsensusRegistry` provide a quorum state machine over a
`ReplicatedLog` interface for deterministic tests and local integration. They
intentionally do not claim to replace full Raft across untrusted networks;
`ReplicatedLog` is the seam for adding a real transport later.
