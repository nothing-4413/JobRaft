package cluster

import (
	"errors"
	"testing"
	"time"
)

type memoryMachine struct{ entries []LogEntry }

func (m *memoryMachine) Apply(e LogEntry) error    { m.entries = append(m.entries, e); return nil }
func (m *memoryMachine) Snapshot() ([]byte, error) { return []byte("snapshot"), nil }

type failingMachine struct{}

func (failingMachine) Apply(LogEntry) error      { return errors.New("unavailable") }
func (failingMachine) Snapshot() ([]byte, error) { return nil, nil }

func TestConsensusReplicatesToQuorum(t *testing.T) {
	r := NewConsensusRegistry()
	a := &memoryMachine{}
	b := &memoryMachine{}
	ga, _ := r.Join("jobs", "a", a)
	_, _ = r.Join("jobs", "b", b)
	_ = ga.Renew(time.Second)
	gb, _ := r.Join("jobs", "b", b)
	_ = gb.Renew(time.Second)
	if !ga.IsLeader() {
		t.Fatal("expected a leader")
	}
	e, err := ga.Apply([]byte("set x"))
	if err != nil || e.Index != 1 {
		t.Fatalf("apply failed: %+v %v", e, err)
	}
	if len(a.entries) != 1 || len(b.entries) != 1 {
		t.Fatalf("replication failed: %d %d", len(a.entries), len(b.entries))
	}
}

func TestConsensusRejectsFollowerApply(t *testing.T) {
	r := NewConsensusRegistry()
	a, _ := r.Join("jobs", "a", &memoryMachine{})
	b, _ := r.Join("jobs", "b", &memoryMachine{})
	_ = a.Renew(time.Second)
	_ = b.Renew(time.Second)
	if _, err := b.Apply([]byte("write")); err == nil {
		t.Fatal("expected follower apply to fail")
	}
}

func TestConsensusDoesNotCommitWithoutQuorum(t *testing.T) {
	r := NewConsensusRegistry()
	a, _ := r.Join("jobs", "a", &memoryMachine{})
	_, _ = r.Join("jobs", "b", failingMachine{})
	_ = a.Renew(time.Second)
	if _, err := a.Apply([]byte("write")); err == nil {
		t.Fatal("expected quorum error")
	}
	if a.CommitIndex() != 0 || len(a.Log()) != 0 {
		t.Fatalf("uncommitted entry leaked: index=%d log=%d", a.CommitIndex(), len(a.Log()))
	}
}
