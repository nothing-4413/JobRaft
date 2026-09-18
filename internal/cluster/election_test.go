package cluster

import (
	"testing"
	"time"
)

func TestMemoryElectionChoosesStableLeader(t *testing.T) {
	r := NewMemoryRegistry()
	if _, err := r.Renew("node-b", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Renew("node-a", time.Second); err != nil {
		t.Fatal(err)
	}
	leader, ok := r.Leader()
	if !ok || leader.ID != "node-a" {
		t.Fatalf("unexpected leader: %+v %v", leader, ok)
	}
}

func TestMemoryRegistryHasExactlyOneLeader(t *testing.T) {
	r := NewMemoryRegistry()
	if _, err := r.Renew("node-b", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Renew("node-a", time.Second); err != nil {
		t.Fatal(err)
	}
	leaders := 0
	for _, node := range r.List() {
		if node.Role == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected one leader, got %d: %+v", leaders, r.List())
	}
}

func TestMemoryListRefreshesLeaderRole(t *testing.T) {
	r := NewMemoryRegistry()
	_, _ = r.Renew("node-b", time.Second)
	_, _ = r.Renew("node-a", time.Second)
	for _, node := range r.List() {
		if node.ID == "node-a" && node.Role != Leader {
			t.Fatalf("node-a role=%s", node.Role)
		}
		if node.ID == "node-b" && node.Role == Leader {
			t.Fatal("node-b must not remain leader")
		}
	}
}

func TestElectorAcceptsSubMillisecondTTL(t *testing.T) {
	e, err := NewElector(NewMemoryRegistry(), "node-a", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	e.Start()
	e.Stop()
}
