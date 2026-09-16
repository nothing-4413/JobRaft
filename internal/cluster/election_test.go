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
