package cluster

import (
	"testing"
	"time"
)

func TestMemoryElectionChoosesStableLeader(t *testing.T) {
	r := NewMemoryRegistry()
	a, _ := NewElector(r, "node-b", 100*time.Millisecond)
	b, _ := NewElector(r, "node-a", 100*time.Millisecond)
	a.Start()
	b.Start()
	defer a.Stop()
	defer b.Stop()
	time.Sleep(20 * time.Millisecond)
	leader, ok := r.Leader()
	if !ok || leader.ID != "node-a" {
		t.Fatalf("unexpected leader: %+v %v", leader, ok)
	}
}
