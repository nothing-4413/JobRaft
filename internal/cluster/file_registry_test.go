package cluster

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileRegistryElectsLeader(t *testing.T) {
	r, err := NewFileRegistry(filepath.Join(t.TempDir(), "cluster.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Renew("node-b", time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Renew("node-a", time.Second); err != nil {
		t.Fatal(err)
	}
	leader, ok := r.Leader()
	if !ok || leader.ID != "node-a" {
		t.Fatalf("leader=%+v ok=%v", leader, ok)
	}
}
