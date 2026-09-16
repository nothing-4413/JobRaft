package cluster

import (
	"os"
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

func TestFileRegistryTakesOverExpiredLeader(t *testing.T) {
	r, err := NewFileRegistry(filepath.Join(t.TempDir(), "cluster.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Renew("node-a", 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err = r.Renew("node-b", time.Second); err != nil {
		t.Fatal(err)
	}
	leader, ok := r.Leader()
	if !ok || leader.ID != "node-b" {
		t.Fatalf("leader=%+v ok=%v", leader, ok)
	}
}

func TestFileRegistryRemovesStaleLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.json")
	r, err := NewFileRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := path + ".lock"
	if err := os.WriteFile(lockPath, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Renew("node-a", time.Second); err != nil {
		t.Fatal(err)
	}
}
