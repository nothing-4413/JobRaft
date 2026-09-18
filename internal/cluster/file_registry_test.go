package cluster

import (
	"encoding/json"
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

func TestFileListRefreshesLeaderRole(t *testing.T) {
	r, err := NewFileRegistry(filepath.Join(t.TempDir(), "cluster.json"))
	if err != nil {
		t.Fatal(err)
	}
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

func TestFileRegistryRejectsCorruptState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(path, []byte("not-json"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := NewFileRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Renew("node-a", time.Second); err == nil {
		t.Fatal("expected corrupt registry error")
	}
}

func TestFileRegistryPersistsExpiredNodeCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.json")
	old := time.Now().Add(-time.Minute)
	state, _ := json.Marshal(map[string]Node{"expired": {ID: "expired", LeaseUntil: old}})
	if err := os.WriteFile(path, state, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := NewFileRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Leader(); ok {
		t.Fatal("expired node elected as leader")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var nodes map[string]Node
	if err := json.Unmarshal(b, &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expired node was not removed: %+v", nodes)
	}
}
