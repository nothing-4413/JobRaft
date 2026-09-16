package cluster

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)

// FileRegistry coordinates nodes through a shared JSON file. An exclusive
// lock file serializes updates across processes; expired leases are removed on
// every renewal, allowing another node to take over after a crash.
type FileRegistry struct {
	path string
	mu   sync.Mutex
}

func NewFileRegistry(path string) (*FileRegistry, error) {
	if path == "" {
		return nil, errors.New("registry path is required")
	}
	return &FileRegistry{path: path}, nil
}

func (r *FileRegistry) Renew(id string, ttl time.Duration) (Node, error) {
	if id == "" || ttl <= 0 {
		return Node{}, errors.New("node id and positive ttl are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	unlock, err := r.lock()
	if err != nil {
		return Node{}, err
	}
	defer unlock()
	nodes := r.read()
	now := time.Now()
	for key, n := range nodes {
		if !n.LeaseUntil.After(now) {
			delete(nodes, key)
		}
	}
	n := nodes[id]
	n.ID, n.LastContact, n.LeaseUntil, n.Role = id, now, now.Add(ttl), Follower
	nodes[id] = n
	ids := make([]string, 0, len(nodes))
	for key := range nodes {
		ids = append(ids, key)
	}
	sort.Strings(ids)
	if len(ids) > 0 {
		leader := nodes[ids[0]]
		leader.Role = Leader
		nodes[ids[0]] = leader
	}
	if err := r.write(nodes); err != nil {
		return Node{}, err
	}
	return nodes[id], nil
}

func (r *FileRegistry) Leader() (Node, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	nodes := r.read()
	now := time.Now()
	for id, n := range nodes {
		if !n.LeaseUntil.After(now) {
			delete(nodes, id)
			continue
		}
		if n.Role == Leader && n.LeaseUntil.After(now) {
			return n, true
		}
	}
	return Node{}, false
}
func (r *FileRegistry) List() []Node {
	r.mu.Lock()
	defer r.mu.Unlock()
	nodes := r.read()
	now := time.Now()
	result := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if n.LeaseUntil.After(now) {
			result = append(result, n)
		}
	}
	return result
}

func (r *FileRegistry) read() map[string]Node {
	result := make(map[string]Node)
	b, err := ioutil.ReadFile(r.path)
	if err == nil {
		_ = json.Unmarshal(b, &result)
	}
	return result
}
func (r *FileRegistry) write(nodes map[string]Node) error {
	b, err := json.MarshalIndent(nodes, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := ioutil.WriteFile(tmp, b, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}
func (r *FileRegistry) lock() (func(), error) {
	lockPath := r.path + ".lock"
	for i := 0; i < 50; i++ {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			f.Close()
			return func() { _ = os.Remove(lockPath) }, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, errors.New("registry lock timeout")
}
