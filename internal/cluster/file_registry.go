package cluster

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/nothing-4413/JobRaft/internal/fileutil"
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
	nodes, err := r.read()
	if err != nil {
		return Node{}, err
	}
	now := time.Now()
	for key, n := range nodes {
		if !n.LeaseUntil.After(now) {
			delete(nodes, key)
		}
	}
	nodes[id] = Node{ID: id}
	n := nodes[id]
	n.ID, n.LastContact, n.LeaseUntil, n.Role = id, now, now.Add(ttl), Follower
	nodes[id] = n
	ids := make([]string, 0, len(nodes))
	for key, node := range nodes {
		node.Role = Follower
		nodes[key] = node
		ids = append(ids, key)
	}
	sort.Strings(ids)
	if len(ids) > 0 {
		leader := nodes[ids[0]]
		leader.Role = Leader
		nodes[ids[0]] = leader
	}
	if node := nodes[id]; node.Role == Leader {
		n.Role = Leader
	}
	if err := r.write(nodes); err != nil {
		return Node{}, err
	}
	return nodes[id], nil
}

func (r *FileRegistry) Leader() (Node, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	unlock, err := r.lock()
	if err != nil {
		return Node{}, false
	}
	defer unlock()
	nodes, err := r.read()
	if err != nil {
		return Node{}, false
	}
	now := time.Now()
	ids := make([]string, 0, len(nodes))
	for id, n := range nodes {
		if !n.LeaseUntil.After(now) {
			delete(nodes, id)
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		_ = r.write(nodes)
		return Node{}, false
	}
	sort.Strings(ids)
	n := nodes[ids[0]]
	n.Role = Leader
	nodes[ids[0]] = n
	_ = r.write(nodes)
	return n, true
}
func (r *FileRegistry) List() []Node {
	r.mu.Lock()
	defer r.mu.Unlock()
	unlock, err := r.lock()
	if err != nil {
		return nil
	}
	defer unlock()
	nodes, err := r.read()
	if err != nil {
		return nil
	}
	now := time.Now()
	ids := make([]string, 0, len(nodes))
	for id, node := range nodes {
		if node.LeaseUntil.After(now) {
			node.Role = Follower
			nodes[id] = node
			ids = append(ids, id)
		} else {
			delete(nodes, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > 0 {
		node := nodes[ids[0]]
		node.Role = Leader
		nodes[ids[0]] = node
	}
	_ = r.write(nodes)
	result := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if n.LeaseUntil.After(now) {
			result = append(result, n)
		}
	}
	return result
}

func (r *FileRegistry) read() (map[string]Node, error) {
	result := make(map[string]Node)
	b, err := ioutil.ReadFile(r.path)
	if os.IsNotExist(err) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return result, nil
	}
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, err
	}
	return result, nil
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
	return fileutil.Replace(tmp, r.path)
}
func (r *FileRegistry) lock() (func(), error) {
	lockPath := r.path + ".lock"
	for i := 0; i < 50; i++ {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			f.Close()
			return func() { _ = os.Remove(lockPath) }, nil
		}
		if info, statErr := os.Stat(lockPath); statErr == nil && time.Since(info.ModTime()) > 30*time.Second {
			_ = os.Remove(lockPath)
			continue
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, errors.New("registry lock timeout")
}
