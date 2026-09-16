// Package cluster provides leader-election primitives for scheduler nodes.
// The in-memory registry is deterministic and useful for local clusters and
// tests; its Registry interface can be backed by Raft/etcd in production.
package cluster

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type Role string

const (
	Follower Role = "follower"
	Leader   Role = "leader"
)

type Node struct {
	ID          string    `json:"id"`
	Role        Role      `json:"role"`
	LastContact time.Time `json:"last_contact"`
	LeaseUntil  time.Time `json:"lease_until"`
}

type Registry interface {
	Renew(id string, ttl time.Duration) (Node, error)
	Leader() (Node, bool)
	List() []Node
}

type MemoryRegistry struct {
	mu    sync.Mutex
	nodes map[string]Node
}

func NewMemoryRegistry() *MemoryRegistry { return &MemoryRegistry{nodes: make(map[string]Node)} }

func (r *MemoryRegistry) Renew(id string, ttl time.Duration) (Node, error) {
	if id == "" || ttl <= 0 {
		return Node{}, errors.New("node id and positive ttl are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if _, ok := r.nodes[id]; !ok {
		r.nodes[id] = Node{ID: id}
	}
	for key, n := range r.nodes {
		if !n.LeaseUntil.After(now) {
			delete(r.nodes, key)
		}
	}
	n := r.nodes[id]
	n.LastContact, n.LeaseUntil, n.Role = now, now.Add(ttl), Follower
	ids := make([]string, 0, len(r.nodes))
	for key := range r.nodes {
		ids = append(ids, key)
	}
	sort.Strings(ids)
	if len(ids) > 0 {
		r.nodes[ids[0]] = Node{ID: ids[0], LastContact: now, LeaseUntil: r.nodes[ids[0]].LeaseUntil, Role: Leader}
	}
	r.nodes[id] = n
	if ids[0] == id {
		n.Role = Leader
		r.nodes[id] = n
	}
	return n, nil
}

func (r *MemoryRegistry) Leader() (Node, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for _, n := range r.nodes {
		if n.Role == Leader && n.LeaseUntil.After(now) {
			return n, true
		}
	}
	return Node{}, false
}
func (r *MemoryRegistry) List() []Node {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		result = append(result, n)
	}
	return result
}

type Elector struct {
	registry Registry
	id       string
	ttl      time.Duration
	mu       sync.RWMutex
	node     Node
	stop     chan struct{}
	done     chan struct{}
	started  bool
}

func NewElector(reg Registry, id string, ttl time.Duration) (*Elector, error) {
	if reg == nil || id == "" || ttl <= 0 {
		return nil, errors.New("registry, node id and positive ttl are required")
	}
	return &Elector{registry: reg, id: id, ttl: ttl, stop: make(chan struct{}), done: make(chan struct{})}, nil
}
func (e *Elector) Start() {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return
	}
	e.started = true
	e.mu.Unlock()
	go e.loop()
}
func (e *Elector) loop() {
	defer close(e.done)
	ticker := time.NewTicker(e.ttl / 3)
	defer ticker.Stop()
	e.renew()
	for {
		select {
		case <-ticker.C:
			e.renew()
		case <-e.stop:
			return
		}
	}
}
func (e *Elector) renew() {
	n, err := e.registry.Renew(e.id, e.ttl)
	if err == nil {
		e.mu.Lock()
		e.node = n
		e.mu.Unlock()
	}
}
func (e *Elector) Stop() {
	e.mu.RLock()
	started := e.started
	e.mu.RUnlock()
	if !started {
		return
	}
	select {
	case <-e.stop:
	default:
		close(e.stop)
	}
	<-e.done
}
func (e *Elector) Node() Node     { e.mu.RLock(); defer e.mu.RUnlock(); return e.node }
func (e *Elector) IsLeader() bool { return e.Node().Role == Leader }
