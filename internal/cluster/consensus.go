package cluster

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type LogEntry struct {
	Term        uint64    `json:"term"`
	Index       uint64    `json:"index"`
	Command     []byte    `json:"command"`
	CommittedAt time.Time `json:"committed_at"`
}

// ReplicatedLog is the minimal state-machine contract used by ConsensusGroup.
type ReplicatedLog interface {
	Apply(LogEntry) error
	Snapshot() ([]byte, error)
}

type ConsensusGroup struct {
	mu        sync.Mutex
	id, group string
	registry  *ConsensusRegistry
	term      uint64
	commit    uint64
	log       []LogEntry
	machine   ReplicatedLog
}

type ConsensusRegistry struct {
	mu     sync.Mutex
	groups map[string]*ConsensusGroup
	leases map[string]time.Time
}

func NewConsensusRegistry() *ConsensusRegistry {
	return &ConsensusRegistry{groups: make(map[string]*ConsensusGroup), leases: make(map[string]time.Time)}
}
func (r *ConsensusRegistry) Join(group, id string, machine ReplicatedLog) (*ConsensusGroup, error) {
	if group == "" || id == "" || machine == nil {
		return nil, errors.New("group, node id and state machine are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := group + "/" + id
	if g := r.groups[key]; g != nil {
		return g, nil
	}
	g := &ConsensusGroup{id: id, group: group, registry: r, machine: machine}
	r.groups[key] = g
	return g, nil
}
func (r *ConsensusRegistry) members(group string) []*ConsensusGroup {
	result := make([]*ConsensusGroup, 0)
	for key, g := range r.groups {
		if len(key) > len(group) && key[:len(group)+1] == group+"/" {
			result = append(result, g)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return result
}
func (r *ConsensusRegistry) leader(group string) *ConsensusGroup {
	now := time.Now()
	var best *ConsensusGroup
	for _, g := range r.members(group) {
		if until := r.leases[group+"/"+g.id]; until.After(now) {
			if best == nil || g.id < best.id {
				best = g
			}
		}
	}
	return best
}

func (g *ConsensusGroup) GroupName() string { return g.group }
func (g *ConsensusGroup) Renew(ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("positive ttl required")
	}
	g.registry.mu.Lock()
	g.registry.leases[g.GroupName()+"/"+g.id] = time.Now().Add(ttl)
	g.registry.mu.Unlock()
	return nil
}
func (g *ConsensusGroup) IsLeader() bool {
	g.registry.mu.Lock()
	defer g.registry.mu.Unlock()
	return g.registry.leader(g.GroupName()) == g
}
func (g *ConsensusGroup) Term() uint64        { g.mu.Lock(); defer g.mu.Unlock(); return g.term }
func (g *ConsensusGroup) CommitIndex() uint64 { g.mu.Lock(); defer g.mu.Unlock(); return g.commit }
func (g *ConsensusGroup) Apply(command []byte) (LogEntry, error) {
	g.registry.mu.Lock()
	defer g.registry.mu.Unlock()
	if g.registry.leader(g.GroupName()) != g {
		return LogEntry{}, errors.New("not leader")
	}
	members := g.registry.members(g.GroupName())
	g.term++
	entry := LogEntry{Term: g.term, Index: g.commit + 1, Command: append([]byte(nil), command...), CommittedAt: time.Now()}
	applied := make([]*ConsensusGroup, 0, len(members))
	for _, m := range members {
		if err := m.machine.Apply(entry); err == nil {
			applied = append(applied, m)
		}
	}
	if len(applied)*2 <= len(members) {
		return LogEntry{}, errors.New("quorum unavailable")
	}
	for _, m := range applied {
		m.mu.Lock()
		m.log = append(m.log, entry)
		m.commit = entry.Index
		m.term = entry.Term
		m.mu.Unlock()
	}
	return entry, nil
}
func (g *ConsensusGroup) Log() []LogEntry {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]LogEntry(nil), g.log...)
}
