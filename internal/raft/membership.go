package raft

import (
	"context"
	"encoding/json"
	"sync"
)

type ClusterConfig struct {
	Peers map[string]string // NodeID -> address
}

func (c *ClusterConfig) Clone() *ClusterConfig {
	peers := make(map[string]string, len(c.Peers))
	for k, v := range c.Peers {
		peers[k] = v
	}
	return &ClusterConfig{Peers: peers}
}

func (c *ClusterConfig) AddPeer(nodeID, addr string) {
	c.Peers[nodeID] = addr
}

func (c *ClusterConfig) RemovePeer(nodeID string) {
	delete(c.Peers, nodeID)
}

func (c *ClusterConfig) HasPeer(nodeID string) bool {
	_, ok := c.Peers[nodeID]
	return ok
}

func (c *ClusterConfig) Size() int {
	return len(c.Peers)
}

func (c *ClusterConfig) Majority() int {
	return c.Size()/2 + 1
}

type ChangeType int

const (
	AddServer ChangeType = iota
	RemoveServer
)

type MembershipChange struct {
	Type    ChangeType `json:"type"`
	NodeID  string     `json:"node_id"`
	Address string     `json:"address,omitempty"`
}

func (r *RaftNode) ProposeMembershipChange(change MembershipChange) error {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return ErrNotLeader
	}

	if r.pendingMembership {
		r.mu.Unlock()
		return ErrMembershipChangePending
	}

	data, err := json.Marshal(change)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	entry := LogEntry{
		Index:   r.log.LastIndex() + 1,
		Term:    r.currentTerm,
		Type:    EntryMembership,
		Command: data,
	}

	if err := r.log.Append(entry); err != nil {
		r.mu.Unlock()
		return err
	}

	r.pendingMembership = true
	r.mu.Unlock()

	ctx := context.Background()
	var wg sync.WaitGroup
	r.mu.RLock()
	peers := r.clusterConfig.Clone()
	r.mu.RUnlock()

	for peerID := range peers.Peers {
		if peerID == r.config.NodeID {
			continue
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			r.mu.RLock()
			ni := r.nextIndex[id]
			r.mu.RUnlock()
			_ = r.replicateTo(ctx, id, ni)
		}(peerID)
	}
	wg.Wait()

	r.mu.Lock()
	r.advanceCommitIndex()
	r.mu.Unlock()

	return nil
}

func (r *RaftNode) applyMembership(entry LogEntry) error {
	var change MembershipChange
	if err := json.Unmarshal(entry.Command, &change); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	newConfig := r.clusterConfig.Clone()
	switch change.Type {
	case AddServer:
		newConfig.AddPeer(change.NodeID, change.Address)
		r.nextIndex[change.NodeID] = r.log.LastIndex() + 1
		r.matchIndex[change.NodeID] = 0
		// Transport'u güncelle — lock dışında çağır (deadlock önleme)
		go r.transport.AddPeer(change.NodeID, change.Address)
	case RemoveServer:
		newConfig.RemovePeer(change.NodeID)
		delete(r.nextIndex, change.NodeID)
		delete(r.matchIndex, change.NodeID)
		// Transport'u güncelle — lock dışında çağır
		go r.transport.RemovePeer(change.NodeID)
		if change.NodeID == r.config.NodeID {
			r.stepDown(r.currentTerm)
		}
	}

	r.clusterConfig = newConfig
	r.pendingMembership = false
	return nil
}
