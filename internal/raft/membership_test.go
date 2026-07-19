package raft

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestMembership_AddServer(t *testing.T) {
	peers := map[string]string{
		"node2": "localhost:7002",
		"node3": "localhost:7003",
	}
	cfg := testConfig("node1", peers)
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.mu.Unlock()

	change := MembershipChange{
		Type:    AddServer,
		NodeID:  "node4",
		Address: "localhost:7004",
	}

	err = node.ProposeMembershipChange(change)
	if err != nil {
		t.Fatalf("ProposeMembershipChange failed: %v", err)
	}

	entry, _ := node.log.GetEntry(node.log.LastIndex())
	err = node.applyMembership(entry)
	if err != nil {
		t.Fatalf("applyMembership failed: %v", err)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.clusterConfig.Size() != 4 {
		t.Errorf("expected cluster size 4, got %d", node.clusterConfig.Size())
	}
	if _, ok := node.nextIndex["node4"]; !ok {
		t.Errorf("nextIndex for node4 should be set")
	}
}

func TestMembership_RemoveServer(t *testing.T) {
	peers := map[string]string{
		"node2": "localhost:7002",
		"node3": "localhost:7003",
	}
	cfg := testConfig("node1", peers)
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.nextIndex["node3"] = 1
	node.matchIndex["node3"] = 1
	node.mu.Unlock()

	change := MembershipChange{
		Type:   RemoveServer,
		NodeID: "node3",
	}

	err = node.ProposeMembershipChange(change)
	if err != nil {
		t.Fatalf("ProposeMembershipChange failed: %v", err)
	}

	entry, _ := node.log.GetEntry(node.log.LastIndex())
	_ = node.applyMembership(entry)

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.clusterConfig.Size() != 2 {
		t.Errorf("expected cluster size 2, got %d", node.clusterConfig.Size())
	}
	if _, ok := node.nextIndex["node3"]; ok {
		t.Errorf("nextIndex for node3 should be deleted")
	}
}

func TestMembership_RemoveSelf_StepsDown(t *testing.T) {
	cfg := testConfig("node1", map[string]string{})
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.mu.Unlock()

	change := MembershipChange{
		Type:   RemoveServer,
		NodeID: "node1",
	}

	err = node.ProposeMembershipChange(change)
	if err != nil {
		t.Fatalf("ProposeMembershipChange failed: %v", err)
	}

	entry, _ := node.log.GetEntry(node.log.LastIndex())
	_ = node.applyMembership(entry)

	if node.State() != Follower {
		t.Errorf("expected to step down to Follower, got %s", node.State())
	}
}

func TestMembership_PendingChange_Rejected(t *testing.T) {
	cfg := testConfig("node1", map[string]string{})
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.pendingMembership = true
	node.mu.Unlock()

	change := MembershipChange{
		Type:   AddServer,
		NodeID: "node2",
	}

	err = node.ProposeMembershipChange(change)
	if err != ErrMembershipChangePending {
		t.Errorf("expected ErrMembershipChangePending, got %v", err)
	}
}

func TestMembership_NonLeader_Rejected(t *testing.T) {
	cfg := testConfig("node1", map[string]string{})
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	change := MembershipChange{Type: AddServer, NodeID: "node2"}
	err = node.ProposeMembershipChange(change)
	if err != ErrNotLeader {
		t.Errorf("expected ErrNotLeader, got %v", err)
	}
}

func TestMembership_ElectionUsesNewConfig(t *testing.T) {
	peers := map[string]string{
		"node2": "localhost:7002",
	}
	cfg := testConfig("node1", peers)

	var reqCount int32
	transport := &mockTransport{
		requestVoteFn: func(ctx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
			atomic.AddInt32(&reqCount, 1)
			return &RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
	}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.clusterConfig.AddPeer("node3", "localhost:7003")
	node.mu.Unlock()

	node.startElection()

	if atomic.LoadInt32(&reqCount) != 2 {
		t.Errorf("expected 2 vote requests sent to node2 and node3, got %d", reqCount)
	}
}

func TestMembership_CommitUsesNewConfig(t *testing.T) {
	peers := map[string]string{
		"node2": "localhost:7002",
		"node3": "localhost:7003",
	}
	cfg := testConfig("node1", peers)
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.mu.Unlock()

	_ = node.log.Append(LogEntry{Index: 1, Term: 1})
	node.mu.Lock()
	node.clusterConfig.RemovePeer("node3")
	node.matchIndex["node2"] = 1
	node.advanceCommitIndex()
	node.mu.Unlock()

	if node.CommitIndex() != 1 {
		t.Errorf("expected commitIndex 1, got %d", node.CommitIndex())
	}
}
