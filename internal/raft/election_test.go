package raft

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockTransport, Transport interface'ini test için implement eder.
type mockTransport struct {
	requestVoteFn func(ctx context.Context, peerID string,
		req *RequestVoteRequest) (*RequestVoteResponse, error)
	addedPeers   map[string]string
	removedPeers map[string]bool
	mu           sync.Mutex
}

func (m *mockTransport) RequestVote(ctx context.Context, peerID string,
	req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if m.requestVoteFn != nil {
		return m.requestVoteFn(ctx, peerID, req)
	}
	return &RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (m *mockTransport) AppendEntries(ctx context.Context, peerID string,
	req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (m *mockTransport) InstallSnapshot(ctx context.Context, peerID string,
	req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	return &InstallSnapshotResponse{Term: req.Term}, nil
}

func (m *mockTransport) AddPeer(nodeID, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.addedPeers == nil {
		m.addedPeers = make(map[string]string)
	}
	m.addedPeers[nodeID] = address
}

func (m *mockTransport) RemovePeer(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.removedPeers == nil {
		m.removedPeers = make(map[string]bool)
	}
	m.removedPeers[nodeID] = true
}

// mockStateMachine, StateMachine interface'ini test için implement eder.
type mockStateMachine struct{}

func (m *mockStateMachine) Apply(entry LogEntry) error { return nil }
func (m *mockStateMachine) Snapshot() ([]byte, error)  { return nil, nil }
func (m *mockStateMachine) Restore([]byte) error       { return nil }

// testConfig, test için minimal Config oluşturur.
func testConfig(nodeID string, peers map[string]string) *Config {
	return &Config{
		NodeID:             nodeID,
		Peers:              peers,
		ElectionTimeoutMin: 2000 * time.Millisecond,
		ElectionTimeoutMax: 4000 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		DataDir:            "",
	}
}

func TestElection_SingleNode_BecomesLeader(t *testing.T) {
	cfg := testConfig("node1", map[string]string{})
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.startElection()

	if node.State() != Leader {
		t.Errorf("single node should become leader, got %s", node.State())
	}
}

func TestElection_ThreeNodes_WinWithMajority(t *testing.T) {
	peers := map[string]string{
		"node2": "localhost:7002",
		"node3": "localhost:7003",
	}
	cfg := testConfig("node1", peers)
	transport := &mockTransport{
		requestVoteFn: func(ctx context.Context, peerID string,
			req *RequestVoteRequest) (*RequestVoteResponse, error) {
			return &RequestVoteResponse{
				Term:        req.Term,
				VoteGranted: true,
			}, nil
		},
	}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.startElection()
	for i := 0; i < 10; i++ {
		if node.State() == Leader {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if node.State() != Leader {
		t.Errorf("should become leader with majority votes, got %s", node.State())
	}
}

func TestElection_ThreeNodes_LoseWithoutMajority(t *testing.T) {
	peers := map[string]string{
		"node2": "localhost:7002",
		"node3": "localhost:7003",
	}
	cfg := testConfig("node1", peers)
	transport := &mockTransport{
		requestVoteFn: func(ctx context.Context, peerID string,
			req *RequestVoteRequest) (*RequestVoteResponse, error) {
			return &RequestVoteResponse{
				Term:        req.Term,
				VoteGranted: false,
			}, nil
		},
	}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.startElection()

	if node.State() == Leader {
		t.Errorf("should not become leader without majority, got %s", node.State())
	}
}

func TestElection_HandleRequestVote_HigherTerm_BecomesFollower(t *testing.T) {
	cfg := testConfig("node1", map[string]string{})
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.state = Candidate
	node.currentTerm = 1
	node.mu.Unlock()

	req := &RequestVoteRequest{
		Term:         5,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp := node.handleRequestVote(req)
	node.mu.RLock()
	votedFor := node.votedFor
	node.mu.RUnlock()
	if votedFor != "node2" {
		t.Errorf("votedFor should be node2, got '%s'", votedFor)
	}

	if !resp.VoteGranted {
		t.Error("should grant vote to higher term candidate")
	}
	if node.State() != Follower {
		t.Errorf("should become follower on higher term, got %s", node.State())
	}
	if node.CurrentTerm() != 5 {
		t.Errorf("should update term to 5, got %d", node.CurrentTerm())
	}
}

func TestElection_HandleRequestVote_LowerTerm_Rejected(t *testing.T) {
	cfg := testConfig("node1", map[string]string{})
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()

	req := &RequestVoteRequest{
		Term:         2,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp := node.handleRequestVote(req)

	if resp.VoteGranted {
		t.Error("should reject vote from lower term candidate")
	}
}

func TestElection_HandleRequestVote_SameTerm_VoteOnce(t *testing.T) {
	cfg := testConfig("node1", map[string]string{})
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.currentTerm = 1
	node.mu.Unlock()

	req1 := &RequestVoteRequest{
		Term: 1, CandidateID: "node2",
		LastLogIndex: 0, LastLogTerm: 0,
	}
	resp1 := node.handleRequestVote(req1)
	if !resp1.VoteGranted {
		t.Error("should grant first vote")
	}

	req2 := &RequestVoteRequest{
		Term: 1, CandidateID: "node3",
		LastLogIndex: 0, LastLogTerm: 0,
	}
	resp2 := node.handleRequestVote(req2)
	if resp2.VoteGranted {
		t.Error("should not grant second vote in same term")
	}
}

func TestElection_StaleCandidate_HigherTermResponse(t *testing.T) {
	peers := map[string]string{
		"node2": "localhost:7002",
	}
	cfg := testConfig("node1", peers)
	transport := &mockTransport{
		requestVoteFn: func(ctx context.Context, peerID string,
			req *RequestVoteRequest) (*RequestVoteResponse, error) {
			return &RequestVoteResponse{
				Term:        req.Term + 10,
				VoteGranted: false,
			}, nil
		},
	}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	node.startElection()
	for i := 0; i < 10; i++ {
		if node.State() == Follower {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if node.State() != Follower {
		t.Errorf("should revert to follower on higher term response, got %s",
			node.State())
	}
}

func TestElection_ResetHeartbeat(t *testing.T) {
	cfg := testConfig("node1", map[string]string{})
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	before := node.lastHeartbeat
	time.Sleep(10 * time.Millisecond)
	node.resetHeartbeat()
	after := node.lastHeartbeat

	if !after.After(before) {
		t.Error("resetHeartbeat should update lastHeartbeat to a later time")
	}
}

func TestElection_HandleRequestVote_StaleLog_Rejected(t *testing.T) {
	cfg := testConfig("node1", map[string]string{})
	transport := &mockTransport{}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	// Node'un logunu daha güncel yap: term=2, index=10
	node.mu.Lock()
	node.currentTerm = 2
	node.mu.Unlock()

	for i := 0; i < 10; i++ {
		entry := LogEntry{
			Index: uint64(i + 1),
			Term:  2,
		}
		_ = node.log.Append(entry)
	}

	req := &RequestVoteRequest{
		Term:         3,
		CandidateID:  "node2",
		LastLogIndex: 5,
		LastLogTerm:  1,
	}

	resp := node.handleRequestVote(req)
	if resp.VoteGranted {
		t.Error("should reject candidate with stale log even if term is higher")
	}
}

// T1 doğrulama testi: Leader seçilince nextIndex ve matchIndex doğru init ediliyor mu?
func TestElection_LeaderState_InitializedOnElection(t *testing.T) {
	peers := map[string]string{
		"node1": "localhost:7001",
		"node2": "localhost:7002",
		"node3": "localhost:7003",
	}
	cfg := testConfig("node1", peers)
	transport := &mockTransport{
		requestVoteFn: func(ctx context.Context, peerID string,
			req *RequestVoteRequest) (*RequestVoteResponse, error) {
			return &RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
	}
	sm := &mockStateMachine{}

	node, err := NewRaftNode(cfg, sm, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	defer node.Stop()

	// Log'a 3 entry ekle — nextIndex bu değere göre ayarlanmalı
	_ = node.log.Append(
		LogEntry{Index: 1, Term: 1},
		LogEntry{Index: 2, Term: 1},
		LogEntry{Index: 3, Term: 1},
	)

	node.startElection()

	// Leader olana kadar bekle
	for i := 0; i < 20; i++ {
		if node.State() == Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if node.State() != Leader {
		t.Fatalf("node should be leader, got %s", node.State())
	}

	// Raft commits after a majority acknowledges the no-op, so commitIndex alone does
	// not prove that every follower has finished replication. Wait for each peer.
	const (
		expectedNextIndex  = uint64(5)
		expectedMatchIndex = uint64(4)
	)
	deadline := time.Now().Add(time.Second)
	for {
		allReplicated := true
		node.mu.RLock()
		for peerID := range peers {
			if peerID == node.config.NodeID {
				continue
			}
			if node.nextIndex[peerID] != expectedNextIndex || node.matchIndex[peerID] != expectedMatchIndex {
				allReplicated = false
				break
			}
		}
		node.mu.RUnlock()

		if allReplicated {
			break
		}
		if time.Now().After(deadline) {
			node.mu.RLock()
			statuses := make([]string, 0, len(peers)-1)
			for peerID := range peers {
				if peerID != node.config.NodeID {
					statuses = append(statuses, fmt.Sprintf("%s(nextIndex=%d, matchIndex=%d)",
						peerID, node.nextIndex[peerID], node.matchIndex[peerID]))
				}
			}
			node.mu.RUnlock()
			t.Fatalf("timed out waiting for follower replication: %s", strings.Join(statuses, ", "))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
