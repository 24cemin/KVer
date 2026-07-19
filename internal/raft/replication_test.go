package raft

import (
	"context"
	"testing"
	"time"
)

// mockTransportFull, AppendEntries davranışını da özelleştirilebilir yapıda tutar.
type mockTransportFull struct {
	requestVoteFn    func(ctx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	appendEntriesFn  func(ctx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	installSnapshotFn func(ctx context.Context, peerID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

func (m *mockTransportFull) RequestVote(ctx context.Context, peerID string,
	req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if m.requestVoteFn != nil {
		return m.requestVoteFn(ctx, peerID, req)
	}
	return &RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (m *mockTransportFull) AppendEntries(ctx context.Context, peerID string,
	req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if m.appendEntriesFn != nil {
		return m.appendEntriesFn(ctx, peerID, req)
	}
	return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (m *mockTransportFull) InstallSnapshot(ctx context.Context, peerID string,
	req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	if m.installSnapshotFn != nil {
		return m.installSnapshotFn(ctx, peerID, req)
	}
	return &InstallSnapshotResponse{Term: req.Term}, nil
}

func (m *mockTransportFull) AddPeer(nodeID, address string) {}
func (m *mockTransportFull) RemovePeer(nodeID string)       {}

// newLeaderNode, 3-node cluster'da leader olarak ayarlanmış bir RaftNode döner.
func newLeaderNode(t *testing.T, peers map[string]string, transport Transport) *RaftNode {
	t.Helper()
	cfg := &Config{
		NodeID:              "node1",
		Peers:               peers,
		HeartbeatInterval: 50 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		MaxLogEntriesPerRPC: 100,
	}
	clusterConfig := &ClusterConfig{Peers: make(map[string]string)}
	for k, v := range peers {
		clusterConfig.Peers[k] = v
	}
	clusterConfig.Peers[cfg.NodeID] = ""

	r := &RaftNode{
		config:        cfg,
		clusterConfig: clusterConfig,
		state:         Leader,
		currentTerm:   1,
		log:           newRaftLog(),
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		stopCh:        make(chan struct{}),
		applyCh:       make(chan struct{}, 1),
		proposeCh:     make(chan *proposeRequest, 1000),
		waiters:       make(map[uint64]chan error),
		transport:     transport,
	}
	// nextIndex'i 1'den başlat
	for peerID := range peers {
		if peerID != "node1" {
			r.nextIndex[peerID] = 1
		}
	}

	r.wg.Add(1)
	go r.batchLoop()

	return r
}

func TestReplication_LeaderReplicatesEntry(t *testing.T) {
	t.Run("LeaderAppendsAndReplicates", func(t *testing.T) {
		peers := map[string]string{
			"node1": "localhost:7001",
			"node2": "localhost:7002",
			"node3": "localhost:7003",
		}
		transport := &mockTransportFull{
			appendEntriesFn: func(ctx context.Context, peerID string,
				req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
				return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
			},
		}
		r := newLeaderNode(t, peers, transport)

		go func() { _ = r.Propose([]byte("set foo bar")) }()
		// Batch loop'un işlemesi için bekle
		time.Sleep(10 * time.Millisecond)

		if r.log.LastIndex() != 1 {
			t.Errorf("expected LastIndex 1, got %d", r.log.LastIndex())
		}
	})
}

func TestReplication_CommitAfterMajority(t *testing.T) {
	t.Run("CommitIndexAdvanced", func(t *testing.T) {
		peers := map[string]string{
			"node1": "localhost:7001",
			"node2": "localhost:7002",
			"node3": "localhost:7003",
		}
		transport := &mockTransportFull{
			appendEntriesFn: func(ctx context.Context, peerID string,
				req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
				return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
			},
		}
		r := newLeaderNode(t, peers, transport)

		go func() { _ = r.Propose([]byte("set key val")) }()
		// Batch loop'un işlemesi için bekle
		time.Sleep(10 * time.Millisecond)

		r.mu.RLock()
		ci := r.commitIndex
		r.mu.RUnlock()

		if ci == 0 {
			t.Errorf("expected commitIndex > 0 after majority, got 0")
		}
	})
}

func TestReplication_ConflictResolution(t *testing.T) {
	t.Run("PrevLogTermMismatch_ReturnsFalseWithConflict", func(t *testing.T) {
		r := &RaftNode{
			currentTerm: 1,
			state:       Follower,
			log:         newRaftLog(),
		}
		// Log'a term 1 ile 3 entry ekle
		_ = r.log.Append(
			LogEntry{Index: 1, Term: 1},
			LogEntry{Index: 2, Term: 1},
			LogEntry{Index: 3, Term: 1},
		)

		// PrevLogIndex=2, PrevLogTerm=2 gönder → term 1 ≠ 2 → conflict
		req := &AppendEntriesRequest{
			Term:         2,
			LeaderID:     "node2",
			PrevLogIndex: 2,
			PrevLogTerm:  2, // log'da term 1 var
			Entries:      []LogEntry{{Index: 3, Term: 2}},
		}

		resp := r.handleAppendEntries(req)

		if resp.Success {
			t.Errorf("expected success=false on term mismatch")
		}
		if resp.ConflictIndex == 0 {
			t.Errorf("expected non-zero ConflictIndex, got 0")
		}
	})
}

func TestReplication_HeartbeatMaintainsAuthority(t *testing.T) {
	t.Run("HeartbeatResetsTimer", func(t *testing.T) {
		r := &RaftNode{
			state:       Leader,
			currentTerm: 1,
			config:      &Config{NodeID: "node1"},
		}
		r.lastHeartbeat = time.Now().Add(-1 * time.Hour)
		oldHeartbeat := r.lastHeartbeat

		req := &AppendEntriesRequest{
			Term:     1,
			LeaderID: "node2",
		}

		resp := r.handleAppendEntries(req)
		if !resp.Success {
			t.Errorf("expected success=true, got %v", resp.Success)
		}
		if r.lastHeartbeat.Before(oldHeartbeat) || r.lastHeartbeat.Equal(oldHeartbeat) {
			t.Errorf("lastHeartbeat not updated")
		}
		if r.state != Follower {
			t.Errorf("expected state to be Follower, got %s", r.state)
		}
	})
}

func TestReplication_HandleAppendEntries(t *testing.T) {
	t.Run("OldTerm_Rejected", func(t *testing.T) {
		r := &RaftNode{
			currentTerm: 5,
			state:       Follower,
			log:         newRaftLog(),
		}
		req := &AppendEntriesRequest{
			Term: 3,
		}
		resp := r.handleAppendEntries(req)
		if resp.Success {
			t.Errorf("expected rejected for old term")
		}
		if resp.Term != 5 {
			t.Errorf("expected term 5, got %d", resp.Term)
		}
		if r.currentTerm != 5 {
			t.Errorf("currentTerm should not change")
		}
	})

	t.Run("HigherTerm_AcceptedAndStepDown", func(t *testing.T) {
		r := &RaftNode{
			currentTerm: 2,
			state:       Candidate,
			log:         newRaftLog(),
		}
		req := &AppendEntriesRequest{
			Term: 5,
		}
		resp := r.handleAppendEntries(req)
		if !resp.Success {
			t.Errorf("expected success for higher term")
		}
		if r.state != Follower {
			t.Errorf("node should step down to Follower")
		}
		if r.currentTerm != 5 {
			t.Errorf("node should update currentTerm to 5, got %d", r.currentTerm)
		}
	})

	t.Run("SameTerm_Follower_Accepted", func(t *testing.T) {
		r := &RaftNode{
			currentTerm: 3,
			state:       Follower,
			log:         newRaftLog(),
		}
		req := &AppendEntriesRequest{
			Term: 3,
		}
		resp := r.handleAppendEntries(req)
		if !resp.Success {
			t.Errorf("expected success for same term")
		}
		if r.state != Follower {
			t.Errorf("node should remain Follower")
		}
	})
}

func TestReplication_HandleAppendEntries_WithEntries(t *testing.T) {
	t.Run("EntriesAreSavedAndCommitIndexUpdated", func(t *testing.T) {
		r := &RaftNode{
			currentTerm: 1,
			state:       Follower,
			log:         newRaftLog(),
		}

		req := &AppendEntriesRequest{
			Term:         1,
			LeaderID:     "node1",
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries: []LogEntry{
				{Index: 1, Term: 1, Command: []byte("cmd1")},
				{Index: 2, Term: 1, Command: []byte("cmd2")},
			},
			LeaderCommit: 2,
		}

		resp := r.handleAppendEntries(req)
		if !resp.Success {
			t.Fatalf("expected success=true, got false")
		}
		if r.log.LastIndex() != 2 {
			t.Errorf("expected LastIndex 2, got %d", r.log.LastIndex())
		}
		if r.commitIndex != 2 {
			t.Errorf("expected commitIndex 2, got %d", r.commitIndex)
		}
	})
}

func TestReplication_HandleAppendEntries_ConflictResolution(t *testing.T) {
	t.Run("ConflictReturnsCorrectIndex", func(t *testing.T) {
		r := &RaftNode{
			currentTerm: 1,
			state:       Follower,
			log:         newRaftLog(),
		}
		_ = r.log.Append(
			LogEntry{Index: 1, Term: 1},
			LogEntry{Index: 2, Term: 1},
			LogEntry{Index: 3, Term: 1},
		)

		req := &AppendEntriesRequest{
			Term:         2,
			LeaderID:     "node2",
			PrevLogIndex: 2,
			PrevLogTerm:  2, // log'da term 1 var → conflict
		}

		resp := r.handleAppendEntries(req)
		if resp.Success {
			t.Errorf("expected success=false on conflict")
		}
		if resp.ConflictIndex == 0 {
			t.Errorf("expected non-zero ConflictIndex")
		}
		if resp.ConflictTerm != 1 {
			t.Errorf("expected ConflictTerm=1, got %d", resp.ConflictTerm)
		}
	})
}

func TestReplication_ReplicateTo_Success(t *testing.T) {
	t.Run("NextIndexAndMatchIndexUpdated", func(t *testing.T) {
		peers := map[string]string{
			"node1": "localhost:7001",
			"node2": "localhost:7002",
		}
		transport := &mockTransportFull{
			appendEntriesFn: func(ctx context.Context, peerID string,
				req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
				return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
			},
		}
		r := newLeaderNode(t, peers, transport)

		// 2 entry ekle
		_ = r.log.Append(
			LogEntry{Index: 1, Term: 1, Command: []byte("a")},
			LogEntry{Index: 2, Term: 1, Command: []byte("b")},
		)

		ctx := context.Background()
		err := r.replicateTo(ctx, "node2", 1)
		if err != nil {
			t.Fatalf("replicateTo failed: %v", err)
		}

		r.mu.RLock()
		ni := r.nextIndex["node2"]
		mi := r.matchIndex["node2"]
		r.mu.RUnlock()

		if ni != 3 {
			t.Errorf("expected nextIndex[node2]=3, got %d", ni)
		}
		if mi != 2 {
			t.Errorf("expected matchIndex[node2]=2, got %d", mi)
		}
	})
}
func newTestRaftNode(t *testing.T, nodeID string, peers map[string]string) *RaftNode {
	t.Helper()
	cfg := &Config{
		NodeID:              nodeID,
		Peers:               peers,
		HeartbeatInterval: 50 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		MaxLogEntriesPerRPC: 100,
	}
	clusterConfig := &ClusterConfig{Peers: make(map[string]string)}
	for k, v := range peers {
		clusterConfig.Peers[k] = v
	}
	clusterConfig.Peers[nodeID] = ""

	return &RaftNode{
		config:        cfg,
		clusterConfig: clusterConfig,
		state:         Follower,
		currentTerm:   1,
		log:           newRaftLog(),
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		stopCh:        make(chan struct{}),
		transport:     &mockTransportFull{},
	}
}

func becomeLeader(t *testing.T, r *RaftNode) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = Leader
	r.currentTerm = 1
	r.noopCommitted = true
}

// ─── ReadIndex Tests ───────────────────────────────────────────────────────────

func TestReadIndex_SingleNode_Success(t *testing.T) {
	t.Run("SingleNodeLeader_ReturnsImmediately", func(t *testing.T) {
		r := newTestRaftNode(t, "node1", map[string]string{})
		becomeLeader(t, r)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		idx, err := r.ReadIndex(ctx)
		if err != nil {
			t.Fatalf("ReadIndex failed on single-node leader: %v", err)
		}
		// Single node: commitIndex == lastApplied == 0 initially
		_ = idx
	})
}

func TestReadIndex_NotLeader_Error(t *testing.T) {
	t.Run("Follower_ReturnsErrNotLeader", func(t *testing.T) {
		r := newTestRaftNode(t, "node1", map[string]string{"node2": "addr2"})
		// node1 starts as follower by default

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		_, err := r.ReadIndex(ctx)
		if err != ErrNotLeader {
			t.Fatalf("expected ErrNotLeader, got %v", err)
		}
	})
}

func TestLeaderAddr_ReturnsEmptyWhenLeader(t *testing.T) {
	t.Run("Leader_ReturnsEmpty", func(t *testing.T) {
		r := newTestRaftNode(t, "node1", map[string]string{})
		becomeLeader(t, r)

		addr := r.LeaderAddr()
		if addr != "" {
			t.Errorf("expected empty LeaderAddr when node is leader, got %q", addr)
		}
	})
}

func TestLeaderAddr_ReturnsEmptyWhenUnknown(t *testing.T) {
	t.Run("Follower_NoLeaderKnown", func(t *testing.T) {
		r := newTestRaftNode(t, "node1", map[string]string{"node2": "localhost:7002"})
		// leaderID is empty by default

		addr := r.LeaderAddr()
		if addr != "" {
			t.Errorf("expected empty LeaderAddr when leader unknown, got %q", addr)
		}
	})
}

func TestLeaderAddr_ReturnsAddrAfterAppendEntries(t *testing.T) {
	t.Run("Follower_LeaderKnownFromHeartbeat", func(t *testing.T) {
		r := newTestRaftNode(t, "node1", map[string]string{
			"node2": "localhost:7002",
		})
		// Simulate receiving heartbeat from node2 (the leader)
		resp := r.HandleAppendEntries(&AppendEntriesRequest{
			Term:     1,
			LeaderID: "node2",
		})
		if !resp.Success {
			t.Fatalf("expected heartbeat to succeed, got failure")
		}

		addr := r.LeaderAddr()
		if addr != "localhost:7002" {
			t.Errorf("expected leader addr 'localhost:7002', got %q", addr)
		}
	})
}
