// Package integration — Raft entegrasyon testleri.
// In-process cluster: gerçek RaftNode'lar, localTransport ile ağ simülasyonu.
// Gerçek gRPC bağlantısı yok — test hızı ve izolasyon için.
package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/emin/kver/internal/raft"
)

// ─── In-process Transport ────────────────────────────────────────────────────

// localTransport, aynı process içindeki RaftNode'lar arasında
// direkt metod çağrısı ile iletişim sağlar (ağ simülasyonu).
type localTransport struct {
	mu       sync.RWMutex
	peers    map[string]*raft.RaftNode // nodeID → node
	isolated map[string]bool
}

func newLocalTransport() *localTransport {
	return &localTransport{peers: make(map[string]*raft.RaftNode), isolated: make(map[string]bool)}
}

func (t *localTransport) register(nodeID string, node *raft.RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[nodeID] = node
}

func (t *localTransport) isolate(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolated[nodeID] = true
}

func (t *localTransport) heal(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolated[nodeID] = false
}

func (t *localTransport) RequestVote(_ context.Context, peerID string,
	req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	t.mu.RLock()
	node, ok := t.peers[peerID]
	isIsolated := t.isolated[peerID] || t.isolated[req.CandidateID]
	t.mu.RUnlock()
	if !ok || isIsolated {
		return nil, context.DeadlineExceeded
	}
	return node.HandleRequestVote(req), nil
}

func (t *localTransport) AppendEntries(_ context.Context, peerID string,
	req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	t.mu.RLock()
	node, ok := t.peers[peerID]
	isIsolated := t.isolated[peerID] || t.isolated[req.LeaderID]
	t.mu.RUnlock()
	if !ok || isIsolated {
		return nil, context.DeadlineExceeded
	}
	return node.HandleAppendEntries(req), nil
}

func (t *localTransport) InstallSnapshot(_ context.Context, peerID string,
	req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	t.mu.RLock()
	node, ok := t.peers[peerID]
	isIsolated := t.isolated[peerID] || t.isolated[req.LeaderID]
	t.mu.RUnlock()
	if !ok || isIsolated {
		return nil, context.DeadlineExceeded
	}
	return node.HandleInstallSnapshot(req), nil
}

func (t *localTransport) AddPeer(nodeID, address string) {}
func (t *localTransport) RemovePeer(nodeID string)       {}

// ─── Cluster helpers ─────────────────────────────────────────────────────────

type mockSM struct{}

func (m *mockSM) Apply(_ raft.LogEntry) error { return nil }
func (m *mockSM) Snapshot() ([]byte, error)   { return nil, nil }
func (m *mockSM) Restore(_ []byte) error      { return nil }

// makeCluster, n node'lu bir in-process Raft cluster'ı kurar.
// Tüm node'lar aynı localTransport üzerinden birbirini görebilir.
func makeCluster(t *testing.T, n int) ([]*raft.RaftNode, *localTransport, func()) {
	t.Helper()

	peers := make(map[string]string, n)
	for i := 0; i < n; i++ {
		id := nodeID(i)
		peers[id] = "local://" + id // addr sadece peer map için gerekli
	}

	transport := newLocalTransport()
	nodes := make([]*raft.RaftNode, n)

	for i := 0; i < n; i++ {
		id := nodeID(i)

		// Build peers map excluding self
		nodePeers := make(map[string]string)
		for j := 0; j < n; j++ {
			if i != j {
				pID := nodeID(j)
				nodePeers[pID] = "local://" + pID
			}
		}

		cfg := &raft.Config{
			NodeID:              id,
			Peers:               nodePeers,
			HeartbeatInterval: 20 * time.Millisecond,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			MaxLogEntriesPerRPC: 100,
		}
		node, err := raft.NewRaftNode(cfg, &mockSM{}, transport)
		if err != nil {
			t.Fatalf("NewRaftNode(%s) failed: %v", id, err)
		}
		nodes[i] = node
		transport.register(id, node)
	}

	cleanup := func() {
		for _, n := range nodes {
			n.Stop()
		}
	}
	return nodes, transport, cleanup
}

func nodeID(i int) string {
	return "node" + string(rune('1'+i))
}

// waitForLeader, cluster içinde bir leader seçilene kadar bekler.
func waitForLeader(t *testing.T, nodes []*raft.RaftNode, timeout time.Duration) *raft.RaftNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.State() == raft.Leader {
				return n
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// ─── Tests ───────────────────────────────────────────────────────────────────

func TestRaft_ThreeNodeElection(t *testing.T) {
	nodes, _, cleanup := makeCluster(t, 3)
	defer cleanup()

	leader := waitForLeader(t, nodes, 3*time.Second)

	// Tek bir leader olmalı
	leaderCount := 0
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected 1 leader, got %d", leaderCount)
	}
	_ = leader

	// Follower'lar leader'ın term'ini benimsemeli
	leaderTerm := leader.CurrentTerm()
	for _, n := range nodes {
		if n.CurrentTerm() != leaderTerm {
			t.Errorf("node term mismatch: leader=%d node=%d", leaderTerm, n.CurrentTerm())
		}
	}
}

func TestRaft_LogReplication(t *testing.T) {
	nodes, _, cleanup := makeCluster(t, 3)
	defer cleanup()

	leader := waitForLeader(t, nodes, 3*time.Second)

	// Leader'a 3 komut propose et
	cmds := []string{"set foo bar", "set baz qux", "incr counter"}
	for _, cmd := range cmds {
		if err := leader.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose failed: %v", err)
		}
	}

	// Log replication asenkron — follower'ların kopyalaması için bekle
	time.Sleep(1000 * time.Millisecond)

	// Leader log'unda 3 entry olmalı
	lastIdx := leader.LastLogIndex()
	if lastIdx < 3 {
		t.Errorf("expected at least 3 log entries on leader, got %d", lastIdx)
	}

	// commitIndex ilerlemeli
	commitIdx := leader.CommitIndex()
	if commitIdx == 0 {
		t.Errorf("expected commitIndex > 0 after majority replication")
	}
}

func TestRaft_LeaderFailover(t *testing.T) {
	nodes, _, cleanup := makeCluster(t, 3)
	defer cleanup()

	leader := waitForLeader(t, nodes, 3*time.Second)

	// Stop leader
	leader.Stop()

	// Wait for new leader
	var remaining []*raft.RaftNode
	for _, n := range nodes {
		if n != leader {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 3*time.Second)
	if newLeader == nil {
		t.Fatalf("Failed to elect new leader")
	}
	if newLeader == leader {
		t.Fatalf("Old leader cannot be new leader")
	}
}

func TestRaft_NetworkPartition(t *testing.T) {
	nodes, transport, cleanup := makeCluster(t, 3)
	defer cleanup()

	leader := waitForLeader(t, nodes, 3*time.Second)

	// Simüle partition: leader'ı izole et
	var leaderID string
	for i, n := range nodes {
		if n == leader {
			leaderID = nodeID(i)
			break
		}
	}
	transport.isolate(leaderID)

	// Wait for new leader among remaining 2 nodes
	var remaining []*raft.RaftNode
	for _, n := range nodes {
		if n != leader {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 3*time.Second)
	if newLeader == nil {
		t.Fatalf("Failed to elect new leader after partition")
	}

	// Heal partition
	transport.heal(leaderID)

	// Eski leader (şu anki term'den geri kaldığı için) heartbeat alınca stepDown yapmalı
	time.Sleep(1000 * time.Millisecond)

	if leader.State() == raft.Leader {
		t.Fatalf("Old leader did not step down after partition healed")
	}
}

func TestRaft_MembershipChange_E2E(t *testing.T) {
	transport := newLocalTransport()

	cfg1 := &raft.Config{
		NodeID:             "node1",
		Peers:              map[string]string{}, // Single node initially
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
	}
	sm1 := &mockSM{}
	node1, err := raft.NewRaftNode(cfg1, sm1, transport)
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}
	defer node1.Stop()
	transport.register("node1", node1)

	// node1 leader olsun
	time.Sleep(1000 * time.Millisecond)
	if node1.State() != raft.Leader {
		t.Fatalf("node1 failed to become leader")
	}

	// node2'yi oluştur
	cfg2 := &raft.Config{
		NodeID:             "node2",
		Peers:              map[string]string{}, // Boş başlatıyoruz
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
	}
	sm2 := &mockSM{}
	node2, err := raft.NewRaftNode(cfg2, sm2, transport)
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}
	defer node2.Stop()
	transport.register("node2", node2)

	// node1'e node2'yi ekle
	change := raft.MembershipChange{
		Type:    raft.AddServer,
		NodeID:  "node2",
		Address: "127.0.0.1:7002",
	}

	err = node1.ProposeMembershipChange(change)
	if err != nil {
		t.Fatalf("ProposeMembershipChange failed: %v", err)
	}

	// Dynamic membership'in işlenmesi ve replikasyon için bekle
	time.Sleep(1000 * time.Millisecond)

	// node1 config boyutu 2 olmalı
	peers1 := node1.Peers()
	if len(peers1) != 2 {
		t.Errorf("expected node1 to have 2 peers, got %d: %v", len(peers1), peers1)
	}

	// node2'ye replikasyon gitmiş olmalı
	if _, ok := peers1["node2"]; !ok {
		t.Errorf("node2 should be present in node1's peers")
	}
}
