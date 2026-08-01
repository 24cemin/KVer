package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/emin/kver/internal/kvstore"
	"github.com/emin/kver/internal/raft"
	"github.com/emin/kver/internal/server"
	"github.com/emin/kver/pkg/sdk"
)

func makeThreeNodeCluster(t *testing.T, basePort int) ([]*server.Server, []*raft.RaftNode, func()) {
	t.Helper()
	var servers []*server.Server
	var nodes []*raft.RaftNode
	var kvs []*kvstore.KVStore

	peers := map[string]string{
		"node1": fmt.Sprintf("127.0.0.1:%d", basePort),
		"node2": fmt.Sprintf("127.0.0.1:%d", basePort+1),
		"node3": fmt.Sprintf("127.0.0.1:%d", basePort+2),
	}

	for i := 1; i <= 3; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		grpcAddr := fmt.Sprintf(":%d", basePort+i-1)
		dataDir := t.TempDir()

		kv := kvstore.NewKVStore()
		kvs = append(kvs, kv)

		cfg := &raft.Config{
			NodeID:              nodeID,
			Peers:               peers,
			ElectionTimeoutMin:  150 * time.Millisecond,
			ElectionTimeoutMax:  300 * time.Millisecond,
			HeartbeatInterval:   50 * time.Millisecond,
			MaxLogEntriesPerRPC: 100,
			SnapshotThreshold:   1000,
			DataDir:             dataDir,
		}

		transport := raft.NewGRPCTransport(peers)
		node, err := raft.NewRaftNode(cfg, kv, transport)
		if err != nil {
			t.Fatalf("failed to create raft node: %v", err)
		}
		nodes = append(nodes, node)

		srvCfg := &server.ServerConfig{
			GRPCAddr: grpcAddr,
			NodeID:   nodeID,
			DataDir:  dataDir,
		}
		srv := server.NewServer(srvCfg, node, kv)
		servers = append(servers, srv)
	}

	ctx, cancel := context.WithCancel(context.Background())
	for _, srv := range servers {
		go func(s *server.Server) {
			_ = s.Start(ctx)
		}(srv)
	}

	time.Sleep(1000 * time.Millisecond) // Let cluster form

	cleanup := func() {
		cancel()
		for _, node := range nodes {
			node.Stop()
		}
		for _, kv := range kvs {
			kv.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}

	return servers, nodes, cleanup
}

func TestMultiNode_ThreeNodeCluster_SetGet(t *testing.T) {
	// Ports 7201-7203 reserved for this test
	_, _, cleanup := makeThreeNodeCluster(t, 7201)
	t.Cleanup(cleanup)

	client := sdk.NewClient([]string{"127.0.0.1:7201", "127.0.0.1:7202", "127.0.0.1:7203"})
	registerClientCleanup(t, client)

	if err := client.Set("cluster_key", "cluster_val", 0); err != nil {
		t.Fatalf("SDK Set failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	val, err := client.Get("cluster_key")
	if err != nil || val != "cluster_val" {
		t.Fatalf("SDK Get failed: val=%s err=%v", val, err)
	}
}

func TestMultiNode_SDK_LeaderDiscovery(t *testing.T) {
	// Ports 7211-7213 reserved for this test
	_, nodes, cleanup := makeThreeNodeCluster(t, 7211)
	t.Cleanup(cleanup)

	var followerAddr string
	var otherAddrs []string
	for i, n := range nodes {
		addr := fmt.Sprintf("127.0.0.1:%d", 7211+i)
		if n.State() != raft.Leader && followerAddr == "" {
			followerAddr = addr
		} else {
			otherAddrs = append(otherAddrs, addr)
		}
	}

	addrs := append([]string{followerAddr}, otherAddrs...)
	client := sdk.NewClient(addrs)
	registerClientCleanup(t, client)

	if err := client.Set("discover", "success", 0); err != nil {
		t.Fatalf("Set to follower failed (should discover leader): %v", err)
	}

	time.Sleep(1000 * time.Millisecond)
	val, err := client.Get("discover")
	if err != nil || val != "success" {
		t.Fatalf("Get failed: %v", err)
	}
}

func TestMultiNode_DataConsistency(t *testing.T) {
	// Ports 7221-7223 reserved for this test
	_, _, cleanup := makeThreeNodeCluster(t, 7221)
	t.Cleanup(cleanup)

	client := sdk.NewClient([]string{"127.0.0.1:7221", "127.0.0.1:7222", "127.0.0.1:7223"})
	registerClientCleanup(t, client)

	for i := 0; i < 10; i++ {
		if err := client.Set(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 0); err != nil {
			t.Fatalf("Set %d failed: %v", i, err)
		}
	}
	time.Sleep(1000 * time.Millisecond)

	for i := 0; i < 10; i++ {
		v, _ := client.Get(fmt.Sprintf("k%d", i))
		if v != fmt.Sprintf("v%d", i) {
			t.Fatalf("mismatch at %d", i)
		}
	}
}
