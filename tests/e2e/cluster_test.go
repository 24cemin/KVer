// Package e2e — uçtan uca cluster testleri.
// Gerçek binary'leri başlatarak tam cluster davranışını test eder.
//
// TODO: Hafta 8 — implement.
package e2e

import (
	"testing"
	"time"

	"github.com/emin/kver/internal/kvstore"
	"github.com/emin/kver/internal/raft"
	kvpb "github.com/emin/kver/proto/kv/gen"
	"google.golang.org/protobuf/proto"
)

// These scenarios are covered in tests/integration/multinode_test.go

func TestCluster_NodeRestart(t *testing.T) {
	dataDir := t.TempDir()
	
	// Create a single node
	kv := kvstore.NewKVStore()
	defer kv.Close()
	
	cfg := &raft.Config{
		NodeID:              "node1",
		Peers:               map[string]string{},
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		MaxLogEntriesPerRPC: 100,
		SnapshotThreshold:   1000,
		DataDir:             dataDir,
	}

	transport := raft.NewGRPCTransport(map[string]string{})
	node, err := raft.NewRaftNode(cfg, kv, transport)
	if err != nil {
		t.Fatalf("Failed to create RaftNode: %v", err)
	}
	
	// Wait for leader election
	time.Sleep(1000 * time.Millisecond)

	payload := &kvpb.CommandPayload{Op: "SET", Args: []string{"k1", "v1", "0"}}
	b, _ := proto.Marshal(payload)
	if err := node.Propose(b); err != nil {
		t.Fatalf("Failed to propose: %v", err)
	}

	// Wait for commit
	time.Sleep(100 * time.Millisecond)

	// Stop node
	node.Stop()
	kv.Close()

	// Restart node with same dataDir
	kv2 := kvstore.NewKVStore()
	defer kv2.Close()
	
	node2, err := raft.NewRaftNode(cfg, kv2, transport)
	if err != nil {
		t.Fatalf("Failed to restart RaftNode: %v", err)
	}
	defer node2.Stop()
	
	time.Sleep(1000 * time.Millisecond) // Let it recover and apply

	val, err := kv2.Get("k1")
	if err != nil || val != "v1" {
		t.Fatalf("WAL recovery failed, got val: %v, err: %v", val, err)
	}
}
