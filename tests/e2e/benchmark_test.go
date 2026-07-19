package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/emin/kver/internal/kvstore"
	"github.com/emin/kver/internal/raft"
	"github.com/emin/kver/internal/server"
	"github.com/emin/kver/pkg/sdk"
)

func startSingleNodeE2E() (*server.Server, *sdk.Client, func()) {
	// Set up a single node gRPC server
	kv := kvstore.NewKVStore()
	cfg := &raft.Config{
		NodeID:              "bench_node",
		Peers:               map[string]string{},
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		MaxLogEntriesPerRPC: 100,
		SnapshotThreshold:   1000,
		DataDir:             "", // In-memory WAL
	}

	transport := raft.NewGRPCTransport(map[string]string{})
	node, _ := raft.NewRaftNode(cfg, kv, transport)

	grpcAddr := "127.0.0.1:7401"
	serverCfg := &server.ServerConfig{
		NodeID:   "bench_node",
		GRPCAddr: grpcAddr,
	}
	srv := server.NewServer(serverCfg, node, kv)
	go srv.Start(context.Background())
	
	time.Sleep(1000 * time.Millisecond) // Wait for leader
	client := sdk.NewClient([]string{grpcAddr})
	
	return srv, client, func() {
		client.Close()
		srv.Stop()
	}
}

func BenchmarkCluster_WriteLatency(b *testing.B) {
	_, client, cleanup := startSingleNodeE2E()
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = client.Set("bench_key", "val", 0)
	}
}

func BenchmarkCluster_ReadThroughput(b *testing.B) {
	_, client, cleanup := startSingleNodeE2E()
	defer cleanup()

	_ = client.Set("bench_key", "val", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = client.Get("bench_key")
	}
}
