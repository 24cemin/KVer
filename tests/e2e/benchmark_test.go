package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/emin/kver/internal/kvstore"
	"github.com/emin/kver/internal/raft"
	"github.com/emin/kver/internal/server"
	"github.com/emin/kver/pkg/sdk"
	"google.golang.org/grpc"
)

func startSingleNodeE2E(b *testing.B) (*server.Server, *sdk.Client) {
	b.Helper()
	// Set up a single node gRPC server
	kv := kvstore.NewKVStore()
	cfg := &raft.Config{
		NodeID:              "bench_node",
		Peers:               map[string]string{},
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		MaxLogEntriesPerRPC: 100,
		SnapshotThreshold:   1000,
		DataDir:             "", // In-memory WAL
	}

	transport := raft.NewGRPCTransport(map[string]string{})
	node, err := raft.NewRaftNode(cfg, kv, transport)
	if err != nil {
		b.Fatalf("failed to create Raft node: %v", err)
	}

	grpcAddr := "127.0.0.1:7401"
	serverCfg := &server.ServerConfig{
		NodeID:   "bench_node",
		GRPCAddr: grpcAddr,
	}
	srv := server.NewServer(serverCfg, node, kv)
	go func() {
		if err := srv.Start(context.Background()); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			b.Errorf("server failed: %v", err)
		}
	}()

	time.Sleep(1000 * time.Millisecond) // Wait for leader
	client := sdk.NewClient([]string{grpcAddr})

	b.Cleanup(func() {
		if err := client.Close(); err != nil {
			b.Errorf("failed to close SDK client: %v", err)
		}
		srv.Stop()
		node.Stop()
		kv.Close()
	})

	return srv, client
}

func BenchmarkCluster_WriteLatency(b *testing.B) {
	_, client := startSingleNodeE2E(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Set("bench_key", "val", 0); err != nil {
			b.Fatalf("Set failed: %v", err)
		}
	}
}

func BenchmarkCluster_ReadThroughput(b *testing.B) {
	_, client := startSingleNodeE2E(b)

	if err := client.Set("bench_key", "val", 0); err != nil {
		b.Fatalf("benchmark setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Get("bench_key"); err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}
