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

func makeSingleNodeServer(t *testing.T, grpcAddr, httpAddr string) (*server.Server, *server.Gateway, func()) {
	nodeID := "test-node-1"
	dataDir := t.TempDir()

	kv := kvstore.NewKVStore()

	cfg := &raft.Config{
		NodeID:              nodeID,
		Peers:               make(map[string]string),
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		MaxLogEntriesPerRPC: 100,
		SnapshotThreshold:   1000,
		DataDir:             dataDir,
	}

	transport := raft.NewGRPCTransport(make(map[string]string))
	node, err := raft.NewRaftNode(cfg, kv, transport)
	if err != nil {
		t.Fatalf("failed to create raft node: %v", err)
	}

	srvCfg := &server.ServerConfig{
		GRPCAddr: grpcAddr,
		NodeID:   nodeID,
		DataDir:  dataDir,
	}
	srv := server.NewServer(srvCfg, node, kv)

	sdkClient := sdk.NewClient([]string{grpcAddr})
	gateway := server.NewGateway(httpAddr, sdkClient)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := gateway.Start(ctx); err != nil {
			fmt.Printf("gateway error: %v\n", err)
		}
	}()

	go func() {
		if err := srv.Start(ctx); err != nil {
			fmt.Printf("server error: %v\n", err)
		}
	}()

	// Lider seçilmesini bekle
	time.Sleep(1000 * time.Millisecond)

	cleanup := func() {
		cancel()
		if err := sdkClient.Close(); err != nil {
			t.Errorf("failed to close gateway SDK client: %v", err)
		}
		node.Stop()
		kv.Close()
		time.Sleep(100 * time.Millisecond) // Cleanup bekle
	}

	return srv, gateway, cleanup
}

func TestSDK_AllOperations(t *testing.T) {
	grpcAddr := "127.0.0.1:7901"
	httpAddr := "127.0.0.1:8901"

	_, _, cleanup := makeSingleNodeServer(t, grpcAddr, httpAddr)
	t.Cleanup(cleanup)

	// SDK Client oluştur
	client := sdk.NewClient([]string{grpcAddr})
	registerClientCleanup(t, client)

	// --- String ---
	err := client.Set("my_str", "hello", 0)
	if err != nil {
		t.Fatalf("SDK Set failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // Wait for Raft to apply

	val, err := client.Get("my_str")
	if err != nil || val != "hello" {
		t.Fatalf("SDK Get failed: val=%s err=%v", val, err)
	}

	_, err = client.Incr("my_cnt")
	if err != nil {
		t.Fatalf("SDK Incr failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// --- Hash ---
	err = client.HSet("my_hash", "f1", "v1")
	if err != nil {
		t.Fatalf("SDK HSet failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	hval, err := client.HGet("my_hash", "f1")
	if err != nil || hval != "v1" {
		t.Fatalf("SDK HGet failed: val=%s err=%v", hval, err)
	}

	// --- List ---
	_, err = client.LPush("my_list", "a", "b")
	if err != nil {
		t.Fatalf("SDK LPush failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	lval, err := client.LPop("my_list")
	time.Sleep(50 * time.Millisecond)
	// LPush(a,b) -> [b,a]. LPop -> b
	if err != nil || lval != "b" {
		t.Fatalf("SDK LPop failed: val=%s err=%v", lval, err)
	}

	// --- ZSet ---
	err = client.ZAdd("my_zset", 5.5, "mem1")
	if err != nil {
		t.Fatalf("SDK ZAdd failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	zvals, err := client.ZRange("my_zset", 0, -1)
	if err != nil || len(zvals) != 1 || zvals[0] != "mem1" {
		t.Fatalf("SDK ZRange failed: vals=%v err=%v", zvals, err)
	}
}
