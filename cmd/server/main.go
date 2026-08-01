// cmd/server — Raft enabled KV store node'unu başlatır.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/emin/kver/internal/kvstore"
	"github.com/emin/kver/internal/raft"
	"github.com/emin/kver/internal/server"
	"github.com/emin/kver/pkg/sdk"
)

func main() {
	nodeID := flag.String("node-id", "", "Node ID (required)")
	addr := flag.String("addr", ":7001", "gRPC listen address")
	httpAddr := flag.String("http-addr", ":8001", "HTTP gateway listen address")
	peers := flag.String("peers", "",
		"Peer list: node2=localhost:7002,node3=localhost:7003")
	dataDir := flag.String("data-dir", "./data",
		"Data directory for WAL and snapshots")
	flag.Parse()

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "--node-id is required")
		os.Exit(1)
	}

	// Peers parse et
	peerMap := make(map[string]string)
	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) != 2 {
				log.Fatalf("invalid peer format: %s", p)
			}
			peerMap[parts[0]] = parts[1]
		}
	}

	// DataDir oluştur
	nodeDataDir := fmt.Sprintf("%s/%s", *dataDir, *nodeID)
	if err := os.MkdirAll(nodeDataDir, 0o755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	// KVStore oluştur
	kv := kvstore.NewKVStore()

	// Raft config
	cfg := &raft.Config{
		NodeID:               *nodeID,
		Peers:                peerMap,
		ElectionTimeoutMin:   150 * time.Millisecond,
		ElectionTimeoutMax:   300 * time.Millisecond,
		HeartbeatInterval:    50 * time.Millisecond,
		MaxLogEntriesPerRPC:  100,
		SnapshotThreshold:    1000,
		DataDir:              nodeDataDir,
		SyncWrites:           true,            // Production'da her zaman true olmalıdır (Raft Safety Guarantee)
		InitialElectionDelay: 1 * time.Second, // Docker bridge/gRPC kurulum süresi
	}

	// RaftNode oluştur
	transport := raft.NewGRPCTransport(peerMap)
	node, err := raft.NewRaftNode(cfg, kv, transport)
	if err != nil {
		log.Fatalf("failed to create raft node: %v", err)
	}

	// Server oluştur
	srvCfg := &server.ServerConfig{
		GRPCAddr: *addr,
		NodeID:   *nodeID,
		DataDir:  nodeDataDir,
	}
	srv := server.NewServer(srvCfg, node, kv)

	// Graceful shutdown
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT, syscall.SIGTERM,
	)
	defer cancel()

	// Gateway için SDK istemcisini başlat
	allAddrs := []string{*addr} // Kendi adresimiz
	for _, pAddr := range peerMap {
		allAddrs = append(allAddrs, pAddr)
	}
	sdkClient := sdk.NewClient(allAddrs)
	defer func() {
		if err := sdkClient.Close(); err != nil {
			log.Printf("failed to close SDK client: %v", err)
		}
	}()

	gateway := server.NewGateway(*httpAddr, sdkClient)

	// Gateway'i ayrı bir goroutine'de başlat
	go func() {
		if err := gateway.Start(ctx); err != nil {
			log.Printf("HTTP Gateway error: %v", err)
		}
	}()

	log.Printf("Starting node %s on %s", *nodeID, *addr)
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}

	node.Stop()
	kv.Close()
	log.Printf("Node %s stopped", *nodeID)
}
