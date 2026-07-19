// Package server implements the gRPC server that bridges KV operations to the Raft layer.
package server

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/emin/kver/internal/kvstore"
	"github.com/emin/kver/internal/raft"
	kvpb "github.com/emin/kver/proto/kv/gen"
	raftpb "github.com/emin/kver/proto/raft/gen"
)

// ServerConfig, sunucu yapılandırma parametrelerini tutar.
type ServerConfig struct {
	GRPCAddr string
	NodeID   string
	DataDir  string
}

// Server, gRPC sunucusunu ve alt handler'larını bir arada tutar.
type Server struct {
	grpcServer  *grpc.Server
	raftNode    *raft.RaftNode
	kvStore     *kvstore.KVStore
	kvHandler   *KVHandler
	raftHandler *RaftHandler
	config      *ServerConfig
}

// NewServer, yeni bir Server oluşturur.
// Gerçek RaftNode doğrudan KVHandler'a RaftProposer olarak bağlanır.
func NewServer(cfg *ServerConfig, raftNode *raft.RaftNode, kvStore *kvstore.KVStore) *Server {
	kvHandler := newKVHandler(raftNode, kvStore)
	raftHandler := newRaftHandler(raftNode)

	s := grpc.NewServer()
	kvpb.RegisterKVServiceServer(s, kvHandler)
	raftpb.RegisterRaftServiceServer(s, raftHandler)

	return &Server{
		grpcServer:  s,
		raftNode:    raftNode,
		kvStore:     kvStore,
		kvHandler:   kvHandler,
		raftHandler: raftHandler,
		config:      cfg,
	}
}

// Start, belirtilen adreste gRPC sunucusunu başlatır.
// Context kapandığında graceful shutdown yapar.
func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.config.GRPCAddr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("gRPC server listening on %s", s.config.GRPCAddr)
		if err := s.grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("Context cancelled, shutting down gRPC server gracefully...")
		s.grpcServer.GracefulStop()
		s.kvHandler.Close()
		return nil
	case err := <-errCh:
		return err
	}
}

// Stop, gRPC sunucusunu graceful şekilde durdurur.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.kvHandler.Close()
}
