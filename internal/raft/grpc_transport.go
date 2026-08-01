// Package raft — grpc_transport.go
// GRPCTransport, peer'lara gerçek gRPC çağrısı yapan Transport implementasyonudur.
// Connection cache ile her peer'a tek bağlantı açılır ve yeniden kullanılır.
package raft

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	raftpb "github.com/emin/kver/proto/raft/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport, Raft peer'larına gRPC üzerinden RPC gönderir.
type GRPCTransport struct {
	peers map[string]string // nodeID → address
	mu    sync.RWMutex

	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn // address → cached connection
}

// NewGRPCTransport, peer adresleriyle yeni bir GRPCTransport oluşturur.
func NewGRPCTransport(peers map[string]string) *GRPCTransport {
	cp := make(map[string]string, len(peers))
	for k, v := range peers {
		cp[k] = v
	}
	return &GRPCTransport{
		peers: cp,
		conns: make(map[string]*grpc.ClientConn),
	}
}

// getConn, belirtilen adres için cached gRPC bağlantısı döndürür.
// Bağlantı yoksa yeni bir tane açar.
func (t *GRPCTransport) getConn(addr string) (*grpc.ClientConn, error) {
	t.connMu.Lock()
	defer t.connMu.Unlock()

	if conn, ok := t.conns[addr]; ok {
		return conn, nil
	}

	backoffCfg := backoff.DefaultConfig
	backoffCfg.MaxDelay = 50 * time.Millisecond

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoffCfg,
			MinConnectTimeout: 500 * time.Millisecond,
		}),
	)
	if err != nil {
		return nil, err
	}
	t.conns[addr] = conn
	return conn, nil
}

// Close, tüm cached bağlantıları kapatır.
func (t *GRPCTransport) Close() {
	t.connMu.Lock()
	defer t.connMu.Unlock()

	for addr, conn := range t.conns {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close Raft connection %s: %v", addr, err)
		}
		delete(t.conns, addr)
	}
}

// RequestVote, belirtilen peer'a RequestVote RPC gönderir.
func (t *GRPCTransport) RequestVote(ctx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	t.mu.RLock()
	addr, ok := t.peers[peerID]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown peer: %s", peerID)
	}

	conn, err := t.getConn(addr)
	if err != nil {
		return nil, err
	}

	client := raftpb.NewRaftServiceClient(conn)
	resp, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		PreVote:      req.PreVote,
	})
	if err != nil {
		return nil, err
	}

	return &RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

// AppendEntries, belirtilen peer'a AppendEntries RPC gönderir.
func (t *GRPCTransport) AppendEntries(ctx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	t.mu.RLock()
	addr, ok := t.peers[peerID]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown peer: %s", peerID)
	}

	conn, err := t.getConn(addr)
	if err != nil {
		return nil, err
	}

	// internal → proto: entries dönüşümü
	pbEntries := make([]*raftpb.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		pbEntries[i] = &raftpb.LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Type:    raftpb.EntryType(e.Type),
			Command: e.Command,
		}
	}

	client := raftpb.NewRaftServiceClient(conn)
	resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}

	return &AppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

// InstallSnapshot, belirtilen peer'a InstallSnapshot RPC gönderir.
func (t *GRPCTransport) InstallSnapshot(ctx context.Context, peerID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	t.mu.RLock()
	addr, ok := t.peers[peerID]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown peer: %s", peerID)
	}

	conn, err := t.getConn(addr)
	if err != nil {
		return nil, err
	}

	client := raftpb.NewRaftServiceClient(conn)
	resp, err := client.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          req.LeaderID,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	if err != nil {
		return nil, err
	}

	return &InstallSnapshotResponse{
		Term: resp.Term,
	}, nil
}

// AddPeer, transport peer haritasina yeni bir node ekler (thread-safe).
func (t *GRPCTransport) AddPeer(nodeID, address string) {
	t.mu.Lock()
	t.peers[nodeID] = address
	t.mu.Unlock()
}

// RemovePeer, transport peer haritasindan bir node'u cikarir (thread-safe).
// Onbellek baglantisi da kapatilir.
func (t *GRPCTransport) RemovePeer(nodeID string) {
	t.mu.Lock()
	addr, ok := t.peers[nodeID]
	delete(t.peers, nodeID)
	t.mu.Unlock()

	if ok {
		t.connMu.Lock()
		if conn, exists := t.conns[addr]; exists {
			if err := conn.Close(); err != nil {
				log.Printf("failed to close removed peer %s connection %s: %v", nodeID, addr, err)
			}
			delete(t.conns, addr)
		}
		t.connMu.Unlock()
	}
}

// compile-time check
var _ Transport = (*GRPCTransport)(nil)
