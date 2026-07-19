// Package raft — transport.go
// Transport katmanı, Raft peer'ları arasındaki ağ iletişimini soyutlar.
// election.go ve replication.go bu interface'i kullanır; gRPC detaylarından habersizdir.
package raft

import "context"

// AppendEntriesRequest, Raft AppendEntries RPC'nin istek yapısıdır.
// Proto'dan gelen mesaj bu yapıya dönüştürülür (katman ayrımı için).
//
// (Proto tanımıyla sekronizedir)
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesResponse, AppendEntries RPC'nin yanıt yapısıdır.
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	// ConflictIndex, başarısız durumda follower'ın conflict noktasını döndürür.
	ConflictIndex uint64
	ConflictTerm  uint64
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

// RequestVoteResponse, RequestVote RPC'nin yanıt yapısıdır.
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// InstallSnapshotRequest, InstallSnapshot RPC'nin istek yapısıdır.
type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotResponse, InstallSnapshot RPC'nin yanıt yapısıdır.
type InstallSnapshotResponse struct {
	Term uint64
}

// Transport, Raft node'larının birbirine RPC göndermesi için kullandığı interface'dir.
// Bu sayede election.go ve replication.go ağ katmanını doğrudan çağırmaz.
type Transport interface {
	// AppendEntries, belirtilen peer'a AppendEntries RPC gönderir.
	AppendEntries(ctx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)

	// RequestVote, belirtilen peer'a RequestVote RPC gönderir.
	RequestVote(ctx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error)

	// InstallSnapshot, belirtilen peer'a snapshot gönderir.
	InstallSnapshot(ctx context.Context, peerID string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)

	// AddPeer, transport'a yeni bir peer ekler (dynamic membership).
	// applyMembership tarafından commit anında çağrılır.
	AddPeer(nodeID, address string)

	// RemovePeer, transport'tan bir peer'i kaldırır (dynamic membership).
	RemovePeer(nodeID string)
}
