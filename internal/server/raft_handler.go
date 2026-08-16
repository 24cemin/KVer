// Package server — raft_handler.go
// Raft RPC handler'ı: AppendEntries, RequestVote, InstallSnapshot isteklerini alır.
// Proto tiplerini internal Raft tiplerine dönüştürerek adapter pattern uygular.
package server

import (
	"context"
	"errors"

	"github.com/24cemin/KVer/internal/raft"
	raftpb "github.com/24cemin/KVer/proto/raft/gen"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RaftHandler, Raft gRPC servisini implement eder.
// Peer Raft node'ları bu handler üzerinden iletişim kurar.
type RaftHandler struct {
	raftpb.UnimplementedRaftServiceServer
	node *raft.RaftNode
}

// newRaftHandler, yeni bir RaftHandler oluşturur.
func newRaftHandler(node *raft.RaftNode) *RaftHandler {
	return &RaftHandler{node: node}
}

// AppendEntries, gelen AppendEntries RPC'yi proto → internal dönüşümüyle Raft node'una iletir.
func (h *RaftHandler) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	// proto → internal
	internalReq := &raft.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		LeaderCommit: req.LeaderCommit,
	}
	for _, e := range req.Entries {
		internalReq.Entries = append(internalReq.Entries, raft.LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Type:    raft.EntryType(e.Type),
			Command: e.Command,
		})
	}

	resp, err := h.node.HandleAppendEntriesWithError(internalReq)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// internal → proto
	return &raftpb.AppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

// RequestVote, gelen RequestVote RPC'yi proto → internal dönüşümüyle Raft node'una iletir.
func (h *RaftHandler) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	internalReq := &raft.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		PreVote:      req.PreVote,
	}

	resp := h.node.HandleRequestVote(internalReq)

	return &raftpb.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

// InstallSnapshot, gelen InstallSnapshot RPC'yi proto → internal dönüşümüyle Raft node'una iletir.
func (h *RaftHandler) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	internalReq := &raft.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Done:              req.Done,
		TotalSize:         req.TotalSize,
		Checksum:          req.Checksum,
		ClusterConfig:     req.ClusterConfig,
		Data:              req.Data,
	}

	resp, err := h.node.HandleInstallSnapshot(internalReq)
	if err != nil {
		return nil, installSnapshotStatusError(err)
	}

	return &raftpb.InstallSnapshotResponse{
		Term: resp.Term,
	}, nil
}

func installSnapshotStatusError(err error) error {
	switch {
	case errors.Is(err, raft.ErrInvalidSnapshot):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, raft.ErrSnapshotOffset),
		errors.Is(err, raft.ErrSnapshotBoundaryMismatch),
		errors.Is(err, raft.ErrSnapshotChunkMismatch):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, raft.ErrSnapshotChecksum),
		errors.Is(err, raft.ErrCorruptLog):
		return status.Error(codes.DataLoss, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
