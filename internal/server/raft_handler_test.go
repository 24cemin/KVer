package server

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/24cemin/KVer/internal/raft"
	raftpb "github.com/24cemin/KVer/proto/raft/gen"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type raftHandlerStateMachine struct {
	restored []byte
}

func (m *raftHandlerStateMachine) Apply(raft.LogEntry) error {
	return nil
}

func (m *raftHandlerStateMachine) Snapshot() ([]byte, error) {
	return nil, nil
}

func (m *raftHandlerStateMachine) Restore(data []byte) error {
	m.restored = append([]byte(nil), data...)
	return nil
}

func newRaftHandlerNode(t *testing.T, stateMachine raft.StateMachine) *raft.RaftNode {
	t.Helper()
	node, err := raft.NewRaftNode(&raft.Config{
		NodeID:             "node1",
		Peers:              map[string]string{},
		ElectionTimeoutMin: time.Hour,
		ElectionTimeoutMax: 2 * time.Hour,
		HeartbeatInterval:  time.Second,
	}, stateMachine, nil)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	t.Cleanup(node.Stop)
	return node
}

func TestRaftHandler_InstallSnapshotMapsValidationError(t *testing.T) {
	handler := newRaftHandler(newRaftHandlerNode(t, &raftHandlerStateMachine{}))
	_, err := handler.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotRequest{
		Term:              1,
		LeaderId:          "node2",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Done:              true,
		ClusterConfig:     map[string]string{"node1": ""},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestRaftHandler_InstallSnapshotMapsChunkFields(t *testing.T) {
	stateMachine := &raftHandlerStateMachine{}
	handler := newRaftHandler(newRaftHandlerNode(t, stateMachine))
	data := []byte("snapshot")
	checksum := sha256.Sum256(data)

	response, err := handler.InstallSnapshot(context.Background(), &raftpb.InstallSnapshotRequest{
		Term:              1,
		LeaderId:          "node2",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              data,
		Done:              true,
		TotalSize:         uint64(len(data)),
		Checksum:          checksum[:],
		ClusterConfig: map[string]string{
			"node1": "",
			"node2": "addr2",
		},
	})
	if err != nil {
		t.Fatalf("InstallSnapshot failed: %v", err)
	}
	if response.Term != 1 || string(stateMachine.restored) != "snapshot" {
		t.Fatalf("InstallSnapshot fields were not mapped correctly")
	}
}
