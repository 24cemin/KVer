package server

import (
	"context"
	"testing"

	"github.com/emin/kver/internal/kvstore"
	"github.com/emin/kver/internal/raft"
	kvpb "github.com/emin/kver/proto/kv/gen"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ─── Mock helpers ─────────────────────────────────────────────────────────────

type mockKVReader struct {
	strings map[string]string
	hashes  map[string]map[string]string
}

func newMockKVReader() *mockKVReader {
	return &mockKVReader{
		strings: make(map[string]string),
		hashes:  make(map[string]map[string]string),
	}
}

func (m *mockKVReader) Get(key string) (string, error) {
	v, ok := m.strings[key]
	if !ok {
		return "", kvstore.ErrKeyNotFound
	}
	return v, nil
}
func (m *mockKVReader) HGet(key, field string) (string, error) {
	h, ok := m.hashes[key]
	if !ok {
		return "", kvstore.ErrKeyNotFound
	}
	v, ok := h[field]
	if !ok {
		return "", kvstore.ErrKeyNotFound
	}
	return v, nil
}
func (m *mockKVReader) HGetAll(key string) (map[string]string, error) { return m.hashes[key], nil }
func (m *mockKVReader) HExists(key, field string) (bool, error) {
	h, ok := m.hashes[key]
	if !ok {
		return false, nil
	}
	_, exists := h[field]
	return exists, nil
}
func (m *mockKVReader) LRange(_ string, _, _ int) ([]string, error)          { return nil, nil }
func (m *mockKVReader) LLen(_ string) (int64, error)                         { return 0, nil }
func (m *mockKVReader) ZScore(_, _ string) (float64, error)                  { return 0, nil }
func (m *mockKVReader) ZRank(_, _ string) (int, error)                       { return 0, nil }
func (m *mockKVReader) ZRange(_ string, _, _ int, _ bool) ([]string, error)  { return nil, nil }
func (m *mockKVReader) ZRevRange(_ string, _, _ int, _ bool) ([]string, error) { return nil, nil }

type mockProposerT struct {
	isLeader   bool
	leaderAddr string // non-empty → simulate follower with known leader
	proposed   []*kvpb.CommandPayload
	proposeCnt int
}

func (m *mockProposerT) Propose(cmd []byte) error {
	var p kvpb.CommandPayload
	_ = proto.Unmarshal(cmd, &p)
	m.proposed = append(m.proposed, &p)
	m.proposeCnt++
	return nil
}
func (m *mockProposerT) IsLeader() bool { return m.isLeader }
func (m *mockProposerT) ProposeMembershipChange(change raft.MembershipChange) error {
	return nil
}
func (m *mockProposerT) ReadIndex(_ context.Context) (uint64, error) {
	if !m.isLeader {
		return 0, raft.ErrNotLeader
	}
	return 0, nil
}
func (m *mockProposerT) LeaderAddr() string { return m.leaderAddr }
func (m *mockProposerT) Term() uint64       { return 1 }
func (m *mockProposerT) State() raft.NodeState {
	if m.isLeader {
		return raft.Leader
	}
	return raft.Follower
}
func (m *mockProposerT) NodeID() string { return "mock-node" }
func (m *mockProposerT) last() *kvpb.CommandPayload {
	if len(m.proposed) == 0 {
		return nil
	}
	return m.proposed[len(m.proposed)-1]
}

// ─── T4a: Okuma testleri ──────────────────────────────────────────────────────

func TestKVHandler_Get_ReadsFromStore(t *testing.T) {
	reader := newMockKVReader()
	reader.strings["foo"] = "bar"
	h := newKVHandler(&mockProposerT{isLeader: true}, reader)

	resp, err := h.Get(context.Background(), &kvpb.GetRequest{Key: "foo"})
	if err != nil || resp.Value != "bar" {
		t.Errorf("expected 'bar', got '%s' err=%v", resp.GetValue(), err)
	}
}

func TestKVHandler_Get_KeyNotFound(t *testing.T) {
	h := newKVHandler(&mockProposerT{isLeader: true}, newMockKVReader())
	_, err := h.Get(context.Background(), &kvpb.GetRequest{Key: "missing"})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func TestKVHandler_HGet_ReadsFromStore(t *testing.T) {
	reader := newMockKVReader()
	reader.hashes["user"] = map[string]string{"name": "emin"}
	h := newKVHandler(&mockProposerT{isLeader: true}, reader)

	resp, err := h.HGet(context.Background(), &kvpb.HGetRequest{Key: "user", Field: "name"})
	if err != nil || resp.Value != "emin" {
		t.Errorf("expected 'emin', got '%s' err=%v", resp.GetValue(), err)
	}
}

func TestKVHandler_HGetAll_ReadsFromStore(t *testing.T) {
	reader := newMockKVReader()
	reader.hashes["cfg"] = map[string]string{"host": "localhost", "port": "8080"}
	h := newKVHandler(&mockProposerT{isLeader: true}, reader)

	resp, err := h.HGetAll(context.Background(), &kvpb.HGetAllRequest{Key: "cfg"})
	if err != nil || resp.Fields["host"] != "localhost" || resp.Fields["port"] != "8080" {
		t.Errorf("unexpected HGetAll result: %v err=%v", resp.GetFields(), err)
	}
}

func TestKVHandler_HExists_ReadsFromStore(t *testing.T) {
	reader := newMockKVReader()
	reader.hashes["h"] = map[string]string{"f": "v"}
	h := newKVHandler(&mockProposerT{isLeader: true}, reader)

	resp, err := h.HExists(context.Background(), &kvpb.HExistsRequest{Key: "h", Field: "f"})
	if err != nil || !resp.Exists {
		t.Errorf("expected HExists=true, got %v err=%v", resp.GetExists(), err)
	}
	resp, _ = h.HExists(context.Background(), &kvpb.HExistsRequest{Key: "h", Field: "missing"})
	if resp.Exists {
		t.Error("expected HExists=false for missing field")
	}
}

// ─── T4b: Yazma testleri — Propose çağrısı doğrulanıyor ──────────────────────

func TestKVHandler_Set_ProposesCalled(t *testing.T) {
	proposer := &mockProposerT{isLeader: true}
	h := newKVHandler(proposer, newMockKVReader())

	_, err := h.Set(context.Background(), &kvpb.SetRequest{Key: "k", Value: "v", TtlMs: 0})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	p := proposer.last()
	if p.Op != "SET" {
		t.Errorf("expected Op=SET, got %s", p.Op)
	}
	if len(p.Args) < 2 || p.Args[0] != "k" || p.Args[1] != "v" {
		t.Errorf("unexpected Args: %v", p.Args)
	}
	if proposer.proposeCnt != 1 {
		t.Errorf("expected 1 Propose call, got %d", proposer.proposeCnt)
	}
}

func TestKVHandler_Set_NotLeader_ReturnsError(t *testing.T) {
	proposer := &mockProposerT{isLeader: false}
	h := newKVHandler(proposer, newMockKVReader())

	_, err := h.Set(context.Background(), &kvpb.SetRequest{Key: "k", Value: "v"})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", err)
	}
	if proposer.proposeCnt != 0 {
		t.Error("Propose should not be called when not leader")
	}
}

func TestKVHandler_Delete_ProposesCalled(t *testing.T) {
	proposer := &mockProposerT{isLeader: true}
	h := newKVHandler(proposer, newMockKVReader())

	_, err := h.Delete(context.Background(), &kvpb.DeleteRequest{Key: "mykey"})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	p := proposer.last()
	if p.Op != "DEL" || p.Args[0] != "mykey" {
		t.Errorf("unexpected payload: %+v", p)
	}
}

func TestKVHandler_HSet_ProposesCalled(t *testing.T) {
	proposer := &mockProposerT{isLeader: true}
	h := newKVHandler(proposer, newMockKVReader())

	_, err := h.HSet(context.Background(), &kvpb.HSetRequest{Key: "user", Field: "name", Value: "emin"})
	if err != nil {
		t.Fatalf("HSet failed: %v", err)
	}
	p := proposer.last()
	if p.Op != "HSET" || p.Args[0] != "user" || p.Args[1] != "name" || p.Args[2] != "emin" {
		t.Errorf("unexpected payload: %+v", p)
	}
}

func TestKVHandler_HDelete_ProposesCalled(t *testing.T) {
	proposer := &mockProposerT{isLeader: true}
	h := newKVHandler(proposer, newMockKVReader())

	_, err := h.HDelete(context.Background(), &kvpb.HDeleteRequest{Key: "user", Field: "name"})
	if err != nil {
		t.Fatalf("HDelete failed: %v", err)
	}
	p := proposer.last()
	if p.Op != "HDEL" || p.Args[0] != "user" || p.Args[1] != "name" {
		t.Errorf("unexpected payload: %+v", p)
	}
}

func TestKVHandler_LPush_ProposesCalled(t *testing.T) {
	proposer := &mockProposerT{isLeader: true}
	h := newKVHandler(proposer, newMockKVReader())

	_, err := h.LPush(context.Background(), &kvpb.LPushRequest{Key: "list", Values: []string{"a", "b"}, TtlMs: 0})
	if err != nil {
		t.Fatalf("LPush failed: %v", err)
	}
	p := proposer.last()
	if p.Op != "LPUSH" || p.Args[0] != "list" {
		t.Errorf("unexpected payload: %+v", p)
	}
	// Args: ["list", "a", "b", "0"]
	if p.Args[1] != "a" || p.Args[2] != "b" {
		t.Errorf("values not in args: %v", p.Args)
	}
}

func TestKVHandler_ZAdd_ProposesCalled(t *testing.T) {
	proposer := &mockProposerT{isLeader: true}
	h := newKVHandler(proposer, newMockKVReader())

	_, err := h.ZAdd(context.Background(), &kvpb.ZAddRequest{Key: "scores", Score: 9.5, Member: "alice"})
	if err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}
	p := proposer.last()
	if p.Op != "ZADD" || p.Args[0] != "scores" || p.Args[2] != "alice" {
		t.Errorf("unexpected payload: %+v", p)
	}
}

func TestKVHandler_ZRem_ProposesCalled(t *testing.T) {
	proposer := &mockProposerT{isLeader: true}
	h := newKVHandler(proposer, newMockKVReader())

	_, err := h.ZRem(context.Background(), &kvpb.ZRemRequest{Key: "scores", Member: "alice"})
	if err != nil {
		t.Fatalf("ZRem failed: %v", err)
	}
	p := proposer.last()
	if p.Op != "ZREM" || p.Args[0] != "scores" || p.Args[1] != "alice" {
		t.Errorf("unexpected payload: %+v", p)
	}
}

func TestKVHandler_Incr_ProposesCalled(t *testing.T) {
	proposer := &mockProposerT{isLeader: true}
	reader := newMockKVReader()
	reader.strings["counter"] = "1"
	h := newKVHandler(proposer, reader)

	_, err := h.Incr(context.Background(), &kvpb.IncrRequest{Key: "counter"})
	if err != nil {
		t.Fatalf("Incr failed: %v", err)
	}
	p := proposer.last()
	if p.Op != "INCR" || p.Args[0] != "counter" {
		t.Errorf("unexpected payload: %+v", p)
	}
}

func TestKVHandler_SetOnNonLeader(t *testing.T) {
	// Yukarıda TestKVHandler_Set_NotLeader_ReturnsError ile kapsanıyor.
}
func TestKVHandler_Delete(t *testing.T) {
	// Yukarıda TestKVHandler_Delete_ProposesCalled ile kapsanıyor.
}
