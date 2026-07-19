// Package server — kv_handler.go
// KV gRPC handler'ı: GET, SET, DEL ve diğer KV komutlarını işler.
//
// Mimari:
//
//	OKUMA  → ReadIndex (linearizable) → lider ise yerel store, değilse lidere forward
//	YAZMA  → lider ise Raft propose, değilse lidere forward
package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/emin/kver/internal/kvstore"
	"github.com/emin/kver/internal/raft"
	kvpb "github.com/emin/kver/proto/kv/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RaftProposer defines the minimal set of Raft operations needed by the KVHandler.
type RaftProposer interface {
	Propose(cmd []byte) error
	IsLeader() bool
	ProposeMembershipChange(change raft.MembershipChange) error
	// ReadIndex, linearizable okuma için liderliği çoğunluğa doğrular.
	ReadIndex(ctx context.Context) (uint64, error)
	// LeaderAddr, bilinen güncel liderin gRPC adresini döner.
	// Lider bu node ise veya bilinmiyorsa boş string döner.
	LeaderAddr() string
	Term() uint64
	State() raft.NodeState
	NodeID() string
}

// KVHandler, KV gRPC servisini implement eder.
// Okumalar: ReadIndex ile linearizable, follower'da forward.
// Yazmalar: lider ise Raft log'una, follower ise lidere forward.
type KVHandler struct {
	kvpb.UnimplementedKVServiceServer
	raft    RaftProposer
	kvStore kvstore.KVReader

	// Leader forwarding bağlantı cache'i
	fwdMu   sync.Mutex
	fwdAddr string
	fwdConn *grpc.ClientConn
}

// newKVHandler, yeni bir KVHandler oluşturur.
func newKVHandler(raft RaftProposer, kv kvstore.KVReader) *KVHandler {
	return &KVHandler{raft: raft, kvStore: kv}
}

// Close, leader forwarding bağlantısını kapatır.
func (h *KVHandler) Close() {
	h.fwdMu.Lock()
	defer h.fwdMu.Unlock()
	if h.fwdConn != nil {
		_ = h.fwdConn.Close()
		h.fwdConn = nil
		h.fwdAddr = ""
	}
}

// getLeaderClient, lidere olan gRPC bağlantısını döner.
// Adres değişmişse eski bağlantıyı kapatır ve yeni bağlantı kurar.
func (h *KVHandler) getLeaderClient(addr string) (kvpb.KVServiceClient, error) {
	h.fwdMu.Lock()
	defer h.fwdMu.Unlock()
	if h.fwdConn != nil && h.fwdAddr != addr {
		_ = h.fwdConn.Close()
		h.fwdConn = nil
	}
	if h.fwdConn == nil {
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
		h.fwdConn = conn
		h.fwdAddr = addr
	}
	return kvpb.NewKVServiceClient(h.fwdConn), nil
}

// forwardWrite, yazma isteğini lider adresine forward eder.
// fn, lider client'ı kullanarak asıl RPC çağrısını yapar.
func (h *KVHandler) forwardWrite(ctx context.Context, fn func(kvpb.KVServiceClient) error) error {
	leaderAddr := h.raft.LeaderAddr()
	if leaderAddr == "" {
		return status.Error(codes.FailedPrecondition, "not leader, no known leader")
	}
	client, err := h.getLeaderClient(leaderAddr)
	if err != nil {
		return status.Errorf(codes.Unavailable, "leader unreachable: %v", err)
	}
	return fn(client)
}

// forwardRead, okuma isteğini lider adresine forward eder.
func (h *KVHandler) forwardRead(ctx context.Context, fn func(kvpb.KVServiceClient) error) error {
	leaderAddr := h.raft.LeaderAddr()
	if leaderAddr == "" {
		return status.Error(codes.FailedPrecondition, "no known leader")
	}
	client, err := h.getLeaderClient(leaderAddr)
	if err != nil {
		return status.Errorf(codes.Unavailable, "leader unreachable: %v", err)
	}
	return fn(client)
}

// propose, komutu JSON'a serialize edip Raft'a gönderir.
// Çağıran daha önce IsLeader() kontrolü yapmış olmalıdır.
// raft.Propose() ErrNotLeader dönerse FailedPrecondition olarak wrap edilir.
func (h *KVHandler) propose(op string, args ...string) error {
	payload := &kvpb.CommandPayload{Op: op, Args: args}
	b, err := proto.Marshal(payload)
	if err != nil {
		return status.Errorf(codes.Internal, "proto marshal error: %v", err)
	}
	if err := h.raft.Propose(b); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return status.Error(codes.FailedPrecondition, "not leader")
		}
		if errors.Is(err, raft.ErrProposeChannelFull) {
			return status.Error(codes.ResourceExhausted, "server is overloaded: propose channel is full")
		}
		return status.Errorf(codes.Internal, "propose error: %v", err)
	}
	return nil
}

// notFoundToGRPC, ErrKeyNotFound hatalarını gRPC codes.NotFound'a çevirir.
func notFoundToGRPC(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, kvstore.ErrKeyNotFound) {
		return status.Error(codes.NotFound, "key not found")
	}
	if errors.Is(err, kvstore.ErrWrongType) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Errorf(codes.Internal, "%v", err)
}

// ─── String Operations ────────────────────────────────────────────────────────

func (h *KVHandler) Set(ctx context.Context, req *kvpb.SetRequest) (*kvpb.SetResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.SetResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.Set(ctx, req)
			return e
		})
		return resp, err
	}
	var expiryMilli int64
	if req.TtlMs > 0 {
		expiryMilli = time.Now().UnixMilli() + req.TtlMs
	}
	if err := h.propose("SET", req.Key, req.Value, strconv.FormatInt(expiryMilli, 10)); err != nil {
		return nil, err
	}
	return &kvpb.SetResponse{Success: true}, nil
}

func (h *KVHandler) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	// ReadIndex: linearizable read — liderliği doğrula
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		// Lider değiliz veya ReadIndex başarısız — lidere forward et
		var resp *kvpb.GetResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.Get(ctx, req)
			return e
		})
		return resp, ferr
	}
	val, err := h.kvStore.Get(req.Key)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.GetResponse{Value: val}, nil
}

func (h *KVHandler) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.DeleteResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.Delete(ctx, req)
			return e
		})
		return resp, err
	}
	if err := h.propose("DEL", req.Key); err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{Success: true}, nil
}

func (h *KVHandler) Incr(ctx context.Context, req *kvpb.IncrRequest) (*kvpb.IncrResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.IncrResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.Incr(ctx, req)
			return e
		})
		return resp, err
	}
	if err := h.propose("INCR", req.Key); err != nil {
		return nil, err
	}
	valStr, err := h.kvStore.Get(req.Key)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	newVal, _ := strconv.ParseInt(valStr, 10, 64)
	return &kvpb.IncrResponse{NewValue: newVal}, nil
}

func (h *KVHandler) Decr(ctx context.Context, req *kvpb.DecrRequest) (*kvpb.DecrResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.DecrResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.Decr(ctx, req)
			return e
		})
		return resp, err
	}
	if err := h.propose("DECR", req.Key); err != nil {
		return nil, err
	}
	valStr, err := h.kvStore.Get(req.Key)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	newVal, _ := strconv.ParseInt(valStr, 10, 64)
	return &kvpb.DecrResponse{NewValue: newVal}, nil
}

// ─── Hash Operations ──────────────────────────────────────────────────────────

func (h *KVHandler) HSet(ctx context.Context, req *kvpb.HSetRequest) (*kvpb.HSetResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.HSetResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.HSet(ctx, req)
			return e
		})
		return resp, err
	}
	var expiryMilli int64
	if req.TtlMs > 0 {
		expiryMilli = time.Now().UnixMilli() + req.TtlMs
	}
	if err := h.propose("HSET", req.Key, req.Field, req.Value, strconv.FormatInt(expiryMilli, 10)); err != nil {
		return nil, err
	}
	return &kvpb.HSetResponse{Success: true}, nil
}

func (h *KVHandler) HGet(ctx context.Context, req *kvpb.HGetRequest) (*kvpb.HGetResponse, error) {
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		var resp *kvpb.HGetResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.HGet(ctx, req)
			return e
		})
		return resp, ferr
	}
	val, err := h.kvStore.HGet(req.Key, req.Field)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.HGetResponse{Value: val}, nil
}

func (h *KVHandler) HDelete(ctx context.Context, req *kvpb.HDeleteRequest) (*kvpb.HDeleteResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.HDeleteResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.HDelete(ctx, req)
			return e
		})
		return resp, err
	}
	if err := h.propose("HDEL", req.Key, req.Field); err != nil {
		return nil, err
	}
	return &kvpb.HDeleteResponse{Success: true}, nil
}

func (h *KVHandler) HGetAll(ctx context.Context, req *kvpb.HGetAllRequest) (*kvpb.HGetAllResponse, error) {
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		var resp *kvpb.HGetAllResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.HGetAll(ctx, req)
			return e
		})
		return resp, ferr
	}
	fields, err := h.kvStore.HGetAll(req.Key)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.HGetAllResponse{Fields: fields}, nil
}

func (h *KVHandler) HExists(ctx context.Context, req *kvpb.HExistsRequest) (*kvpb.HExistsResponse, error) {
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		var resp *kvpb.HExistsResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.HExists(ctx, req)
			return e
		})
		return resp, ferr
	}
	exists, err := h.kvStore.HExists(req.Key, req.Field)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.HExistsResponse{Exists: exists}, nil
}

// ─── List Operations ──────────────────────────────────────────────────────────

func (h *KVHandler) LPush(ctx context.Context, req *kvpb.LPushRequest) (*kvpb.LPushResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.LPushResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.LPush(ctx, req)
			return e
		})
		return resp, err
	}
	var expiryMilli int64
	if req.TtlMs > 0 {
		expiryMilli = time.Now().UnixMilli() + req.TtlMs
	}
	args := append([]string{req.Key}, req.Values...)
	args = append(args, strconv.FormatInt(expiryMilli, 10))
	if err := h.propose("LPUSH", args...); err != nil {
		return nil, err
	}
	return &kvpb.LPushResponse{Count: 0}, nil
}

func (h *KVHandler) RPush(ctx context.Context, req *kvpb.RPushRequest) (*kvpb.RPushResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.RPushResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.RPush(ctx, req)
			return e
		})
		return resp, err
	}
	var expiryMilli int64
	if req.TtlMs > 0 {
		expiryMilli = time.Now().UnixMilli() + req.TtlMs
	}
	args := append([]string{req.Key}, req.Values...)
	args = append(args, strconv.FormatInt(expiryMilli, 10))
	if err := h.propose("RPUSH", args...); err != nil {
		return nil, err
	}
	return &kvpb.RPushResponse{Count: 0}, nil
}

func (h *KVHandler) LPop(ctx context.Context, req *kvpb.LPopRequest) (*kvpb.LPopResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.LPopResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.LPop(ctx, req)
			return e
		})
		return resp, err
	}
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		return nil, err
	}
	val, err := h.kvStore.LRange(req.Key, 0, 0)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	if len(val) == 0 {
		return nil, status.Error(codes.NotFound, "list is empty")
	}
	popVal := val[0]

	// KNOWN LIMITATION: This read happens before propose. Under concurrent
	// LPop operations, multiple clients might read the same head element,
	// but different elements will be popped from the store. This is a
	// semantic limitation of the current StateMachine interface and is
	// documented as a known issue in the thesis.
	if err := h.propose("LPOP", req.Key); err != nil {
		return nil, err
	}
	return &kvpb.LPopResponse{Value: popVal}, nil
}

func (h *KVHandler) RPop(ctx context.Context, req *kvpb.RPopRequest) (*kvpb.RPopResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.RPopResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.RPop(ctx, req)
			return e
		})
		return resp, err
	}
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		return nil, err
	}
	val, err := h.kvStore.LRange(req.Key, -1, -1)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	if len(val) == 0 {
		return nil, status.Error(codes.NotFound, "list is empty")
	}
	popVal := val[0]

	// KNOWN LIMITATION: This read happens before propose. Under concurrent
	// RPop operations, multiple clients might read the same tail element,
	// but different elements will be popped from the store. This is a
	// semantic limitation of the current StateMachine interface and is
	// documented as a known issue in the thesis.
	if err := h.propose("RPOP", req.Key); err != nil {
		return nil, err
	}
	return &kvpb.RPopResponse{Value: popVal}, nil
}

func (h *KVHandler) LRange(ctx context.Context, req *kvpb.LRangeRequest) (*kvpb.LRangeResponse, error) {
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		var resp *kvpb.LRangeResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.LRange(ctx, req)
			return e
		})
		return resp, ferr
	}
	vals, err := h.kvStore.LRange(req.Key, int(req.Start), int(req.Stop))
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.LRangeResponse{Values: vals}, nil
}

func (h *KVHandler) LLen(ctx context.Context, req *kvpb.LLenRequest) (*kvpb.LLenResponse, error) {
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		var resp *kvpb.LLenResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.LLen(ctx, req)
			return e
		})
		return resp, ferr
	}
	length, err := h.kvStore.LLen(req.Key)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.LLenResponse{Count: length}, nil
}

// ─── Sorted Set Operations ────────────────────────────────────────────────────

func (h *KVHandler) ZAdd(ctx context.Context, req *kvpb.ZAddRequest) (*kvpb.ZAddResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.ZAddResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.ZAdd(ctx, req)
			return e
		})
		return resp, err
	}
	var expiryMilli int64
	if req.TtlMs > 0 {
		expiryMilli = time.Now().UnixMilli() + req.TtlMs
	}
	scoreStr := strconv.FormatFloat(req.Score, 'f', -1, 64)
	if err := h.propose("ZADD", req.Key, scoreStr, req.Member, strconv.FormatInt(expiryMilli, 10)); err != nil {
		return nil, err
	}
	return &kvpb.ZAddResponse{Success: true}, nil
}

func (h *KVHandler) ZScore(ctx context.Context, req *kvpb.ZScoreRequest) (*kvpb.ZScoreResponse, error) {
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		var resp *kvpb.ZScoreResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.ZScore(ctx, req)
			return e
		})
		return resp, ferr
	}
	score, err := h.kvStore.ZScore(req.Key, req.Member)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.ZScoreResponse{Score: score}, nil
}

func (h *KVHandler) ZRank(ctx context.Context, req *kvpb.ZRankRequest) (*kvpb.ZRankResponse, error) {
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		var resp *kvpb.ZRankResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.ZRank(ctx, req)
			return e
		})
		return resp, ferr
	}
	rank, err := h.kvStore.ZRank(req.Key, req.Member)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.ZRankResponse{Rank: int64(rank)}, nil
}

func (h *KVHandler) ZRange(ctx context.Context, req *kvpb.ZRangeRequest) (*kvpb.ZRangeResponse, error) {
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		var resp *kvpb.ZRangeResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.ZRange(ctx, req)
			return e
		})
		return resp, ferr
	}
	members, err := h.kvStore.ZRange(req.Key, int(req.Start), int(req.Stop), req.WithScores)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.ZRangeResponse{Members: members}, nil
}

func (h *KVHandler) ZRevRange(ctx context.Context, req *kvpb.ZRevRangeRequest) (*kvpb.ZRevRangeResponse, error) {
	if _, err := h.raft.ReadIndex(ctx); err != nil {
		var resp *kvpb.ZRevRangeResponse
		ferr := h.forwardRead(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.ZRevRange(ctx, req)
			return e
		})
		return resp, ferr
	}
	members, err := h.kvStore.ZRevRange(req.Key, int(req.Start), int(req.Stop), req.WithScores)
	if err != nil {
		return nil, notFoundToGRPC(err)
	}
	return &kvpb.ZRevRangeResponse{Members: members}, nil
}

func (h *KVHandler) ZRem(ctx context.Context, req *kvpb.ZRemRequest) (*kvpb.ZRemResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.ZRemResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.ZRem(ctx, req)
			return e
		})
		return resp, err
	}
	if err := h.propose("ZREM", req.Key, req.Member); err != nil {
		return nil, err
	}
	return &kvpb.ZRemResponse{Success: true}, nil
}

// ─── Cluster Management ───────────────────────────────────────────────────────

func (h *KVHandler) AddNode(ctx context.Context, req *kvpb.AddNodeRequest) (*kvpb.AddNodeResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.AddNodeResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.AddNode(ctx, req)
			return e
		})
		return resp, err
	}
	change := raft.MembershipChange{
		Type:    raft.AddServer,
		NodeID:  req.NodeId,
		Address: req.Address,
	}
	if err := h.raft.ProposeMembershipChange(change); err != nil {
		return nil, status.Errorf(codes.Internal, "membership change failed: %v", err)
	}
	return &kvpb.AddNodeResponse{Success: true}, nil
}

func (h *KVHandler) RemoveNode(ctx context.Context, req *kvpb.RemoveNodeRequest) (*kvpb.RemoveNodeResponse, error) {
	if !h.raft.IsLeader() {
		var resp *kvpb.RemoveNodeResponse
		err := h.forwardWrite(ctx, func(c kvpb.KVServiceClient) error {
			var e error
			resp, e = c.RemoveNode(ctx, req)
			return e
		})
		return resp, err
	}
	change := raft.MembershipChange{
		Type:   raft.RemoveServer,
		NodeID: req.NodeId,
	}
	if err := h.raft.ProposeMembershipChange(change); err != nil {
		return nil, status.Errorf(codes.Internal, "membership change failed: %v", err)
	}
	return &kvpb.RemoveNodeResponse{Success: true}, nil
}

// ─── Observability ────────────────────────────────────────────────────────────

func (h *KVHandler) Ping(ctx context.Context, req *kvpb.PingRequest) (*kvpb.PingResponse, error) {
	fmt.Printf("[%s] Received Ping request\n", h.raft.NodeID())
	stateMap := map[raft.NodeState]string{
		raft.Follower:     "Follower",
		raft.Candidate:    "Candidate",
		raft.PreCandidate: "PreCandidate",
		raft.Leader:       "Leader",
	}
	stateStr := stateMap[h.raft.State()]
	return &kvpb.PingResponse{
		NodeId: h.raft.NodeID(),
		State:  stateStr,
		Term:   h.raft.Term(),
	}, nil
}
