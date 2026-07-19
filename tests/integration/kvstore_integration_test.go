package integration

import (
	"strconv"
	"testing"
	"time"

	"github.com/emin/kver/internal/kvstore"
	"github.com/emin/kver/internal/raft"
	kvpb "github.com/emin/kver/proto/kv/gen"
	"google.golang.org/protobuf/proto"
)

// makeSingleNodeKVCluster creates a 1-node Raft cluster wired to a real KVStore.
func makeSingleNodeKVCluster(t *testing.T) (*raft.RaftNode, *kvstore.KVStore, func()) {
	t.Helper()

	kv := kvstore.NewKVStore()
	id := "node1"
	peers := map[string]string{}

	cfg := &raft.Config{
		NodeID:              id,
		Peers:               peers,
		HeartbeatInterval: 20 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		MaxLogEntriesPerRPC: 100,
	}

	transport := newLocalTransport()
	node, err := raft.NewRaftNode(cfg, kv, transport)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}
	transport.register(id, node)

	// Wait to become leader (election timer will automatically trigger)
	timeout := time.After(3 * time.Second)
	for node.State() != raft.Leader {
		select {
		case <-timeout:
			t.Fatal("node did not become leader")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cleanup := func() {
		node.Stop()
		kv.Close()
	}

	return node, kv, cleanup
}

func proposeAndWait(t *testing.T, node *raft.RaftNode, op string, args ...string) {
	t.Helper()

	payload := &kvpb.CommandPayload{Op: op, Args: args}
	b, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	expectedIndex := node.LastLogIndex() + 1

	err = node.Propose(b)
	if err != nil {
		t.Fatalf("Propose %s failed: %v", op, err)
	}

	// Wait for the commit index to catch up and the entry to be applied
	timeout := time.After(2 * time.Second)
	for node.CommitIndex() < expectedIndex {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for commit index to reach %d", expectedIndex)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Wait a bit more for apply loop to process the committed entry
	// In the future, we should probably have a way to know exactly when apply is done.
	time.Sleep(50 * time.Millisecond)
}

func TestKVStore_SetGetIntegration(t *testing.T) {
	node, kv, cleanup := makeSingleNodeKVCluster(t)
	defer cleanup()

	// Propose a SET command without explicit TTL
	proposeAndWait(t, node, "SET", "mykey", "myval")

	// Read via KVStore directly
	val, err := kv.Get("mykey")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "myval" {
		t.Errorf("expected 'myval', got '%s'", val)
	}
}

func TestKVStore_TTLExpiry(t *testing.T) {
	node, kv, cleanup := makeSingleNodeKVCluster(t)
	defer cleanup()

	// Propose SET with absolute expiry: now + 100ms
	expiryMs := time.Now().Add(100 * time.Millisecond).UnixMilli()
	proposeAndWait(t, node, "SET", "ttlkey", "ephemeral", strconv.FormatInt(expiryMs, 10))

	// Should exist immediately
	val, err := kv.Get("ttlkey")
	if err != nil || val != "ephemeral" {
		t.Fatalf("expected 'ephemeral' before expiry, got err=%v, val=%v", err, val)
	}

	// Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)

	_, err = kv.Get("ttlkey")
	if err != kvstore.ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after expiry, got: %v", err)
	}
}

func TestKVStore_AllDataTypes(t *testing.T) {
	node, kv, cleanup := makeSingleNodeKVCluster(t)
	defer cleanup()

	// Hash
	proposeAndWait(t, node, "HSET", "myhash", "field1", "val1")
	if v, _ := kv.HGet("myhash", "field1"); v != "val1" {
		t.Errorf("HGet failed, got %v", v)
	}

	// List
	proposeAndWait(t, node, "LPUSH", "mylist", "item1", "0")
	proposeAndWait(t, node, "LPUSH", "mylist", "item2", "0")
	if l, _ := kv.LLen("mylist"); l != 2 {
		t.Errorf("LLen failed, got %d", l)
	}

	// SortedSet
	proposeAndWait(t, node, "ZADD", "myset", "10.5", "member1")
	if score, _ := kv.ZScore("myset", "member1"); score != 10.5 {
		t.Errorf("ZScore failed, got %v", score)
	}
}

func TestKVStore_SnapshotAndRestore(t *testing.T) {
	node, kv, cleanup := makeSingleNodeKVCluster(t)
	defer cleanup()

	// Veri ekle — string, hash, list
	proposeAndWait(t, node, "SET", "mykey", "myval")
	proposeAndWait(t, node, "HSET", "myhash", "field1", "val1")
	proposeAndWait(t, node, "LPUSH", "mylist", "item1", "0")
	proposeAndWait(t, node, "ZADD", "myset", "42.5", "member1")

	// Snapshot al
	data, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Snapshot returned empty data")
	}

	// Yeni KVStore oluştur ve restore et
	kv2 := kvstore.NewKVStore()
	defer kv2.Close()
	if err := kv2.Restore(data); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// String doğrula
	val, err := kv2.Get("mykey")
	if err != nil || val != "myval" {
		t.Errorf("String restore failed: val=%v err=%v", val, err)
	}

	// Hash doğrula
	hval, err := kv2.HGet("myhash", "field1")
	if err != nil || hval != "val1" {
		t.Errorf("Hash restore failed: val=%v err=%v", hval, err)
	}

	// List doğrula
	llen, _ := kv2.LLen("mylist")
	if llen != 1 {
		t.Errorf("List restore failed: len=%d", llen)
	}

	// SortedSet doğrula
	score, err := kv2.ZScore("myset", "member1")
	if err != nil || score != 42.5 {
		t.Errorf("SortedSet restore failed: score=%v err=%v", score, err)
	}
}

