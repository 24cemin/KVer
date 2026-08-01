package kvstore

import (
	"strconv"
	"testing"
	"time"

	"github.com/emin/kver/internal/raft"
	kvpb "github.com/emin/kver/proto/kv/gen"
	"google.golang.org/protobuf/proto"
)

// makeEntry, verilen op ve args değerleriyle test için bir raft.LogEntry oluşturur.
func makeEntry(t *testing.T, op string, args ...string) raft.LogEntry {
	t.Helper()
	payload := &kvpb.CommandPayload{Op: op, Args: args}
	b, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return raft.LogEntry{Type: raft.EntryKV, Command: b}
}

func TestApply_String_SetAndGet(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	err := kv.Apply(makeEntry(t, "SET", "key1", "hello"))
	if err != nil {
		t.Fatalf("Apply SET failed: %v", err)
	}

	got, err := kv.strings.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != "hello" {
		t.Errorf("expected 'hello', got '%s'", got)
	}
}

func TestApply_String_SetWithTTL(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	// Absolute unix timestamp: now + 50ms
	expiryMs := time.Now().Add(50 * time.Millisecond).UnixMilli()
	err := kv.Apply(makeEntry(t, "SET", "ttlkey", "val", strconv.FormatInt(expiryMs, 10)))
	if err != nil {
		t.Fatalf("Apply SET with TTL failed: %v", err)
	}

	// Hemen okuyabilmeli
	got, err := kv.strings.Get("ttlkey")
	if err != nil || got != "val" {
		t.Errorf("expected val before expiry, got '%s' err=%v", got, err)
	}

	time.Sleep(80 * time.Millisecond)

	_, err = kv.strings.Get("ttlkey")
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after expiry, got %v", err)
	}
}

func TestApply_String_Del(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	_ = kv.Apply(makeEntry(t, "SET", "k", "v"))
	if err := kv.Apply(makeEntry(t, "DEL", "k")); err != nil {
		t.Fatalf("Apply DEL failed: %v", err)
	}
	_, err := kv.strings.Get("k")
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after DEL, got %v", err)
	}
}

func TestApply_String_Del_NonExistent_IsIdempotent(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()
	// Var olmayan key → ignoreNotFound sayesinde nil döner
	if err := kv.Apply(makeEntry(t, "DEL", "ghost")); err != nil {
		t.Errorf("DEL on non-existent key should be idempotent, got: %v", err)
	}
}

func TestApply_String_IncrDecr(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	_ = kv.Apply(makeEntry(t, "SET", "counter", "10"))
	_ = kv.Apply(makeEntry(t, "INCR", "counter"))
	_ = kv.Apply(makeEntry(t, "INCR", "counter"))
	_ = kv.Apply(makeEntry(t, "DECR", "counter"))

	got, _ := kv.strings.Get("counter")
	if got != "11" {
		t.Errorf("expected '11', got '%s'", got)
	}
}

func TestApply_Hash_HSetAndHGet(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	if err := kv.Apply(makeEntry(t, "HSET", "user", "name", "emin")); err != nil {
		t.Fatalf("Apply HSET failed: %v", err)
	}

	got, err := kv.hashes.HGet("user", "name")
	if err != nil {
		t.Fatalf("HGet failed: %v", err)
	}
	if got != "emin" {
		t.Errorf("expected 'emin', got '%s'", got)
	}
}

func TestApply_Hash_HDel(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	_ = kv.Apply(makeEntry(t, "HSET", "h", "f", "v"))
	if err := kv.Apply(makeEntry(t, "HDEL", "h", "f")); err != nil {
		t.Fatalf("Apply HDEL failed: %v", err)
	}
	_, err := kv.hashes.HGet("h", "f")
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after HDEL, got %v", err)
	}
}

func TestApply_List_LPushAndLPop(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	_ = kv.Apply(makeEntry(t, "LPUSH", "mylist", "c", "b", "a", "0"))

	got, err := kv.lists.LPop("mylist")
	if err != nil {
		t.Fatalf("LPop failed: %v", err)
	}
	// LPush [c,b,a] → head = a
	if got != "a" {
		t.Errorf("expected 'a', got '%s'", got)
	}
}

func TestApply_List_RPushAndRPop(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	_ = kv.Apply(makeEntry(t, "RPUSH", "mylist", "x", "y", "z", "0"))

	got, err := kv.lists.RPop("mylist")
	if err != nil {
		t.Fatalf("RPop failed: %v", err)
	}
	// RPush [x,y,z] → tail = z
	if got != "z" {
		t.Errorf("expected 'z', got '%s'", got)
	}
}

func TestApply_List_Pop_NonExistent_IsIdempotent(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	if err := kv.Apply(makeEntry(t, "LPOP", "ghost")); err != nil {
		t.Errorf("LPOP on non-existent key should be idempotent, got: %v", err)
	}
}

func TestApply_SortedSet_ZAddAndZScore(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	if err := kv.Apply(makeEntry(t, "ZADD", "scores", "9.5", "alice")); err != nil {
		t.Fatalf("Apply ZADD failed: %v", err)
	}

	score, err := kv.sortedSets.ZScore("scores", "alice")
	if err != nil {
		t.Fatalf("ZScore failed: %v", err)
	}
	if score != 9.5 {
		t.Errorf("expected score 9.5, got %f", score)
	}
}

func TestApply_SortedSet_ZRem(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	_ = kv.Apply(makeEntry(t, "ZADD", "scores", "5.0", "bob"))
	if err := kv.Apply(makeEntry(t, "ZREM", "scores", "bob")); err != nil {
		t.Fatalf("Apply ZREM failed: %v", err)
	}
	_, err := kv.sortedSets.ZScore("scores", "bob")
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after ZREM, got %v", err)
	}
}

func TestApply_InvalidArgs_ReturnsError(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	// SET'e sadece key ver, value yok
	if err := kv.Apply(makeEntry(t, "SET", "onlykey")); err != ErrInvalidArgs {
		t.Errorf("expected ErrInvalidArgs, got %v", err)
	}

	// ZADD'e geçersiz score ver
	if err := kv.Apply(makeEntry(t, "ZADD", "z", "notanumber", "member")); err != ErrInvalidArgs {
		t.Errorf("expected ErrInvalidArgs for bad ZADD score, got %v", err)
	}
}

func TestApply_UnknownOp_ReturnsError(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	if err := kv.Apply(makeEntry(t, "NOSUCHCMD", "k")); err != ErrUnknownCommand {
		t.Errorf("expected ErrUnknownCommand, got %v", err)
	}
}

func TestApply_NonKVEntry_IsIgnored(t *testing.T) {
	kv := NewKVStore()
	defer kv.Close()

	// EntryKV olmayan entry — nil dönmeli
	entry := raft.LogEntry{Type: raft.EntryKV + 1}
	if err := kv.Apply(entry); err != nil {
		t.Errorf("non-KV entry should be ignored, got: %v", err)
	}
}

func TestKVStore_SnapshotAndRestore(t *testing.T) {
	t.Run("SnapshotAndRestore_PreservesAllTypes", func(t *testing.T) {
		kv := NewKVStore()
		defer kv.Close()

		// String ekle
		requireNoError(t, kv.strings.Set("foo", "bar", 0))
		// Hash ekle
		requireNoError(t, kv.hashes.HSet("user:1", "name", "ali", 0))
		// List ekle
		requireLPush(t, kv.lists, "queue", 0, "task1")
		// SortedSet ekle
		requireNoError(t, kv.sortedSets.ZAdd("scores", 100.0, "ali", 0))
		// Snapshot al
		data, err := kv.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot failed: %v", err)
		}
		if len(data) == 0 {
			t.Fatal("Snapshot returned empty data")
		}

		// Yeni KVStore oluştur ve restore et
		kv2 := NewKVStore()
		defer kv2.Close()
		if err := kv2.Restore(data); err != nil {
			t.Fatalf("Restore failed: %v", err)
		}

		// String kontrol
		val, err := kv2.strings.Get("foo")
		if err != nil || val != "bar" {
			t.Errorf("String restore failed: %v %v", val, err)
		}

		// Hash kontrol
		hval, err := kv2.hashes.HGet("user:1", "name")
		if err != nil || hval != "ali" {
			t.Errorf("Hash restore failed: %v %v", hval, err)
		}

		// List kontrol
		llen, _ := kv2.lists.LLen("queue")
		if llen != 1 {
			t.Errorf("List restore failed: len=%d", llen)
		}

		// SortedSet kontrol
		score, err := kv2.sortedSets.ZScore("scores", "ali")
		if err != nil || score != 100.0 {
			t.Errorf("SortedSet restore failed: %v %v", score, err)
		}
	})
}
