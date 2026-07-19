package kvstore

import (
	"container/list"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emin/kver/internal/raft"
	kvpb "github.com/emin/kver/proto/kv/gen"
	"google.golang.org/protobuf/proto"
)



// ─── KVStore ──────────────────────────────────────────────────────────────────

// KVStore, StateMachine interface'ini implement eden ana yapıdır.
// Tüm veri tiplerine (String, Hash, List, SortedSet) erişim buradan sağlanır.
type KVStore struct {
	mu         sync.RWMutex
	keyTypes   map[string]string // "string", "hash", "list"
	strings    *StringStore      // string.go
	hashes     *HashStore        // hash.go
	lists      *ListStore        // list.go
	sortedSets *SortedSetStore   // sortedset.go
	ttl        *TTLManager       // ttl.go
}

// NewKVStore, yeni bir KVStore instance'ı döndürür ve altyapıyı başlatır.
func NewKVStore() *KVStore {
	kv := &KVStore{
		keyTypes: make(map[string]string),
	}

	ttlManager := NewTTLManager(kv.deleteKey)
	kv.ttl = ttlManager
	kv.strings = NewStringStore(kv, ttlManager)
	kv.hashes = NewHashStore(kv, ttlManager)
	kv.lists = NewListStore(kv, ttlManager)
	kv.sortedSets = NewSortedSetStore(kv, ttlManager)

	ttlManager.Start(1 * time.Second) // Starts background cleanup
	return kv
}

func (kv *KVStore) CheckType(key, expected string) error {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	t, ok := kv.keyTypes[key]
	if ok && t != expected {
		return ErrWrongType
	}
	return nil
}

func (kv *KVStore) SetType(key, typ string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.keyTypes[key] = typ
}

func (kv *KVStore) CheckAndSetType(key, expected string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	t, ok := kv.keyTypes[key]
	if ok && t != expected {
		return ErrWrongType
	}
	kv.keyTypes[key] = expected
	return nil
}

func (kv *KVStore) DeleteType(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.keyTypes, key)
}

// deleteKey is the onExpire callback passed to TTLManager.
// It physically removes the key's data from whichever sub-store owns it.
func (kv *KVStore) deleteKey(key string) {
	kv.mu.RLock()
	typ, ok := kv.keyTypes[key]
	kv.mu.RUnlock()
	if !ok {
		return
	}
	switch typ {
	case "string":
		_ = kv.strings.Delete(key)
	case "hash":
		kv.hashes.deleteKey(key)
	case "list":
		kv.lists.deleteKey(key)
	case "zset":
		kv.sortedSets.deleteKey(key)
	}
}

// Apply, StateMachine interface'ini implement eder.
// Raft log'undan gelen commit edilmiş komutları parse edip ilgili veri yapısına yönlendirir.
//
// Args Sözleşmesi (CommandPayload.Args):
//
//	SET  key value [expiry_unix_ms]
//	DEL  key
//	INCR key
//	DECR key
//	HSET key field value [expiry_unix_ms]
//	HDEL key field
//	LPUSH key value... [expiry_unix_ms]  — son arg timestamp; KVHandler tarafından eklenir
//	RPUSH key value... [expiry_unix_ms]
//	LPOP key
//	RPOP key
//	ZADD key score member [expiry_unix_ms]
//	ZREM key member
func (kv *KVStore) Apply(entry raft.LogEntry) error {
	if entry.Type != raft.EntryKV {
		return nil // membership vs. — ilgili yer işler
	}

	var payload kvpb.CommandPayload
	if err := proto.Unmarshal(entry.Command, &payload); err != nil {
		return err
	}

	op := strings.ToUpper(payload.Op)
	args := payload.Args

	switch op {

	// ─── String ───────────────────────────────────────────────────────────────

	case "SET":
		// SET key value [expiry_unix_ms]
		if len(args) < 2 {
			return ErrInvalidArgs
		}
		if err := kv.strings.Set(args[0], args[1], 0); err != nil {
			return ignoreNotFound(err)
		}
		kv.ttl.SetAbsolute(args[0], parseAbsoluteMs(args, 2))
		return nil

	case "DEL":
		// DEL key
		if len(args) < 1 {
			return ErrInvalidArgs
		}
		return ignoreNotFound(kv.strings.Delete(args[0]))

	case "INCR":
		// INCR key
		if len(args) < 1 {
			return ErrInvalidArgs
		}
		_, err := kv.strings.Incr(args[0])
		return err

	case "DECR":
		// DECR key
		if len(args) < 1 {
			return ErrInvalidArgs
		}
		_, err := kv.strings.Decr(args[0])
		return err

	// ─── Hash ─────────────────────────────────────────────────────────────────

	case "HSET":
		// HSET key field value [expiry_unix_ms]
		if len(args) < 3 {
			return ErrInvalidArgs
		}
		if err := kv.hashes.HSet(args[0], args[1], args[2], 0); err != nil {
			return err
		}
		kv.ttl.SetAbsolute(args[0], parseAbsoluteMs(args, 3))
		return nil

	case "HDEL":
		// HDEL key field
		if len(args) < 2 {
			return ErrInvalidArgs
		}
		return ignoreNotFound(kv.hashes.HDelete(args[0], args[1]))

	// ─── List ─────────────────────────────────────────────────────────────────

	case "LPUSH":
		// LPUSH key value... [expiry_unix_ms]
		if len(args) < 2 {
			return ErrInvalidArgs
		}
		values, absMs := splitValuesAbsoluteMs(args[1:])
		if _, err := kv.lists.LPush(args[0], 0, values...); err != nil {
			return err
		}
		kv.ttl.SetAbsolute(args[0], absMs)
		return nil

	case "RPUSH":
		// RPUSH key value... [expiry_unix_ms]
		if len(args) < 2 {
			return ErrInvalidArgs
		}
		values, absMs := splitValuesAbsoluteMs(args[1:])
		if _, err := kv.lists.RPush(args[0], 0, values...); err != nil {
			return err
		}
		kv.ttl.SetAbsolute(args[0], absMs)
		return nil

	case "LPOP":
		// LPOP key
		if len(args) < 1 {
			return ErrInvalidArgs
		}
		_, err := kv.lists.LPop(args[0])
		return ignoreNotFound(err)

	case "RPOP":
		// RPOP key
		if len(args) < 1 {
			return ErrInvalidArgs
		}
		_, err := kv.lists.RPop(args[0])
		return ignoreNotFound(err)

	// ─── Sorted Set ───────────────────────────────────────────────────────────

	case "ZADD":
		// ZADD key score member [expiry_unix_ms]
		if len(args) < 3 {
			return ErrInvalidArgs
		}
		score, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return ErrInvalidArgs
		}
		if err := kv.sortedSets.ZAdd(args[0], score, args[2], 0); err != nil {
			return err
		}
		kv.ttl.SetAbsolute(args[0], parseAbsoluteMs(args, 3))
		return nil

	case "ZREM":
		// ZREM key member
		if len(args) < 2 {
			return ErrInvalidArgs
		}
		return ignoreNotFound(kv.sortedSets.ZRem(args[0], args[1]))

	default:
		return ErrUnknownCommand
	}
}

// parseAbsoluteMs parses an absolute Unix millisecond timestamp from args[pos].
// Returns 0 if pos is out of bounds, the string is empty, or parsing fails.
func parseAbsoluteMs(args []string, pos int) int64 {
	if pos >= len(args) {
		return 0
	}
	ms, err := strconv.ParseInt(args[pos], 10, 64)
	if err != nil {
		return 0
	}
	return ms // may be 0 or negative — SetAbsolute handles those as "no TTL"
}

// splitValuesAbsoluteMs splits values and an absolute ms timestamp from an args slice.
// The last element is treated as the absolute unix-ms expiry (appended by KVHandler).
// Returns the values and the parsed timestamp (0 = no TTL).
func splitValuesAbsoluteMs(args []string) (values []string, absMs int64) {
	if len(args) == 0 {
		return nil, 0
	}
	last := args[len(args)-1]
	ms, err := strconv.ParseInt(last, 10, 64)
	if err == nil {
		return args[:len(args)-1], ms
	}
	return args, 0
}

// ignoreNotFound, ErrKeyNotFound hatalarını yoksayar.
// Apply idempotent olmalı — aynı komut tekrar uygulanabilir.
func ignoreNotFound(err error) error {
	if err == ErrKeyNotFound {
		return nil
	}
	return err
}

// kvSnapshot, KVStore'un tüm state'ini JSON'a serileştirmek için kullanılır.
// Snapshot, StateMachine interface'ini implement eder.
// KVStore'un tüm state'ini Protobuf olarak serialize eder.
func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.RLock()

	snap := &kvpb.Snapshot{
		KeyTypes:   make(map[string]string),
		Strings:    make(map[string]string),
		Hashes:     make(map[string]*kvpb.HashData),
		Lists:      make(map[string]*kvpb.ListData),
		SortedSets: make(map[string]*kvpb.ZSetData),
		Ttls:       make(map[string]int64),
	}

	// KeyTypes kopyala
	for k, v := range kv.keyTypes {
		snap.KeyTypes[k] = v
	}

	// Strings kopyala
	kv.strings.mu.RLock()
	for k, v := range kv.strings.data {
		snap.Strings[k] = v
	}
	kv.strings.mu.RUnlock()

	// Hashes kopyala
	kv.hashes.mu.RLock()
	for k, fields := range kv.hashes.data {
		hd := &kvpb.HashData{Fields: make(map[string]string, len(fields))}
		for f, v := range fields {
			hd.Fields[f] = v
		}
		snap.Hashes[k] = hd
	}
	kv.hashes.mu.RUnlock()

	// Lists kopyala
	kv.lists.mu.RLock()
	for k, ll := range kv.lists.data {
		cp := make([]string, 0, ll.Len())
		for e := ll.Front(); e != nil; e = e.Next() {
			cp = append(cp, e.Value.(string))
		}
		snap.Lists[k] = &kvpb.ListData{Elements: cp}
	}
	kv.lists.mu.RUnlock()

	// SortedSets kopyala
	kv.sortedSets.mu.RLock()
	for k, sl := range kv.sortedSets.data {
		nodes := sl.rangeByRank(0, sl.length-1)
		entries := make([]*kvpb.ZSetEntry, len(nodes))
		for i, n := range nodes {
			entries[i] = &kvpb.ZSetEntry{
				Member: n.member,
				Score:  n.score,
			}
		}
		snap.SortedSets[k] = &kvpb.ZSetData{Entries: entries}
	}
	kv.sortedSets.mu.RUnlock()

	// TTLs kopyala
	kv.ttl.mu.RLock()
	for k, t := range kv.ttl.expiry {
		snap.Ttls[k] = t.UnixMilli()
	}
	kv.ttl.mu.RUnlock()

	// Kilitleri serbest bırak (Serialization I/O veya CPU overhead'ini kilit dışında tutmak için)
	kv.mu.RUnlock()

	return proto.Marshal(snap)
}

// Restore, StateMachine interface'ini implement eder.
// Snapshot bayt dizisinden KVStore state'ini yeniden kurar.
func (kv *KVStore) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var snap kvpb.Snapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// KeyTypes restore
	kv.keyTypes = snap.KeyTypes
	if kv.keyTypes == nil {
		kv.keyTypes = make(map[string]string)
	}

	// Strings restore
	kv.strings.mu.Lock()
	kv.strings.data = snap.Strings
	if kv.strings.data == nil {
		kv.strings.data = make(map[string]string)
	}
	kv.strings.mu.Unlock()

	// Hashes restore
	kv.hashes.mu.Lock()
	kv.hashes.data = make(map[string]map[string]string, len(snap.Hashes))
	for k, hd := range snap.Hashes {
		kv.hashes.data[k] = hd.Fields
	}
	kv.hashes.mu.Unlock()

	// Lists restore
	kv.lists.mu.Lock()
	kv.lists.data = make(map[string]*list.List, len(snap.Lists))
	for k, ld := range snap.Lists {
		ll := list.New()
		for _, v := range ld.Elements {
			ll.PushBack(v)
		}
		kv.lists.data[k] = ll
	}
	kv.lists.mu.Unlock()

	// SortedSets restore
	kv.sortedSets.mu.Lock()
	kv.sortedSets.data = make(map[string]*skipList, len(snap.SortedSets))
	for k, zd := range snap.SortedSets {
		sl := newSkipList()
		for _, e := range zd.Entries {
			sl.insert(e.Score, e.Member)
		}
		kv.sortedSets.data[k] = sl
	}
	kv.sortedSets.mu.Unlock()

	// TTLs restore
	kv.ttl.mu.Lock()
	kv.ttl.expiry = make(map[string]time.Time, len(snap.Ttls))
	for k, ts := range snap.Ttls {
		kv.ttl.expiry[k] = time.UnixMilli(ts)
	}
	kv.ttl.mu.Unlock()

	return nil
}

// Close, KVStore altyapısını kapatır.
func (kv *KVStore) Close() {
	kv.ttl.Stop()
}

// interface uyumluluğunu derleme zamanında doğrula
var _ raft.StateMachine = (*KVStore)(nil)
