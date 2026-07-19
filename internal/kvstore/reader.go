// Package kvstore — reader.go
// KVReader, okuma operasyonları için server katmanına sunulan interface'dir.
// server paketi KVStore'u doğrudan import etmez; bu arayüz üzerinden konuşur.
// Yazma işlemleri Raft üzerinden gelir (Apply), okuma işlemleri buradan yapılır.
package kvstore

// KVReader, KVStore'un okuma metodlarını soyutlar.
// Mimari kural: Handler katmanı sadece bu interface'i görür — KVStore'un iç yapısını bilmez.
type KVReader interface {
	// String
	Get(key string) (string, error)

	// Hash
	HGet(key, field string) (string, error)
	HGetAll(key string) (map[string]string, error)
	HExists(key, field string) (bool, error)

	// List
	LRange(key string, start, stop int) ([]string, error)
	LLen(key string) (int64, error)

	// Sorted Set
	ZScore(key, member string) (float64, error)
	ZRank(key, member string) (int, error)
	ZRange(key string, start, stop int, withScores bool) ([]string, error)
	ZRevRange(key string, start, stop int, withScores bool) ([]string, error)
}

// Compile-time check: KVStore interface'ini sağlamalı.
var _ KVReader = (*KVStore)(nil)

// ─── KVStore KVReader implementasyonu ────────────────────────────────────────
// KVStore'un iç sub-store metodlarını dışa açan thin wrappers.

func (kv *KVStore) Get(key string) (string, error) {
	return kv.strings.Get(key)
}

func (kv *KVStore) HGet(key, field string) (string, error) {
	return kv.hashes.HGet(key, field)
}

func (kv *KVStore) HGetAll(key string) (map[string]string, error) {
	return kv.hashes.HGetAll(key)
}

func (kv *KVStore) HExists(key, field string) (bool, error) {
	return kv.hashes.HExists(key, field)
}

func (kv *KVStore) LRange(key string, start, stop int) ([]string, error) {
	return kv.lists.LRange(key, start, stop)
}

func (kv *KVStore) LLen(key string) (int64, error) {
	return kv.lists.LLen(key)
}

func (kv *KVStore) ZScore(key, member string) (float64, error) {
	return kv.sortedSets.ZScore(key, member)
}

func (kv *KVStore) ZRank(key, member string) (int, error) {
	return kv.sortedSets.ZRank(key, member)
}

func (kv *KVStore) ZRange(key string, start, stop int, withScores bool) ([]string, error) {
	return kv.sortedSets.ZRange(key, start, stop, withScores)
}

func (kv *KVStore) ZRevRange(key string, start, stop int, withScores bool) ([]string, error) {
	return kv.sortedSets.ZRevRange(key, start, stop, withScores)
}
