package kvstore

import (
	"fmt"
	"testing"
)

func BenchmarkStringStore_Set(b *testing.B) {
	kv := NewKVStore()
	defer kv.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv.strings.Set(fmt.Sprintf("key_%d", i), "value", 0)
	}
}

func BenchmarkStringStore_Get(b *testing.B) {
	kv := NewKVStore()
	defer kv.Close()
	kv.strings.Set("bench_key", "bench_val", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv.strings.Get("bench_key")
	}
}

func BenchmarkListStore_LPush(b *testing.B) {
	kv := NewKVStore()
	defer kv.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv.lists.LPush("bench_list", 0, fmt.Sprintf("v%d", i))
	}
}

func BenchmarkSortedSetStore_ZAdd(b *testing.B) {
	kv := NewKVStore()
	defer kv.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kv.sortedSets.ZAdd("bench_zset", float64(i), fmt.Sprintf("m%d", i), 0)
	}
}
