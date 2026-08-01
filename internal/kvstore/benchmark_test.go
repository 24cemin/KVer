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
		if err := kv.strings.Set(fmt.Sprintf("key_%d", i), "value", 0); err != nil {
			b.Fatalf("Set failed: %v", err)
		}
	}
}

func BenchmarkStringStore_Get(b *testing.B) {
	kv := NewKVStore()
	defer kv.Close()
	if err := kv.strings.Set("bench_key", "bench_val", 0); err != nil {
		b.Fatalf("benchmark setup failed: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kv.strings.Get("bench_key"); err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

func BenchmarkListStore_LPush(b *testing.B) {
	kv := NewKVStore()
	defer kv.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kv.lists.LPush("bench_list", 0, fmt.Sprintf("v%d", i)); err != nil {
			b.Fatalf("LPush failed: %v", err)
		}
	}
}

func BenchmarkSortedSetStore_ZAdd(b *testing.B) {
	kv := NewKVStore()
	defer kv.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := kv.sortedSets.ZAdd("bench_zset", float64(i), fmt.Sprintf("m%d", i), 0); err != nil {
			b.Fatalf("ZAdd failed: %v", err)
		}
	}
}
