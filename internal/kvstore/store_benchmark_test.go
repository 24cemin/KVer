package kvstore

import (
	"fmt"
	"testing"
)

func BenchmarkSnapshotProtobuf(b *testing.B) {
	store := NewKVStore()
	store.strings.mu.Lock()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("value_for_the_key_%d", i)
		store.strings.data[key] = val
	}
	store.strings.mu.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Snapshot(); err != nil {
			b.Fatalf("Snapshot failed: %v", err)
		}
	}
}

func BenchmarkRestoreProtobuf(b *testing.B) {
	store := NewKVStore()
	store.strings.mu.Lock()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("value_for_the_key_%d", i)
		store.strings.data[key] = val
	}
	store.strings.mu.Unlock()

	snapshotData, err := store.Snapshot()
	if err != nil {
		b.Fatalf("Snapshot failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newStore := NewKVStore()
		if err := newStore.Restore(snapshotData); err != nil {
			b.Fatalf("Restore failed: %v", err)
		}
	}
}
