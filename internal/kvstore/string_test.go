package kvstore

import (
	"sync"
	"testing"
	"time"
)

func TestStringStore(t *testing.T) {
	setup := func() (*KVStore, *StringStore, *TTLManager) {
		kv := NewKVStore()
		return kv, kv.strings, kv.ttl
	}

	t.Run("HappyPath_SetGet", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("key1", "val1", 0)
		val, err := ss.Get("key1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "val1" {
			t.Errorf("expected 'val1', got '%v'", val)
		}
	})

	t.Run("HappyPath_Delete", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("key2", "val2", 0)
		err := ss.Delete("key2")
		if err != nil {
			t.Fatalf("unexpected error on delete: %v", err)
		}
		_, err = ss.Get("key2")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("HappyPath_IncrNonExistent", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		val, err := ss.Incr("key_incr1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != 1 {
			t.Errorf("expected 1, got %v", val)
		}
	})

	t.Run("HappyPath_IncrExisting", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("key_incr2", "5", 0)
		val, err := ss.Incr("key_incr2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != 6 {
			t.Errorf("expected 6, got %v", val)
		}
	})

	t.Run("HappyPath_DecrExisting", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("key_decr1", "5", 0)
		val, err := ss.Decr("key_decr1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != 4 {
			t.Errorf("expected 4, got %v", val)
		}
	})

	t.Run("ErrorPath_GetNonExistent", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		_, err := ss.Get("missing_key")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_DeleteNonExistent", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		err := ss.Delete("missing_key")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_IncrWrongType", func(t *testing.T) {
		t.Parallel()
		kv, ss, _ := setup()
		// Önce HSet ile hash yap, sonra Incr dene
		kv.hashes.HSet("wrong_type_key", "f1", "v1", 0)
		_, err := ss.Incr("wrong_type_key")
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType, got %v", err)
		}
	})

	t.Run("TTLPath_ExpiredGet", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("ttl_key1", "val1", 100*time.Millisecond)
		time.Sleep(150 * time.Millisecond)
		_, err := ss.Get("ttl_key1")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound for expired key, got %v", err)
		}
	})

	t.Run("TTLPath_UnexpiredGet", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("ttl_key2", "val2", 100*time.Millisecond)
		val, err := ss.Get("ttl_key2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "val2" {
			t.Errorf("expected 'val2', got '%v'", val)
		}
	})

	t.Run("ErrorPath_DecrNonInteger", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("bad_int", "hello", 0)
		_, err := ss.Decr("bad_int")
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType, got %v", err)
		}
	})

	t.Run("TTLPath_ZeroTTLIsPersistent", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("persistent_key", "val", 0)
		time.Sleep(10 * time.Millisecond)
		val, err := ss.Get("persistent_key")
		if err != nil || val != "val" {
			t.Errorf("expected key to persist, got err %v and val %v", err, val)
		}
	})

	t.Run("Concurrency_Incr", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("concurrent_key", "0", 0)
		
		var wg sync.WaitGroup
		numGoroutines := 100
		wg.Add(numGoroutines)
		
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				_, _ = ss.Incr("concurrent_key")
			}()
		}
		
		wg.Wait()
		val, _ := ss.Get("concurrent_key")
		if val != "100" {
			t.Errorf("expected '100' after %d increments, got '%s'", numGoroutines, val)
		}
	})

	t.Run("TTL_NotOverwrittenByZero", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("k_no_overwrite", "v", 200*time.Millisecond)
		ss.Set("k_no_overwrite", "v2", 0)
		
		time.Sleep(250 * time.Millisecond)
		_, err := ss.Get("k_no_overwrite")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound because TTL should persist, got %v", err)
		}
	})

	t.Run("ErrorPath_SetOnWrongType", func(t *testing.T) {
		t.Parallel()
		kv, ss, _ := setup()
		kv.hashes.HSet("k_override", "f1", "v1", 0)
		err := ss.Set("k_override", "val", 0)
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType, got %v", err)
		}
	})

	t.Run("Incr_ExpiredKeyStartsFromZero", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("k_expired_incr", "5", 100*time.Millisecond)
		time.Sleep(150 * time.Millisecond)
		val, err := ss.Incr("k_expired_incr")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != 1 {
			t.Errorf("expected 1, got %v (expired key should reset to 0 before incr)", val)
		}
	})

	t.Run("ErrorPath_IncrNonInteger", func(t *testing.T) {
		t.Parallel()
		_, ss, _ := setup()
		ss.Set("bad_int_incr", "notanumber", 0)
		_, err := ss.Incr("bad_int_incr")
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType, got %v", err)
		}
	})
}
