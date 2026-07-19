package kvstore

import (
	"reflect"
	"testing"
	"time"
)

func TestHashStore(t *testing.T) {
	setup := func() (*KVStore, *HashStore, *TTLManager) {
		kv := NewKVStore()
		return kv, kv.hashes, kv.ttl
	}

	t.Run("HappyPath_HSetGet", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("hkey1", "f1", "v1", 0)
		val, err := hs.HGet("hkey1", "f1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "v1" {
			t.Errorf("expected 'v1', got '%v'", val)
		}
	})

	t.Run("HappyPath_HGetAll", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("hkey2", "f1", "v1", 0)
		hs.HSet("hkey2", "f2", "v2", 0)
		
		res, err := hs.HGetAll("hkey2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		
		expected := map[string]string{"f1": "v1", "f2": "v2"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("HappyPath_HExists", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("hkey3", "f1", "v1", 0)
		
		exists, err := hs.HExists("hkey3", "f1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !exists {
			t.Error("expected true, got false")
		}

		exists, err = hs.HExists("hkey3", "f2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if exists {
			t.Error("expected false, got true")
		}
	})

	t.Run("HappyPath_HDelete", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("hkey4", "f1", "v1", 0)
		err := hs.HDelete("hkey4", "f1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, err = hs.HGet("hkey4", "f1")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_HGetNonExistent", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		_, err := hs.HGet("missing_key", "f1")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_HGetAllNonExistent", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		_, err := hs.HGetAll("missing_key")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_StringKeyHSet", func(t *testing.T) {
		t.Parallel()
		kv, hs, _ := setup()
		kv.strings.Set("str_key", "val", 0)
		err := hs.HSet("str_key", "f1", "v1", 0)
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType, got %v", err)
		}
	})

	t.Run("TTLPath_ExpiredHGet", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("ttl_hash1", "f1", "v1", 100*time.Millisecond)
		time.Sleep(150 * time.Millisecond)
		_, err := hs.HGet("ttl_hash1", "f1")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound for expired hash, got %v", err)
		}
	})

	t.Run("TTLPath_UnexpiredHGetAll", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("ttl_hash2", "f1", "v1", 100*time.Millisecond)
		res, err := hs.HGetAll("ttl_hash2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res["f1"] != "v1" {
			t.Errorf("expected 'v1', got '%v'", res["f1"])
		}
	})

	t.Run("TypeConflict_HashKeyLPush", func(t *testing.T) {
		t.Parallel()
		kv, _, _ := setup()
		kv.hashes.HSet("conflict_hash", "f1", "v1", 0)
		_, err := kv.lists.LPush("conflict_hash", 0, "v2")
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType for list push on hash key, got %v", err)
		}
	})

	t.Run("HappyPath_HSetOverwrite", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("hkey5", "f1", "v1", 0)
		hs.HSet("hkey5", "f1", "v2", 0)
		val, _ := hs.HGet("hkey5", "f1")
		if val != "v2" {
			t.Errorf("expected overwritten value 'v2', got '%s'", val)
		}
	})

	t.Run("HappyPath_HDeletePartial", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("hkey6", "f1", "v1", 0)
		hs.HSet("hkey6", "f2", "v2", 0)
		hs.HDelete("hkey6", "f1")
		// f2 should still exist
		exists, _ := hs.HExists("hkey6", "f2")
		if !exists {
			t.Errorf("expected f2 to still exist after f1 deletion")
		}
	})

	t.Run("HappyPath_HDeleteFull", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("hkey_full_delete", "f1", "v1", 0)
		
		// Delete the only field
		err := hs.HDelete("hkey_full_delete", "f1")
		if err != nil {
			t.Fatalf("unexpected error on hdelete: %v", err)
		}
		
		// Should return ErrKeyNotFound since the entire key is logically deleted and removed from store
		_, err = hs.HGetAll("hkey_full_delete")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound since the entire hash should be deleted, got %v", err)
		}
	})

	t.Run("ErrorPath_ErrWrongType_HGet", func(t *testing.T) {
		t.Parallel()
		kv, hs, _ := setup()
		kv.strings.Set("h_wrong1", "val", 0)
		_, err := hs.HGet("h_wrong1", "f1")
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("ErrorPath_ErrWrongType_HDelete", func(t *testing.T) {
		t.Parallel()
		kv, hs, _ := setup()
		kv.strings.Set("h_wrong2", "val", 0)
		err := hs.HDelete("h_wrong2", "f1")
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("ErrorPath_ErrWrongType_HGetAll", func(t *testing.T) {
		t.Parallel()
		kv, hs, _ := setup()
		kv.strings.Set("h_wrong3", "val", 0)
		_, err := hs.HGetAll("h_wrong3")
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("ErrorPath_ErrWrongType_HExists", func(t *testing.T) {
		t.Parallel()
		kv, hs, _ := setup()
		kv.strings.Set("h_wrong4", "val", 0)
		_, err := hs.HExists("h_wrong4", "f1")
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("ErrorPath_HDeleteNonExistentKey", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		err := hs.HDelete("missing_hkey", "f1")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_HDeleteNonExistentField", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("hkey_fdel", "f1", "v1", 0)
		err := hs.HDelete("hkey_fdel", "f2")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
		val, _ := hs.HGet("hkey_fdel", "f1")
		if val != "v1" {
			t.Errorf("expected original field to be retrievable, got %v", val)
		}
	})

	t.Run("TypeRegistry_AfterFullDelete", func(t *testing.T) {
		t.Parallel()
		kv, hs, _ := setup()
		hs.HSet("k_fulldel", "f1", "v1", 0)
		hs.HDelete("k_fulldel", "f1")
		err := kv.strings.Set("k_fulldel", "val", 0)
		if err != nil { t.Errorf("expected no error after full delete, got %v", err) }
	})

	t.Run("HSet_ExpiredKeyIsEvicted", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("k_expire", "f1", "v1", 100*time.Millisecond)
		time.Sleep(150 * time.Millisecond)
		hs.HSet("k_expire", "f2", "v2", 0)

		_, err := hs.HGet("k_expire", "f1")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound for f1, got err=%v", err)
		}

		val, _ := hs.HGet("k_expire", "f2")
		if val != "v2" {
			t.Errorf("expected v2 for f2, got %v", val)
		}
	})

	t.Run("TTL_NotOverwrittenByZero", func(t *testing.T) {
		t.Parallel()
		_, hs, _ := setup()
		hs.HSet("k_no_overwrite", "f1", "v1", 200*time.Millisecond)
		hs.HSet("k_no_overwrite", "f2", "v2", 0)
		
		time.Sleep(250 * time.Millisecond)
		
		_, err := hs.HGet("k_no_overwrite", "f1")
		if err != ErrKeyNotFound { t.Errorf("expected ErrKeyNotFound for f1, got %v", err) }
		
		_, err = hs.HGet("k_no_overwrite", "f2")
		if err != ErrKeyNotFound { t.Errorf("expected ErrKeyNotFound for f2, got %v", err) }
	})
}
