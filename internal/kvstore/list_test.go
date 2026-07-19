package kvstore

import (
	"reflect"
	"testing"
	"time"
)

func TestListStore(t *testing.T) {
	setup := func() (*KVStore, *ListStore, *TTLManager) {
		kv := NewKVStore()
		return kv, kv.lists, kv.ttl
	}

	t.Run("HappyPath_LPushLLen", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ln, err := ls.LPush("lkey1", 0, "val1", "val2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ln != 2 {
			t.Errorf("expected length 2, got %d", ln)
		}

		ln2, err := ls.LLen("lkey1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ln2 != 2 {
			t.Errorf("expected LLen 2, got %d", ln2)
		}
	})

	t.Run("HappyPath_RPushLRange", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("lkey2", 0, "a", "b", "c")
		res, err := ls.LRange("lkey2", 0, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := []string{"a", "b", "c"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("HappyPath_LPop", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("lkey3", 0, "first", "second")
		val, err := ls.LPop("lkey3")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "first" {
			t.Errorf("expected 'first', got '%s'", val)
		}
	})

	t.Run("HappyPath_RPop", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("lkey4", 0, "first", "second")
		val, err := ls.RPop("lkey4")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "second" {
			t.Errorf("expected 'second', got '%s'", val)
		}
	})

	t.Run("HappyPath_LRangeFullList", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("lkey5", 0, "x", "y", "z")
		res, err := ls.LRange("lkey5", 0, -1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := []string{"x", "y", "z"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("HappyPath_LRangeSpecific", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("lkey6", 0, "0", "1", "2", "3", "4")
		res, err := ls.LRange("lkey6", 1, 3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := []string{"1", "2", "3"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("ErrorPath_LPopEmpty", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("empty_test1", 0, "a")
		ls.LPop("empty_test1") // list is now empty
		
		_, err := ls.LPop("empty_test1")
		if err != ErrKeyNotFound { // Since it deletes key when empty
			// Actually, if it's completely missing, our code returns ErrKeyNotFound
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}

		// What if we try LPop on a truly missing key directly?
		_, err = ls.LPop("missing_key")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_RPopEmpty", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		_, err := ls.RPop("missing_key2")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_LLenNonExistent", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		_, err := ls.LLen("missing_key3")
		// Our implementation returns ErrKeyNotFound for non-existent list keys
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_StringKeyLPush", func(t *testing.T) {
		t.Parallel()
		kv, ls, _ := setup()
		kv.strings.Set("strkey", "abc", 0)
		_, err := ls.LPush("strkey", 0, "a")
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType, got %v", err)
		}
	})

	t.Run("ErrorPath_HashKeyLPush", func(t *testing.T) {
		t.Parallel()
		kv, ls, _ := setup()
		kv.hashes.HSet("hashkey", "f", "v", 0)
		_, err := ls.LPush("hashkey", 0, "a")
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType, got %v", err)
		}
	})

	t.Run("TTLPath_ExpiredLLen", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.LPush("ttl_list1", 100*time.Millisecond, "a")
		
		time.Sleep(150 * time.Millisecond)
		_, err := ls.LLen("ttl_list1")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound for expired list, got %v", err)
		}
	})

	t.Run("TTLPath_UnexpiredLRange", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.LPush("ttl_list2", 100*time.Millisecond, "a")
		
		res, err := ls.LRange("ttl_list2", 0, -1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res) != 1 || res[0] != "a" {
			t.Errorf("expected ['a'], got %v", res)
		}
	})

	t.Run("LimitControl_LRangeOutOfBounds", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("limit_list", 0, "x")
		
		res, err := ls.LRange("limit_list", 5, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res) != 0 {
			t.Errorf("expected empty result safely, got %v", res)
		}
	})

	t.Run("LimitControl_LRangeNegativeIndices", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("neg_list", 0, "a", "b", "c")
		res, _ := ls.LRange("neg_list", 0, -2) // essentially 0 to len-2 -> 0 to 1 -> [a, b]
		expected := []string{"a", "b"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("LimitControl_LRangeStartGreaterThanStop", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("rev_list", 0, "a", "b")
		res, err := ls.LRange("rev_list", 2, 1) // start > stop
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res) != 0 {
			t.Errorf("expected empty list for start > stop, got %v", res)
		}
	})

	t.Run("HappyPath_LPushOrderVerify", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.LPush("lkey_order", 0, "val1", "val2", "val3")
		
		res, err := ls.LRange("lkey_order", 0, -1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		
		expected := []string{"val3", "val2", "val1"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v for push order, got %v", expected, res)
		}
	})

	t.Run("TypeRegistry_AfterFullPop", func(t *testing.T) {
		t.Parallel()
		kv, ls, _ := setup()
		ls.RPush("k_fullpop", 0, "a")
		ls.LPop("k_fullpop")
		err := kv.strings.Set("k_fullpop", "val", 0)
		if err != nil { t.Errorf("expected no error after full pop, got %v", err) }
	})

	t.Run("TypeRegistry_SetType", func(t *testing.T) {
		t.Parallel()
		kv, ls, _ := setup()
		ls.RPush("k_settype", 0, "a")
		err := kv.hashes.HSet("k_settype", "f1", "v1", 0)
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("ErrorPath_ErrWrongType_RPush", func(t *testing.T) {
		t.Parallel()
		kv, ls, _ := setup()
		kv.strings.Set("k_wrong_rpush", "val", 0)
		_, err := ls.RPush("k_wrong_rpush", 0, "a")
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("ErrorPath_ErrWrongType_LPop", func(t *testing.T) {
		t.Parallel()
		kv, ls, _ := setup()
		kv.strings.Set("k_wrong_lpop", "val", 0)
		_, err := ls.LPop("k_wrong_lpop")
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("ErrorPath_ErrWrongType_RPop", func(t *testing.T) {
		t.Parallel()
		kv, ls, _ := setup()
		kv.strings.Set("k_wrong_rpop", "val", 0)
		_, err := ls.RPop("k_wrong_rpop")
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("ErrorPath_ErrWrongType_LRange", func(t *testing.T) {
		t.Parallel()
		kv, ls, _ := setup()
		kv.strings.Set("k_wrong_lrange", "val", 0)
		_, err := ls.LRange("k_wrong_lrange", 0, -1)
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("ErrorPath_ErrWrongType_LLen", func(t *testing.T) {
		t.Parallel()
		kv, ls, _ := setup()
		kv.strings.Set("k_wrong_llen", "val", 0)
		_, err := ls.LLen("k_wrong_llen")
		if err != ErrWrongType { t.Errorf("expected ErrWrongType, got %v", err) }
	})

	t.Run("TTLPath_LPushTTL", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.LPush("ttl_lpush", 100*time.Millisecond, "a")
		time.Sleep(150 * time.Millisecond)
		_, err := ls.LLen("ttl_lpush")
		if err != ErrKeyNotFound { t.Errorf("expected ErrKeyNotFound, got %v", err) }
	})

	t.Run("TTLPath_RPushTTL", func(t *testing.T) {
		t.Parallel()
		_, ls, _ := setup()
		ls.RPush("ttl_rpush", 100*time.Millisecond, "a")
		time.Sleep(150 * time.Millisecond)
		_, err := ls.LRange("ttl_rpush", 0, -1)
		if err != ErrKeyNotFound { t.Errorf("expected ErrKeyNotFound, got %v", err) }
	})
}
