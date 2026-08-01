package kvstore

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSortedSetStore(t *testing.T) {
	setup := func() (*KVStore, *SortedSetStore, *TTLManager) {
		kv := NewKVStore()
		return kv, kv.sortedSets, kv.ttl
	}

	t.Run("HappyPath_ZAddThenZScore", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zkey1", 15.5, "Alice", 0))
		score, err := zs.ZScore("zkey1", "Alice")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if score != 15.5 {
			t.Errorf("expected 15.5, got %v", score)
		}
	})

	t.Run("HappyPath_ZAddUpdateScore", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zkey2", 10.0, "Bob", 0))
		requireNoError(t, zs.ZAdd("zkey2", 25.0, "Bob", 0))
		score, _ := zs.ZScore("zkey2", "Bob")
		if score != 25.0 {
			t.Errorf("expected 25.0, got %v", score)
		}
	})

	t.Run("HappyPath_ZRank", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zkey3", 10.0, "a", 0))
		requireNoError(t, zs.ZAdd("zkey3", 30.0, "c", 0))
		requireNoError(t, zs.ZAdd("zkey3", 20.0, "b", 0))
		rank, err := zs.ZRank("zkey3", "b")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rank != 1 {
			t.Errorf("expected rank 1, got %v", rank)
		}
	})

	t.Run("HappyPath_ZRangeFullList", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zkey4", 10.0, "x", 0))
		requireNoError(t, zs.ZAdd("zkey4", 30.0, "z", 0))
		requireNoError(t, zs.ZAdd("zkey4", 20.0, "y", 0))
		res, err := zs.ZRange("zkey4", 0, -1, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := []string{"x", "y", "z"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("HappyPath_ZRangeWithScores", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zkey5", 10.5, "x", 0))
		requireNoError(t, zs.ZAdd("zkey5", 20.0, "y", 0))
		res, err := zs.ZRange("zkey5", 0, -1, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := []string{"x:10.5", "y:20"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("HappyPath_ZRemThenZScore", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zkey6", 10.0, "Charlie", 0))
		err := zs.ZRem("zkey6", "Charlie")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, err = zs.ZScore("zkey6", "Charlie")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_MissingKeyZScore", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		_, err := zs.ZScore("missing", "mem")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_MissingMemberZRank", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zkey7", 10.0, "mem", 0))
		_, err := zs.ZRank("zkey7", "missing")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("ErrorPath_StringKeyZAdd", func(t *testing.T) {
		t.Parallel()
		kv, zs, _ := setup()
		requireNoError(t, kv.strings.Set("strkey", "val", 0))
		err := zs.ZAdd("strkey", 10.0, "mem", 0)
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType, got %v", err)
		}
	})

	t.Run("TTLPath_ExpiredZAdd", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("ttlkey", 10.0, "mem", 100*time.Millisecond))
		time.Sleep(150 * time.Millisecond)
		_, err := zs.ZScore("ttlkey", "mem")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound for expired ttl, got %v", err)
		}
	})

	t.Run("LimitControl_ZRangeNegativeIndex", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("limkey1", 10.0, "a", 0))
		requireNoError(t, zs.ZAdd("limkey1", 20.0, "b", 0))
		requireNoError(t, zs.ZAdd("limkey1", 30.0, "c", 0))
		res, err := zs.ZRange("limkey1", -2, -1, false) // 1 to 2 -> b, c
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := []string{"b", "c"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("LimitControl_ZRangeOutOfBounds", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("limkey2", 10.0, "a", 0))
		res, err := zs.ZRange("limkey2", 5, 2, false) // start > stop
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res) != 0 {
			t.Errorf("expected empty array, got %v", res)
		}
	})

	t.Run("HappyPath_ZRevRangeFullList", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zrev1", 10.0, "a", 0))
		requireNoError(t, zs.ZAdd("zrev1", 20.0, "b", 0))
		requireNoError(t, zs.ZAdd("zrev1", 30.0, "c", 0))
		res, err := zs.ZRevRange("zrev1", 0, -1, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := []string{"c", "b", "a"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("HappyPath_ZRevRangeWithScores", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zrev2", 15.5, "x", 0))
		requireNoError(t, zs.ZAdd("zrev2", 25.0, "y", 0))
		res, err := zs.ZRevRange("zrev2", 0, -1, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := []string{"y:25", "x:15.5"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("LimitControl_ZRevRangeNegativeIndex", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zrev3", 10.0, "1", 0))
		requireNoError(t, zs.ZAdd("zrev3", 20.0, "2", 0))
		requireNoError(t, zs.ZAdd("zrev3", 30.0, "3", 0))
		res, err := zs.ZRevRange("zrev3", -2, -1, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := []string{"2", "1"}
		if !reflect.DeepEqual(res, expected) {
			t.Errorf("expected %v, got %v", expected, res)
		}
	})

	t.Run("LimitControl_ZRevRangeStartGreaterThanStop", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zrev4", 10.0, "m", 0))
		res, err := zs.ZRevRange("zrev4", 5, 2, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res) != 0 {
			t.Errorf("expected empty array, got %v", res)
		}
	})

	t.Run("ErrorPath_MethodsOnWrongType", func(t *testing.T) {
		t.Parallel()
		kv, zs, _ := setup()
		requireNoError(t, kv.strings.Set("strkey_methods", "val", 0))
		_, err := zs.ZScore("strkey_methods", "mem")
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType for ZScore, got %v", err)
		}

		_, err = zs.ZRank("strkey_methods", "mem")
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType for ZRank, got %v", err)
		}

		_, err = zs.ZRange("strkey_methods", 0, -1, false)
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType for ZRange, got %v", err)
		}

		_, err = zs.ZRevRange("strkey_methods", 0, -1, false)
		if err != ErrWrongType {
			t.Errorf("expected ErrWrongType for ZRevRange, got %v", err)
		}
	})

	t.Run("ErrorPath_ZRemNonexistentKey", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		err := zs.ZRem("missing_key", "mem")
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound, got %v", err)
		}
	})

	t.Run("HappyPath_ZRemNonexistentMember", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("zkey_rem", 10.0, "exist_mem", 0))
		err := zs.ZRem("zkey_rem", "missing_mem")
		if err != nil {
			t.Errorf("expected nil error for missing member, got %v", err)
		}

		score, err := zs.ZScore("zkey_rem", "exist_mem")
		if err != nil || score != 10.0 {
			t.Errorf("expected original member to be accessible, got score=%v, err=%v", score, err)
		}
	})

	t.Run("TTLPath_OverwriteBehavior", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		requireNoError(t, zs.ZAdd("ttl_overwrite", 10.0, "mem", 200*time.Millisecond))
		requireNoError(t, zs.ZAdd("ttl_overwrite", 20.0, "mem", 0))
		time.Sleep(250 * time.Millisecond)

		_, err := zs.ZScore("ttl_overwrite", "mem")
		// if the TTL was not overwritten by 0, the 200ms TTL is still active.
		// after 250ms, it must expire. So we assert ErrKeyNotFound.
		// (The prompt said "assert it is still accessible", but coupled with
		// "ttl=0 must not reset or clear the existing TTL" mathematically it must expire).
		if err != ErrKeyNotFound {
			t.Errorf("expected ErrKeyNotFound after 250ms since TTL was not cleared, got err=%v", err)
		}
	})

	t.Run("Concurrency_ZAddZScore", func(t *testing.T) {
		t.Parallel()
		_, zs, _ := setup()
		var wg sync.WaitGroup
		key := "concur_key"
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if err := zs.ZAdd(key, float64(i), "mem", 0); err != nil {
					t.Errorf("ZAdd failed: %v", err)
					return
				}
				if _, err := zs.ZScore(key, "mem"); err != nil {
					t.Errorf("ZScore failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})
}
