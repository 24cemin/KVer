package kvstore

import (
	"testing"
	"time"
)

func TestTTLManager(t *testing.T) {
	t.Run("IsExpired_TrueForExpiredKey", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		tm.Set("key1", 50*time.Millisecond)
		time.Sleep(100 * time.Millisecond)
		if !tm.IsExpired("key1") {
			t.Errorf("expected key1 to be expired")
		}
	})

	t.Run("IsExpired_FalseForNoTTLKey", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		if tm.IsExpired("key2") {
			t.Errorf("expected key2 (not set) to not be expired")
		}
	})

	t.Run("Delete_RemovesTTLRecord", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		tm.Set("key3", 1*time.Hour)
		tm.Delete("key3")
		if tm.IsExpired("key3") {
			t.Errorf("expected key3 to not be expired after delete")
		}
		// Confirm it doesn't return a TTL duration
		if _, ok := tm.Get("key3"); ok {
			t.Errorf("expected no TTL record for key3 after delete")
		}
	})

	t.Run("IsExpired_FalseForUnexpiredKey", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		tm.Set("key4", 1*time.Hour)
		if tm.IsExpired("key4") {
			t.Errorf("expected key4 to not be expired immediately after set")
		}
	})

	t.Run("CleanupGoroutine_RemovesExpiredKeys", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		tm.Start(50 * time.Millisecond)
		defer tm.Stop()

		tm.Set("key5", 100*time.Millisecond)
		time.Sleep(200 * time.Millisecond) // Wait for expiration and cleanup tick

		// The key should ideally be removed from the map internally.
		// Since we don't expose the map, we check via Get.
		if _, ok := tm.Get("key5"); ok {
			t.Errorf("expected key5 to be cleaned up and return false")
		}
	})

	t.Run("Set_NegativeTTLMakesPersistent", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		tm.Set("key6", 1*time.Hour)
		tm.Set("key6", -1*time.Minute) // Setting to negative should remove TTL and make it persistent
		if tm.IsExpired("key6") {
			t.Errorf("expected key6 to become persistent and not expired")
		}
		if _, ok := tm.Get("key6"); ok {
			t.Errorf("expected no TTL record for key6 after negative set")
		}
	})

	t.Run("Stop_DoubleCallNoPanic", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Stop() panicked on double call: %v", r)
			}
		}()
		tm := NewTTLManager(nil)
		tm.Start(50 * time.Millisecond)
		tm.Stop()
		tm.Stop() // Should not panic
	})

	t.Run("Start_MultipleCallsSingleGoroutine", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		tm.Start(50 * time.Millisecond)
		tm.Start(50 * time.Millisecond) // Should not spawn duplicate leaks
		tm.Start(50 * time.Millisecond)

		tm.Set("leak_test", 100*time.Millisecond)
		time.Sleep(200 * time.Millisecond)

		if _, ok := tm.Get("leak_test"); ok {
			t.Errorf("expected key to be cleaned up by the single running goroutine")
		}
		tm.Stop()
	})
}

func TestTTLManager_SetAbsolute(t *testing.T) {
	t.Run("PastTimestamp_IsExpired", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		past := time.Now().Add(-1 * time.Hour).UnixMilli()
		tm.SetAbsolute("k", past)
		if !tm.IsExpired("k") {
			t.Error("expected key with past timestamp to be expired")
		}
	})

	t.Run("FutureTimestamp_NotExpired", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		future := time.Now().Add(1 * time.Hour).UnixMilli()
		tm.SetAbsolute("k", future)
		if tm.IsExpired("k") {
			t.Error("expected key with future timestamp to not be expired")
		}
		if _, ok := tm.Get("k"); !ok {
			t.Error("expected TTL record to exist for future key")
		}
	})

	t.Run("ZeroTimestamp_NoTTL", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		tm.SetAbsolute("k", 0)
		if tm.IsExpired("k") {
			t.Error("expected key with zero timestamp to not be expired (no TTL)")
		}
		if _, ok := tm.Get("k"); ok {
			t.Error("expected no TTL record for zero timestamp")
		}
	})

	t.Run("NegativeTimestamp_NoTTL", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		tm.SetAbsolute("k", -1000)
		if tm.IsExpired("k") {
			t.Error("expected key with negative timestamp to not be expired (no TTL)")
		}
		if _, ok := tm.Get("k"); ok {
			t.Error("expected no TTL record for negative timestamp")
		}
	})

	t.Run("OverwritesExistingTTL", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		// Set a past TTL first
		tm.SetAbsolute("k", time.Now().Add(-1*time.Hour).UnixMilli())
		if !tm.IsExpired("k") {
			t.Fatal("pre-condition: expected expired")
		}
		// Overwrite with future TTL
		tm.SetAbsolute("k", time.Now().Add(1*time.Hour).UnixMilli())
		if tm.IsExpired("k") {
			t.Error("expected key to not be expired after overwrite with future timestamp")
		}
	})

	t.Run("ZeroTimestamp_ClearsExistingTTL", func(t *testing.T) {
		t.Parallel()
		tm := NewTTLManager(nil)
		// Set a real TTL first
		tm.Set("k", 1*time.Hour)
		if _, ok := tm.Get("k"); !ok {
			t.Fatal("pre-condition: expected TTL to exist")
		}
		// Clear it with zero absolute timestamp
		tm.SetAbsolute("k", 0)
		if _, ok := tm.Get("k"); ok {
			t.Error("expected TTL to be cleared after SetAbsolute(0)")
		}
	})
}
