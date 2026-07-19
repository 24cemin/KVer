package kvstore

import (
	"sync"
	"time"
)

// TTLManager manages expiration times for keys.
type TTLManager struct {
	mu        sync.RWMutex
	expiry    map[string]time.Time
	stopCh    chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	onExpire  func(key string) // called outside the lock when a key expires
}

// NewTTLManager creates a new TTLManager.
// onExpire is an optional callback invoked (outside the TTL lock) for each key
// that expires during background cleanup. Pass nil if no callback is needed.
func NewTTLManager(onExpire func(key string)) *TTLManager {
	return &TTLManager{
		expiry:   make(map[string]time.Time),
		stopCh:   make(chan struct{}),
		onExpire: onExpire,
	}
}

// Set sets the expiration for a key relative to the current time.
// If ttl is <= 0, the key has no expiration (removes any existing ttl).
func (tm *TTLManager) Set(key string, ttl time.Duration) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if ttl <= 0 {
		delete(tm.expiry, key)
		return
	}
	tm.expiry[key] = time.Now().Add(ttl)
}

// Get returns the remaining duration and true if the key has an expiration.
// If the key doesn't have an expiration or is expired, it returns 0, false.
func (tm *TTLManager) Get(key string) (time.Duration, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	exp, ok := tm.expiry[key]
	if !ok {
		return 0, false
	}
	rem := time.Until(exp)
	if rem <= 0 {
		return 0, false
	}
	return rem, true
}

// SetAbsolute sets the expiration for a key using an absolute Unix millisecond timestamp.
// If expiryUnixMilli is <= 0, any existing TTL for the key is removed (key becomes persistent).
// This method is used when applying Raft log entries, where the timestamp was computed
// once at the leader to ensure all nodes expire the key at the same wall-clock time.
func (tm *TTLManager) SetAbsolute(key string, expiryUnixMilli int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if expiryUnixMilli <= 0 {
		delete(tm.expiry, key)
		return
	}
	tm.expiry[key] = time.UnixMilli(expiryUnixMilli)
}

// Delete removes the TTL info for a key.
func (tm *TTLManager) Delete(key string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.expiry, key)
}

// IsExpired checks if a key has expired.
// If the key is not tracked, it is deemed NOT expired (returns false).
func (tm *TTLManager) IsExpired(key string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	exp, ok := tm.expiry[key]
	if !ok {
		return false
	}
	return time.Now().After(exp)
}

// Start launches a background goroutine to clean up expired keys periodically.
func (tm *TTLManager) Start(cleanupInterval time.Duration) {
	tm.startOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(cleanupInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					tm.cleanup()
				case <-tm.stopCh:
					return
				}
			}
		}()
	})
}

// Stop halts the background cleanup goroutine.
func (tm *TTLManager) Stop() {
	tm.stopOnce.Do(func() {
		close(tm.stopCh)
	})
}

func (tm *TTLManager) cleanup() {
	tm.mu.Lock()
	now := time.Now()
	var expired []string
	for k, exp := range tm.expiry {
		if now.After(exp) {
			delete(tm.expiry, k)
			expired = append(expired, k)
		}
	}
	tm.mu.Unlock()

	// Invoke the callback outside the lock to avoid lock-order inversion
	// with the KVStore's own mutexes.
	if tm.onExpire != nil {
		for _, k := range expired {
			tm.onExpire(k)
		}
	}
}
