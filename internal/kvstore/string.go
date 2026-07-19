package kvstore

import (
	"strconv"
	"sync"
	"time"
)

// StringStore manages string key-value pairs.
type StringStore struct {
	mu   sync.RWMutex
	data map[string]string
	kv   *KVStore
	ttl  *TTLManager
}

// NewStringStore creates a new StringStore.
func NewStringStore(kv *KVStore, ttl *TTLManager) *StringStore {
	return &StringStore{
		data: make(map[string]string),
		kv:   kv,
		ttl:  ttl,
	}
}

// Set sets the string value of a key.
func (s *StringStore) Set(key, value string, ttl time.Duration) error {
	if s.ttl.IsExpired(key) {
		s.Delete(key)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.kv.CheckAndSetType(key, "string"); err != nil {
		return err
	}
	s.data[key] = value

	// Set TTL via TTLManager if provided
	if ttl > 0 {
		s.ttl.Set(key, ttl)
	}
	return nil
}

// Get gets the value of a key. Returns ErrKeyNotFound if it does not exist or expired.
func (s *StringStore) Get(key string) (string, error) {
	if err := s.kv.CheckType(key, "string"); err != nil {
		return "", err
	}

	if s.ttl.IsExpired(key) {
		s.Delete(key) // Auto-cleanup on read
		return "", ErrKeyNotFound
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	val, exists := s.data[key]

	if !exists {
		return "", ErrKeyNotFound
	}
	return val, nil
}

// Delete removes the specified key.
func (s *StringStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.data[key]
	if exists {
		delete(s.data, key)
	}

	s.ttl.Delete(key)
	s.kv.DeleteType(key)

	if !exists {
		return ErrKeyNotFound
	}
	return nil
}

// Incr increments the number stored at key by 1.
func (s *StringStore) Incr(key string) (int64, error) {
	return s.addValue(key, 1)
}

// Decr decrements the number stored at key by 1.
func (s *StringStore) Decr(key string) (int64, error) {
	return s.addValue(key, -1)
}

func (s *StringStore) addValue(key string, delta int64) (int64, error) {
	expired := s.ttl.IsExpired(key)
	if expired {
		s.Delete(key)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.kv.CheckAndSetType(key, "string"); err != nil {
		return 0, err
	}

	valStr, exists := s.data[key]
	var val int64
	var err error

	if exists {
		val, err = strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return 0, ErrWrongType
		}
	}

	newVal := val + delta
	s.data[key] = strconv.FormatInt(newVal, 10)
	
	// Incrementing operations do not typically change the TTL in Redis,
	// so we leave it as is if it exists. If it didn't exist, it has no TTL.
	return newVal, nil
}
