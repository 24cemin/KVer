package kvstore

import (
	"sync"
	"time"
)

// HashStore manages hash maps.
type HashStore struct {
	mu   sync.RWMutex
	data map[string]map[string]string
	kv   *KVStore
	ttl  *TTLManager
}

// NewHashStore creates a new HashStore.
func NewHashStore(kv *KVStore, ttl *TTLManager) *HashStore {
	return &HashStore{
		data: make(map[string]map[string]string),
		kv:   kv,
		ttl:  ttl,
	}
}

// HSet sets field in the hash stored at key to value.
func (h *HashStore) HSet(key, field, value string, ttl time.Duration) error {
	if h.ttl.IsExpired(key) {
		h.deleteKey(key)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.kv.CheckAndSetType(key, "hash"); err != nil {
		return err
	}
	if _, exists := h.data[key]; !exists {
		h.data[key] = make(map[string]string)
	}
	h.data[key][field] = value

	if ttl > 0 {
		h.ttl.Set(key, ttl)
	}
	return nil
}

// HGet returns the value associated with field in the hash stored at key.
func (h *HashStore) HGet(key, field string) (string, error) {
	if err := h.kv.CheckType(key, "hash"); err != nil {
		return "", err
	}

	if h.ttl.IsExpired(key) {
		h.deleteKey(key)
		return "", ErrKeyNotFound
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	hash, exists := h.data[key]
	if !exists {
		return "", ErrKeyNotFound
	}

	val, fieldExists := hash[field]
	if !fieldExists {
		return "", ErrKeyNotFound
	}

	return val, nil
}

// HDelete removes the specified fields from the hash stored at key.
// Returns an error if the key does not exist.
func (h *HashStore) HDelete(key, field string) error {
	if err := h.kv.CheckType(key, "hash"); err != nil {
		return err
	}

	if h.ttl.IsExpired(key) {
		h.deleteKey(key)
		return ErrKeyNotFound
	}

	h.mu.Lock()

	hash, exists := h.data[key]
	if !exists {
		h.mu.Unlock()
		return ErrKeyNotFound
	}

	if _, fieldExists := hash[field]; !fieldExists {
		h.mu.Unlock()
		return ErrKeyNotFound
	}

	delete(hash, field)
	isEmpty := len(hash) == 0
	if isEmpty {
		delete(h.data, key)
	}
	h.mu.Unlock()

	if isEmpty {
		h.ttl.Delete(key) // fully remove logically
		h.kv.DeleteType(key)
	}

	return nil
}

// HGetAll returns all fields and values of the hash stored at key.
func (h *HashStore) HGetAll(key string) (map[string]string, error) {
	if err := h.kv.CheckType(key, "hash"); err != nil {
		return nil, err
	}

	if h.ttl.IsExpired(key) {
		h.deleteKey(key)
		return nil, ErrKeyNotFound
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	hash, exists := h.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	// Return a copy to prevent race conditions on the map
	res := make(map[string]string, len(hash))
	for k, v := range hash {
		res[k] = v
	}

	return res, nil
}

// HExists returns a boolean indicating if field is an existing field in the hash stored at key.
func (h *HashStore) HExists(key, field string) (bool, error) {
	if err := h.kv.CheckType(key, "hash"); err != nil {
		return false, err
	}

	if h.ttl.IsExpired(key) {
		h.deleteKey(key)
		return false, ErrKeyNotFound
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	hash, exists := h.data[key]
	if !exists {
		return false, ErrKeyNotFound
	}

	_, fieldExists := hash[field]
	return fieldExists, nil
}

// deleteKey is an internal helper to completely remove a key.
func (h *HashStore) deleteKey(key string) {
	h.mu.Lock()
	delete(h.data, key)
	h.mu.Unlock()
	
	h.ttl.Delete(key)
	h.kv.DeleteType(key)
}
