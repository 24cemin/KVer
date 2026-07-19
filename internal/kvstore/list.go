package kvstore

import (
	"container/list"
	"sync"
	"time"
)

// ListStore manages list data structures using a doubly linked list for O(1) LPush/RPush.
type ListStore struct {
	mu   sync.RWMutex
	data map[string]*list.List
	kv   *KVStore
	ttl  *TTLManager
}

// NewListStore creates a new ListStore.
func NewListStore(kv *KVStore, ttl *TTLManager) *ListStore {
	return &ListStore{
		data: make(map[string]*list.List),
		kv:   kv,
		ttl:  ttl,
	}
}

// LPush inserts all the specified values at the head of the list stored at key.
func (l *ListStore) LPush(key string, ttl time.Duration, values ...string) (int64, error) {
	if l.ttl.IsExpired(key) {
		l.deleteKey(key)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.kv.CheckAndSetType(key, "list"); err != nil {
		return 0, err
	}

	ll, exists := l.data[key]
	if !exists {
		ll = list.New()
		l.data[key] = ll
	}

	for _, v := range values {
		ll.PushFront(v)
	}

	if ttl > 0 {
		l.ttl.Set(key, ttl)
	}

	return int64(ll.Len()), nil
}

// RPush inserts all the specified values at the tail of the list stored at key.
func (l *ListStore) RPush(key string, ttl time.Duration, values ...string) (int64, error) {
	if l.ttl.IsExpired(key) {
		l.deleteKey(key)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.kv.CheckAndSetType(key, "list"); err != nil {
		return 0, err
	}

	ll, exists := l.data[key]
	if !exists {
		ll = list.New()
		l.data[key] = ll
	}

	for _, v := range values {
		ll.PushBack(v)
	}

	if ttl > 0 {
		l.ttl.Set(key, ttl)
	}

	return int64(ll.Len()), nil
}

// LPop removes and returns the first element of the list stored at key.
func (l *ListStore) LPop(key string) (string, error) {
	if err := l.kv.CheckType(key, "list"); err != nil {
		return "", err
	}

	if l.ttl.IsExpired(key) {
		l.deleteKey(key)
		return "", ErrKeyNotFound
	}

	l.mu.Lock()

	ll, exists := l.data[key]
	if !exists {
		l.mu.Unlock()
		return "", ErrKeyNotFound
	}

	if ll.Len() == 0 {
		l.mu.Unlock()
		return "", ErrEmptyList
	}

	e := ll.Front()
	val := e.Value.(string)
	ll.Remove(e)

	isEmpty := ll.Len() == 0
	if isEmpty {
		delete(l.data, key)
	}
	l.mu.Unlock()

	if isEmpty {
		l.ttl.Delete(key)
		l.kv.DeleteType(key)
	}

	return val, nil
}

// RPop removes and returns the last element of the list stored at key.
func (l *ListStore) RPop(key string) (string, error) {
	if err := l.kv.CheckType(key, "list"); err != nil {
		return "", err
	}

	if l.ttl.IsExpired(key) {
		l.deleteKey(key)
		return "", ErrKeyNotFound
	}

	l.mu.Lock()

	ll, exists := l.data[key]
	if !exists {
		l.mu.Unlock()
		return "", ErrKeyNotFound
	}

	if ll.Len() == 0 {
		l.mu.Unlock()
		return "", ErrEmptyList
	}

	e := ll.Back()
	val := e.Value.(string)
	ll.Remove(e)

	isEmpty := ll.Len() == 0
	if isEmpty {
		delete(l.data, key)
	}
	l.mu.Unlock()

	if isEmpty {
		l.ttl.Delete(key)
		l.kv.DeleteType(key)
	}

	return val, nil
}

// LRange returns the specified elements of the list stored at key.
func (l *ListStore) LRange(key string, start, stop int) ([]string, error) {
	if err := l.kv.CheckType(key, "list"); err != nil {
		return nil, err
	}

	if l.ttl.IsExpired(key) {
		l.deleteKey(key)
		return nil, ErrKeyNotFound
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	ll, exists := l.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	length := ll.Len()
	if length == 0 {
		return nil, ErrEmptyList
	}

	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop = length + stop
		if stop < 0 {
			stop = 0
		}
	}

	if start >= length {
		return []string{}, nil
	}

	if stop >= length {
		stop = length - 1
	}

	if start > stop {
		return []string{}, nil
	}

	res := make([]string, 0, stop-start+1)
	
	curr := ll.Front()
	for i := 0; i <= stop; i++ {
		if i >= start {
			res = append(res, curr.Value.(string))
		}
		curr = curr.Next()
	}

	return res, nil
}

// LLen returns the length of the list stored at key.
func (l *ListStore) LLen(key string) (int64, error) {
	if err := l.kv.CheckType(key, "list"); err != nil {
		return 0, err
	}

	if l.ttl.IsExpired(key) {
		l.deleteKey(key)
		return 0, ErrKeyNotFound
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	ll, exists := l.data[key]
	if !exists {
		return 0, ErrKeyNotFound
	}

	return int64(ll.Len()), nil
}

// deleteKey is an internal helper to completely remove a key.
func (l *ListStore) deleteKey(key string) {
	l.mu.Lock()
	delete(l.data, key)
	l.mu.Unlock()

	l.ttl.Delete(key)
	l.kv.DeleteType(key)
}
