package kvstore

import (
	"fmt"
	"sync"
	"time"
)

// SortedSetStore, Redis sorted set benzeri (ZADD/ZRANK/ZSCORE/ZRANGE/ZREVRANGE)
// operasyonlarını yönetir.
type SortedSetStore struct {
	mu   sync.RWMutex
	data map[string]*skipList // key → skip list
	kv   *KVStore
	ttl  *TTLManager
}

// NewSortedSetStore, yeni bir SortedSetStore oluşturur.
func NewSortedSetStore(kv *KVStore, ttl *TTLManager) *SortedSetStore {
	return &SortedSetStore{
		data: make(map[string]*skipList),
		kv:   kv,
		ttl:  ttl,
	}
}

// deleteKeyLocked assumes the lock is already held by the caller.
func (s *SortedSetStore) deleteKeyLocked(key string) {
	delete(s.data, key)
}

// deleteKey tamamen anahtarı temizler.
func (s *SortedSetStore) deleteKey(key string) {
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
	
	s.ttl.Delete(key)
	s.kv.DeleteType(key)
}

// ZAdd, key sorted set'ine (score, member) çifti ekler.
func (s *SortedSetStore) ZAdd(key string, score float64, member string, ttl time.Duration) error {
	if s.ttl.IsExpired(key) {
		s.deleteKey(key)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.kv.CheckAndSetType(key, "zset"); err != nil {
		return err
	}

	sl, exists := s.data[key]
	if !exists {
		sl = newSkipList()
		s.data[key] = sl
	}

	sl.insert(score, member)
	
	if ttl > 0 {
		s.ttl.Set(key, ttl)
	}

	return nil
}

// ZScore, key sorted set'inde member'ın skorunu döndürür.
func (s *SortedSetStore) ZScore(key, member string) (float64, error) {
	if err := s.kv.CheckType(key, "zset"); err != nil {
		return 0, err
	}

	if s.ttl.IsExpired(key) {
		s.deleteKey(key)
		return 0, ErrKeyNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sl, exists := s.data[key]
	if !exists {
		return 0, ErrKeyNotFound
	}

	score, ok := sl.search(member)
	if !ok {
		return 0, ErrKeyNotFound
	}

	return score, nil
}

// ZRank, key sorted set'inde member'ın sırasını (0-based, artan) döndürür.
func (s *SortedSetStore) ZRank(key, member string) (int, error) {
	if err := s.kv.CheckType(key, "zset"); err != nil {
		return 0, err
	}

	if s.ttl.IsExpired(key) {
		s.deleteKey(key)
		return 0, ErrKeyNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sl, exists := s.data[key]
	if !exists {
		return 0, ErrKeyNotFound
	}

	rank, ok := sl.rank(member)
	if !ok {
		return 0, ErrKeyNotFound
	}

	return rank, nil
}

// ZRange, key sorted set'inin [start, stop] sırasındaki üyelerini döndürür.
func (s *SortedSetStore) ZRange(key string, start, stop int, withScores bool) ([]string, error) {
	if err := s.kv.CheckType(key, "zset"); err != nil {
		return nil, err
	}

	if s.ttl.IsExpired(key) {
		s.deleteKey(key)
		return nil, ErrKeyNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sl, exists := s.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	length := sl.length
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

	if start > stop || start >= length {
		return []string{}, nil
	}

	nodes := sl.rangeByRank(start, stop)
	var res []string
	for _, n := range nodes {
		if withScores {
			res = append(res, fmt.Sprintf("%s:%g", n.member, n.score))
		} else {
			res = append(res, n.member)
		}
	}

	return res, nil
}

// ZRevRange, ZRange'in tersten versiyonu.
func (s *SortedSetStore) ZRevRange(key string, start, stop int, withScores bool) ([]string, error) {
	if err := s.kv.CheckType(key, "zset"); err != nil {
		return nil, err
	}

	if s.ttl.IsExpired(key) {
		s.deleteKey(key)
		return nil, ErrKeyNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sl, exists := s.data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}

	length := sl.length
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

	if start > stop || start >= length {
		return []string{}, nil
	}

	nodes := sl.revRangeByRank(start, stop)
	var res []string
	
	for _, n := range nodes {
		if withScores {
			res = append(res, fmt.Sprintf("%s:%g", n.member, n.score))
		} else {
			res = append(res, n.member)
		}
	}

	return res, nil
}

// ZRem, key sorted set'inden member'ı kaldırır.
func (s *SortedSetStore) ZRem(key, member string) error {
	if err := s.kv.CheckType(key, "zset"); err != nil {
		return err
	}

	if s.ttl.IsExpired(key) {
		s.deleteKey(key)
		return ErrKeyNotFound
	}

	s.mu.Lock()

	if s.ttl.IsExpired(key) {
		s.deleteKeyLocked(key)
		s.mu.Unlock()
		s.ttl.Delete(key)
		s.kv.DeleteType(key)
		return ErrKeyNotFound
	}

	sl, exists := s.data[key]
	if !exists {
		s.mu.Unlock()
		return ErrKeyNotFound
	}

	ok := sl.delete(member)
	if !ok {
		// Return nil if the member doesn't exist but the key does.
		s.mu.Unlock()
		return nil
	}

	// Clean up if it was the last element
	isEmpty := sl.length == 0
	if isEmpty {
		delete(s.data, key)
	}
	s.mu.Unlock()
	
	if isEmpty {
		s.ttl.Delete(key)
		s.kv.DeleteType(key)
	}

	return nil
}
