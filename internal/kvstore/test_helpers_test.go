package kvstore

import (
	"testing"
	"time"
)

func requireNoError(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatalf("unexpected error: %v", err)
	}
}

func requireLPush(tb testing.TB, store *ListStore, key string, ttl time.Duration, values ...string) int64 {
	tb.Helper()
	count, err := store.LPush(key, ttl, values...)
	requireNoError(tb, err)
	return count
}

func requireRPush(tb testing.TB, store *ListStore, key string, ttl time.Duration, values ...string) int64 {
	tb.Helper()
	count, err := store.RPush(key, ttl, values...)
	requireNoError(tb, err)
	return count
}

func requireLPop(tb testing.TB, store *ListStore, key string) string {
	tb.Helper()
	value, err := store.LPop(key)
	requireNoError(tb, err)
	return value
}
