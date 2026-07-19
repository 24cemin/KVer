package raft

import (
	"testing"
)

func BenchmarkRaftLog_Append(b *testing.B) {
	log := newRaftLog()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = log.Append(LogEntry{Index: uint64(i + 1), Term: 1})
	}
}
