package raft

import (
	"testing"

	kvpb "github.com/emin/kver/proto/kv/gen"
	"google.golang.org/protobuf/proto"
)

// BenchmarkWALPersistProtobuf measures the time it takes to persist 1000 log entries using Protobuf.
func BenchmarkWALPersistProtobuf(b *testing.B) {
	payload := &kvpb.CommandPayload{Op: "SET", Args: []string{"bench_key", "bench_value", "0"}}
	cmdData, _ := proto.Marshal(payload)

	var entries []LogEntry
	for i := 0; i < 1000; i++ {
		entries = append(entries, LogEntry{
			Index:   uint64(i + 1),
			Term:    1,
			Type:    EntryKV,
			Command: cmdData,
		})
	}

	dir := b.TempDir()
	log, _ := newRaftLogWithWAL(dir, "bench", false)
	log.entries = entries

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = log.persistToDisk()
	}
}

// BenchmarkWALReadProtobuf measures the time it takes to read 1000 log entries using Protobuf.
func BenchmarkWALReadProtobuf(b *testing.B) {
	payload := &kvpb.CommandPayload{Op: "SET", Args: []string{"bench_key", "bench_value", "0"}}
	cmdData, _ := proto.Marshal(payload)

	var entries []LogEntry
	for i := 0; i < 1000; i++ {
		entries = append(entries, LogEntry{
			Index:   uint64(i + 1),
			Term:    1,
			Type:    EntryKV,
			Command: cmdData,
		})
	}

	dir := b.TempDir()
	log, _ := newRaftLogWithWAL(dir, "bench", false)
	log.entries = entries
	_ = log.persistToDisk() // create file

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = log.loadFromDisk()
	}
}
