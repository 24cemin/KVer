package benchmark

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/proto"
	raftpb "github.com/emin/kver/proto/raft/gen"
)

func BenchmarkJSONSerialization(b *testing.B) {
	entry := map[string]interface{}{
		"Term":    10,
		"Index":   100,
		"Type":    0,
		"Command": []byte("sample_payload_data_for_testing"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := json.Marshal(entry)
		if err != nil {
			b.Fatal(err)
		}
		var decoded map[string]interface{}
		err = json.Unmarshal(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProtobufSerialization(b *testing.B) {
	entry := &raftpb.LogEntry{
		Term:    10,
		Index:   100,
		Type:    raftpb.EntryType_ENTRY_KV,
		Command: []byte("sample_payload_data_for_testing"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := proto.Marshal(entry)
		if err != nil {
			b.Fatal(err)
		}
		var decoded raftpb.LogEntry
		err = proto.Unmarshal(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
