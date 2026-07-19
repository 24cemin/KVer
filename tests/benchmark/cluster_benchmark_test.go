package benchmark

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/emin/kver/pkg/sdk"
)

// To run this benchmark against the Docker cluster:
// go test -bench=. -benchtime=5s ./tests/benchmark/

var client *sdk.Client

func init() {
	// Connect to the local cluster
	nodes := []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}
	client = sdk.NewClient(nodes)
}

// BenchmarkSet measures the latency and throughput of SET (Write) operations.
func BenchmarkSet(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		val := "bench_value"
		if err := client.Set(key, val, 0); err != nil {
			b.Fatalf("Set failed: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	// Pre-populate some data
	if err := client.Set("bench_read_key", "bench_value", 0); err != nil {
		b.Fatalf("Failed to pre-populate data for Get benchmark: %v", err)
	}
	time.Sleep(1 * time.Second) // wait for apply

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Get("bench_read_key"); err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkSetParallel measures concurrent Write throughput.
func BenchmarkSetParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("bench_key_p_%d", rand.Int())
			if err := client.Set(key, "parallel_val", 0); err != nil {
				b.Errorf("Parallel Set failed: %v", err)
			}
		}
	})
}
