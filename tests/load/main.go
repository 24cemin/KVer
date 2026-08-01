package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emin/kver/pkg/sdk"
)

func main() {
	client := sdk.NewClient([]string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"})

	var successCount uint64
	var totalLatency time.Duration
	var mu sync.Mutex

	concurrency := 50
	duration := 10 * time.Second

	fmt.Printf("Starting load test with %d concurrent clients for %v...\n", concurrency, duration)

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for time.Since(start) <= duration {
				key := fmt.Sprintf("key_%d", workerID)
				val := "val"

				reqStart := time.Now()
				err := client.Set(key, val, 0)
				reqDuration := time.Since(reqStart)

				if err == nil {
					atomic.AddUint64(&successCount, 1)
					mu.Lock()
					totalLatency += reqDuration
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	actualDuration := time.Since(start)
	totalSuccess := atomic.LoadUint64(&successCount)
	throughput := float64(totalSuccess) / actualDuration.Seconds()

	var meanLatency time.Duration
	if totalSuccess > 0 {
		meanLatency = time.Duration(int64(totalLatency) / int64(totalSuccess))
	}

	fmt.Printf("Results:\n")
	fmt.Printf("Total Requests: %d\n", totalSuccess)
	fmt.Printf("Actual Duration: %v\n", actualDuration)
	fmt.Printf("Throughput: %.2f req/sec\n", throughput)
	fmt.Printf("Mean End-to-End Latency: %v\n", meanLatency)
}
