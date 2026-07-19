package benchmark

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentSDKWrites(t *testing.T) {
	const numGoroutines = 10
	const writesPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(gID int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				key := fmt.Sprintf("concurrent_key_%d_%d", gID, j)
				val := "val"
				if err := client.Set(key, val, 0); err != nil {
					t.Errorf("Set failed for goroutine %d, write %d: %v", gID, j, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	t.Log("All concurrent writes completed successfully!")
}
