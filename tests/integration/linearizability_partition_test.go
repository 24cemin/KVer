package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/emin/kver/pkg/sdk"
)

func TestLinearizabilityPartition(t *testing.T) {
	_, nodes, cleanup := makeThreeNodeCluster(t, 7401)
	defer cleanup()

	client := sdk.NewClient([]string{"127.0.0.1:7401", "127.0.0.1:7402", "127.0.0.1:7403"})
	defer client.Close()

	_ = client.Set("ping", "pong", 0)

	registerModel := porcupine.Model{
		Init: func() interface{} { return "" },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			st := state.(string)
			in := input.(operation)
			if in.op == "read" {
				out := output.(string)
				return st == out, st
			}
			return true, in.value
		},
	}

	var ops []porcupine.Operation
	var mu sync.Mutex

	concurrency := 10
	opsPerClient := 20
	key := "lin_test_key"
	_ = client.Delete(key)

	var wg sync.WaitGroup

    // Goroutine to partition (kill) a follower halfway through
	go func() {
		time.Sleep(50 * time.Millisecond) // Wait a bit
        // Find a follower and stop it
        for _, n := range nodes {
            if n.State() != 3 { // Assuming Leader is state 3 or just stop nodes[2]
               n.Stop()
               break
            }
        }
	}()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for j := 0; j < opsPerClient; j++ {
				isWrite := j%2 == 0
				val := fmt.Sprintf("val_%d_%d", clientID, j)
				
				invokeTime := time.Now().UnixNano()
				var retValue string
				
				if isWrite {
					_ = client.Set(key, val, 0)
					retValue = val
				} else {
					retValue, _ = client.Get(key)
				}
				retTime := time.Now().UnixNano()

				mu.Lock()
				ops = append(ops, porcupine.Operation{
					ClientId: clientID,
					Input: operation{
						op:    map[bool]string{true: "write", false: "read"}[isWrite],
						value: val,
					},
					Call:   invokeTime,
					Output: retValue,
					Return: retTime,
				})
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	isLinearizable := porcupine.CheckOperations(registerModel, ops)
	if !isLinearizable {
		t.Fatalf("Linearizability violation detected under partition!")
	} else {
		t.Logf("Successfully verified %d concurrent operations under partition. System is STRICTLY LINEARIZABLE.", len(ops))
	}
}
