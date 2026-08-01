package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/emin/kver/pkg/sdk"
)

type operation struct {
	op    string // "read" or "write"
	value string
}

func TestLinearizabilityWithPorcupine(t *testing.T) {
	// Start an isolated in-memory 3-node cluster to bypass Docker networking constraints
	_, _, cleanup := makeThreeNodeCluster(t, 7301)
	t.Cleanup(cleanup)

	client := sdk.NewClient([]string{"127.0.0.1:7301", "127.0.0.1:7302", "127.0.0.1:7303"})
	registerClientCleanup(t, client)

	err := client.Set("ping", "pong", 0)
	if err != nil {
		t.Fatalf("Failed to reach isolated cluster: %v", err)
	}

	// Define the linearizability model: A simple Read/Write Register
	registerModel := porcupine.Model{
		Init: func() interface{} {
			return ""
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			st := state.(string)
			in := input.(operation)
			if in.op == "read" {
				out := output.(string)
				// a read must return the current state
				return st == out, st
			}
			// a write changes the state to the new value
			return true, in.value
		},
	}

	var ops []porcupine.Operation
	var mu sync.Mutex

	concurrency := 10
	opsPerClient := 20
	key := "lin_test_key"

	// Reset key
	_ = client.Delete(key)

	var wg sync.WaitGroup

	// Run concurrent clients
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

	// Verify the history using Porcupine
	isLinearizable := porcupine.CheckOperations(registerModel, ops)
	if !isLinearizable {
		t.Fatalf("Linearizability violation detected! The history of %d operations was NOT linearizable.", len(ops))
	} else {
		t.Logf("Successfully verified %d concurrent operations. System is STRICTLY LINEARIZABLE.", len(ops))
	}
}
