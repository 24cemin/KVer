package sdk

import (
	"testing"
)

func TestClient_NewAndClose(t *testing.T) {
	c := NewClient([]string{"localhost:7001"})
	if err := c.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// GetSet, Delete and ContextCancellation tests are covered in tests/integration/sdk_integration_test.go
// and tests/integration/multinode_test.go
