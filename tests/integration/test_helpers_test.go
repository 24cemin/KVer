package integration

import (
	"testing"

	"github.com/24cemin/KVer/pkg/sdk"
)

func registerClientCleanup(t *testing.T, client *sdk.Client) {
	t.Helper()
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("failed to close SDK client: %v", err)
		}
	})
}
