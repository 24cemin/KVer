package main

import (
	"bytes"
	"testing"
)

func TestWriteLLenResult(t *testing.T) {
	tests := []struct {
		name   string
		length int64
		want   string
	}{
		{name: "zero", length: 0, want: "0\n"},
		{name: "normal length", length: 3, want: "3\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := writeLLenResult(&output, test.length); err != nil {
				t.Fatalf("writeLLenResult failed: %v", err)
			}
			if output.String() != test.want {
				t.Errorf("expected output %q, got %q", test.want, output.String())
			}
		})
	}
}
