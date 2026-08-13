package sdk

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	kvpb "github.com/24cemin/KVer/proto/kv/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type llenTestServer struct {
	kvpb.UnimplementedKVServiceServer
	count int64
	err   error
	calls atomic.Int64
}

func (s *llenTestServer) LLen(context.Context, *kvpb.LLenRequest) (*kvpb.LLenResponse, error) {
	s.calls.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	return &kvpb.LLenResponse{Count: s.count}, nil
}

func startLLenTestServer(t *testing.T, implementation *llenTestServer) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	kvpb.RegisterKVServiceServer(grpcServer, implementation)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func TestClient_NewAndClose(t *testing.T) {
	c := NewClient([]string{"localhost:7001"})
	if err := c.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestClient_LLen_RetriesTransientReadOnNextEndpoint(t *testing.T) {
	unavailableServer := &llenTestServer{err: status.Error(codes.Unavailable, "leader unavailable")}
	healthyServer := &llenTestServer{count: 7}
	client := NewClient([]string{
		startLLenTestServer(t, unavailableServer),
		startLLenTestServer(t, healthyServer),
	})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})

	count, err := client.LLen("queue")
	if err != nil {
		t.Fatalf("LLen failed: %v", err)
	}
	if count != 7 {
		t.Fatalf("expected count 7, got %d", count)
	}
	if got := unavailableServer.calls.Load(); got != 1 {
		t.Fatalf("expected one unavailable endpoint call, got %d", got)
	}
	if got := healthyServer.calls.Load(); got != 1 {
		t.Fatalf("expected one healthy endpoint call, got %d", got)
	}
	client.mu.RLock()
	cachedLeader := client.leader
	client.mu.RUnlock()
	if cachedLeader != "" {
		t.Fatalf("read endpoint was incorrectly cached as leader: %q", cachedLeader)
	}
}

func TestClient_LLen_WrongTypeDoesNotRetry(t *testing.T) {
	wrongTypeServer := &llenTestServer{err: status.Error(codes.FailedPrecondition, "wrong type for operation")}
	healthyServer := &llenTestServer{count: 7}
	client := NewClient([]string{
		startLLenTestServer(t, wrongTypeServer),
		startLLenTestServer(t, healthyServer),
	})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})

	_, err := client.LLen("string-key")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	if got := status.Convert(err).Message(); got != "wrong type for operation" {
		t.Fatalf("unexpected error message %q", got)
	}
	if got := wrongTypeServer.calls.Load(); got != 1 {
		t.Fatalf("expected one wrong-type endpoint call, got %d", got)
	}
	if got := healthyServer.calls.Load(); got != 0 {
		t.Fatalf("wrong-type response retried on healthy endpoint %d times", got)
	}
}

func TestIsRetryableReadError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "unavailable", err: status.Error(codes.Unavailable, "unavailable"), want: true},
		{name: "deadline exceeded", err: status.Error(codes.DeadlineExceeded, "timeout"), want: true},
		{name: "wrong type", err: status.Error(codes.FailedPrecondition, "wrong type"), want: false},
		{name: "canceled", err: status.Error(codes.Canceled, "canceled"), want: false},
		{name: "plain error", err: errors.New("plain error"), want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isRetryableReadError(test.err); got != test.want {
				t.Fatalf("isRetryableReadError() = %v, want %v", got, test.want)
			}
		})
	}
}

// GetSet, Delete and ContextCancellation tests are covered in tests/integration/sdk_integration_test.go
// and tests/integration/multinode_test.go
