package raft

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRaft_Batching(t *testing.T) {
	t.Run("BatchCollection", func(t *testing.T) {
		var replicationCalls int32
		transport := &mockTransportFull{
			appendEntriesFn: func(ctx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
				if len(req.Entries) > 0 {
					atomic.AddInt32(&replicationCalls, 1)
				}
				return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
			},
		}
		
		peers := map[string]string{"node2": "localhost:7002", "node3": "localhost:7003"}
		r := newLeaderNode(t, peers, transport)
		defer r.Stop()

		// 50 komut gönder
		for i := 0; i < 50; i++ {
			go func() { _ = r.Propose([]byte("cmd")) }()
		}

		// Batch loop'un işlemesi için bekle
		time.Sleep(100 * time.Millisecond)

		r.mu.RLock()
		lastIdx := r.log.LastIndex()
		r.mu.RUnlock()

		if lastIdx < 50 {
			t.Errorf("expected at least 50 entries in log, got %d", lastIdx)
		}

		calls := atomic.LoadInt32(&replicationCalls)
		// 50 komut için 50 replication çağrısı olmamalı (peer sayısı 2, her batch peer sayısı kadar çağrı yapar)
		// Eğer batching çalışıyorsa, toplam RPC çağrısı 50 * 2 = 100'den az olmalı.
		// Tek batch yapıldıysa 2 çağrı olur.
		if calls >= 100 {
			t.Errorf("batching might not be working, replication calls: %d", calls)
		}
		t.Logf("Replication calls for 50 commands: %d", calls)
	})

	t.Run("TimeoutFlush", func(t *testing.T) {
		transport := &mockTransportFull{
			appendEntriesFn: func(ctx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
				return &AppendEntriesResponse{Term: req.Term, Success: true}, nil
			},
		}
		r := newLeaderNode(t, map[string]string{"n2": "addr"}, transport)
		defer r.Stop()

		go func() { _ = r.Propose([]byte("single cmd")) }()
		
		// 5ms timeout'u bekle
		time.Sleep(20 * time.Millisecond)

		if r.log.LastIndex() == 0 {
			t.Error("expected entry to be flushed by timeout")
		}
	})

	t.Run("BatchOverflow", func(t *testing.T) {
		transport := &mockTransportFull{}
		r := newLeaderNode(t, map[string]string{}, transport)
		// batchLoop'u durdur ki kanal boşalmasın
		r.Stop() 

		// Kanalı doldur (kapasite 1000)
		for i := 0; i < 1000; i++ {
			go func() { _ = r.Propose([]byte("full")) }()
		}
		
		// Buffer'ın dolması için biraz bekle
		time.Sleep(50 * time.Millisecond)

		// 1001. hata vermeli
		err := r.Propose([]byte("overflow"))
		if err == nil {
			t.Error("expected overflow error")
		}
	})
}
