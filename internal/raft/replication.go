// Package raft — replication.go
// Log replication: leader'dan follower'lara AppendEntries gönderimi.
// Heartbeat ve log sync bu dosyada yönetilir.
package raft

import (
	"context"
	"sync"
	"time"
)

// replicateTo, leader olarak tek bir follower'a log replication yapar.
func (r *RaftNode) replicateTo(ctx context.Context, peerID string, nextIndex uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		r.mu.RLock()
		if r.state != Leader {
			r.mu.RUnlock()
			return nil
		}
		term := r.currentTerm
		prevLogIndex := nextIndex - 1
		var prevLogTerm uint64
		if prevLogIndex > 0 {
			entry, err := r.log.GetEntry(prevLogIndex)
			if err != nil {
				r.mu.RUnlock()
				return err
			}
			prevLogTerm = entry.Term
		}
		entries, err := r.log.GetEntriesFrom(nextIndex)
		if err != nil {
			r.mu.RUnlock()
			// Log compacted — snapshot gönder
			return r.sendSnapshot(ctx, peerID)
		}
		commitIndex := r.commitIndex
		r.mu.RUnlock()

		req := &AppendEntriesRequest{
			Term:         term,
			LeaderID:     r.config.NodeID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}

		rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := r.transport.AppendEntries(rpcCtx, peerID, req)
		cancel()
		if err != nil {
			return err
		}

		r.mu.Lock()
		if resp.Term > r.currentTerm {
			r.stepDown(resp.Term)
			r.mu.Unlock()
			return nil
		}

		if r.state != Leader || r.currentTerm != term {
			r.mu.Unlock()
			return nil
		}

		if resp.Success {
			newNextIndex := nextIndex + uint64(len(entries))
			if newNextIndex > r.nextIndex[peerID] {
				r.nextIndex[peerID] = newNextIndex
			}
			newMatchIndex := newNextIndex - 1
			if newMatchIndex > r.matchIndex[peerID] {
				r.matchIndex[peerID] = newMatchIndex
			}
			r.advanceCommitIndex()
			r.mu.Unlock()
			return nil
		} else {
			if resp.ConflictIndex > 0 {
				nextIndex = resp.ConflictIndex
			} else if nextIndex > 1 {
				nextIndex--
			}
			r.nextIndex[peerID] = nextIndex
			r.mu.Unlock()
			// Loop to retry with updated nextIndex
		}
	}
}

// sendHeartbeats, leader olarak tüm peer'lara boş AppendEntries (heartbeat) gönderir.
func (r *RaftNode) sendHeartbeats(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()

	// İlk heartbeat'i hemen gönder
	r.sendHeartbeatOnce(ctx)

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.RLock()
			state := r.state
			r.mu.RUnlock()

			if state != Leader {
				return
			}
			r.sendHeartbeatOnce(ctx)
		}
	}
}

// sendHeartbeatOnce, tüm peer'lara tek bir heartbeat gönderir.
func (r *RaftNode) sendHeartbeatOnce(ctx context.Context) {
	r.mu.RLock()
	term := r.currentTerm
	peers := r.clusterConfig.Clone().Peers
	r.mu.RUnlock()

	var wg sync.WaitGroup
	for peerID := range peers {
		if peerID == r.config.NodeID {
			continue
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			rpcCtx, cancel := context.WithTimeout(ctx, r.config.HeartbeatInterval*2)
			defer cancel()

			req := &AppendEntriesRequest{
				Term:     term,
				LeaderID: r.config.NodeID,
			}
			resp, err := r.transport.AppendEntries(rpcCtx, id, req)
			if err != nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()
			if resp.Term > r.currentTerm {
				r.stepDown(resp.Term)
			}
		}(peerID)
	}
	wg.Wait()
}

// handleAppendEntries, gelen AppendEntries RPC'yi işler (follower tarafı).
func (r *RaftNode) handleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req == nil {
		return &AppendEntriesResponse{Term: r.currentTerm, Success: false}
	}

	// Eski term — reddet
	if req.Term < r.currentTerm {
		return &AppendEntriesResponse{Term: r.currentTerm, Success: false}
	}

	// Geçerli liderden mesaj — stepDown ve heartbeat sıfırla
	if req.Term > r.currentTerm || r.state != Follower {
		r.stepDown(req.Term)
	}
	r.leaderID = req.LeaderID // geçerli lideri güncelle
	r.lastHeartbeat = time.Now()

	// PrevLogIndex/PrevLogTerm kontrolü (Log Matching Property)
	if req.PrevLogIndex > 0 {
		entry, err := r.log.GetEntry(req.PrevLogIndex)
		if err != nil {
			// Log'da bu index yok — conflict
			return &AppendEntriesResponse{
				Term:          r.currentTerm,
				Success:       false,
				ConflictIndex: r.log.LastIndex() + 1,
				ConflictTerm:  0,
			}
		}
		if entry.Term != req.PrevLogTerm {
			// Term uyuşmazlığı — conflicting term'in başlangıcını bul
			conflictTerm := entry.Term
			conflictIndex := req.PrevLogIndex
			for conflictIndex > 1 {
				e, err := r.log.GetEntry(conflictIndex - 1)
				if err != nil || e.Term != conflictTerm {
					break
				}
				conflictIndex--
			}
			return &AppendEntriesResponse{
				Term:          r.currentTerm,
				Success:       false,
				ConflictIndex: conflictIndex,
				ConflictTerm:  conflictTerm,
			}
		}
	}

	// Entries ekle — Log Matching: çakışan entry'leri kes, yenilerini ekle
	if len(req.Entries) > 0 {
		for i, entry := range req.Entries {
			existing, err := r.log.GetEntry(entry.Index)
			if err == nil && existing.Term != entry.Term {
				// Çakışma: bu index'ten sonrasını sil ve yenilerini ekle
				_ = r.log.TruncateAfter(entry.Index - 1)
				_ = r.log.Append(req.Entries[i:]...)
				break
			} else if err != nil {
				// Log'da yoksa ekle
				_ = r.log.Append(req.Entries[i:]...)
				break
			}
		}
	}

	// commitIndex güncelle
	if req.LeaderCommit > r.commitIndex {
		lastIndex := r.log.LastIndex()
		if req.LeaderCommit < lastIndex {
			r.commitIndex = req.LeaderCommit
		} else {
			r.commitIndex = lastIndex
		}
		select {
		case r.commitCh <- struct{}{}:
		default:
		}
	}

	return &AppendEntriesResponse{Term: r.currentTerm, Success: true}
}

// advanceCommitIndex, çoğunluk tarafından kopyalanmış en yüksek index'i commit eder.
// ÇAĞIRAN r.mu.Lock() tutmalıdır.
func (r *RaftNode) advanceCommitIndex() {
	if r.state != Leader {
		return
	}

	lastIndex := r.log.LastIndex()
	for n := lastIndex; n > r.commitIndex; n-- {
		entry, err := r.log.GetEntry(n)
		if err != nil {
			continue
		}
		// Raft güvenlik kuralı: sadece mevcut term'deki entry'ler commit edilebilir
		if entry.Term != r.currentTerm {
			break
		}

		// Kaç node bu index'i kopyaladı?
		count := 1 // self
		for peerID := range r.clusterConfig.Peers {
			if peerID == r.config.NodeID {
				continue
			}
			if r.matchIndex[peerID] >= n {
				count++
			}
		}

		majority := r.clusterConfig.Majority()
		if count >= majority {
			r.commitIndex = n
			select {
			case r.commitCh <- struct{}{}:
			default:
			}
			break
		}
	}
}

// ReadIndex, okuma sırasında liderliği çoğunluğa doğrulayarak linearizable (sıraya uygun)
// okuma garantisi sağlar. Yalnızca lider çağırabilir; aksi hâlde ErrNotLeader döner.
//
// Tek node'lu cluster'da majority kontrolü atlanır.
// Çok node'lu cluster'da mevcut commitIndex kaydedilir ve çoğunluğa boş AppendEntries
// gönderilerek liderlik teyit edilir; teyitten sonra lastApplied bu index'e yetişene kadar
// beklenir.
func (r *RaftNode) ReadIndex(ctx context.Context) (uint64, error) {
	r.mu.RLock()
	if r.state != Leader {
		r.mu.RUnlock()
		return 0, ErrNotLeader
	}
	if !r.noopCommitted {
		r.mu.RUnlock()
		return 0, ErrLeaderNotReady
	}
	readIndex := r.commitIndex
	peers := r.clusterConfig.Clone()
	term := r.currentTerm
	r.mu.RUnlock()

	// Tek node cluster: majority kontrolüne gerek yok
	if peers.Size() == 1 {
		return r.waitForApply(ctx, readIndex)
	}

	// Çoğunluğa heartbeat at, liderliği doğrula
	confirmCh := make(chan bool, peers.Size())
	confirmCh <- true // self-confirm

	for peerID := range peers.Peers {
		if peerID == r.config.NodeID {
			continue
		}
		go func(id string) {
			// Increase timeout from HeartbeatInterval (50ms) to 2s to avoid false positives
			rpcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			req := &AppendEntriesRequest{
				Term:     term,
				LeaderID: r.config.NodeID,
			}
			resp, err := r.transport.AppendEntries(rpcCtx, id, req)
			if err != nil || resp.Term > term {
				confirmCh <- false
				return
			}
			confirmCh <- true
		}(peerID)
	}

	majority := peers.Majority()
	confirmed := 0
	total := 0
	size := peers.Size()
	for total < size {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case ok := <-confirmCh:
			total++
			if ok {
				confirmed++
			}
			if confirmed >= majority {
				return r.waitForApply(ctx, readIndex)
			}
			// Majority imkânsız hâle geldi
			if total-confirmed > size-majority {
				return 0, ErrNotLeader
			}
		}
	}
	return 0, ErrNotLeader
}

// waitForApply, lastApplied >= index olana kadar bekler.
// Context iptal edilirse context hatası döner.
func (r *RaftNode) waitForApply(ctx context.Context, index uint64) (uint64, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		r.mu.RLock()
		applied := r.lastApplied
		r.mu.RUnlock()
		if applied >= index {
			return index, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-r.applyCh:
			// log uygulandı, tekrar kontrol et
		case <-ticker.C:
			// güvenlik amaçlı periyodik kontrol
		}
	}
}

// LeaderAddr, bilinen güncel liderin gRPC adresini döner.
// Bu node lider ise boş string döner (forward gerekmez).
// Lider bilinmiyorsa boş string döner.
func (r *RaftNode) LeaderAddr() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state == Leader {
		return "" // lider biziz, forward gerekmez
	}
	if r.leaderID == "" {
		return "" // lider henüz bilinmiyor
	}
	addr, ok := r.clusterConfig.Peers[r.leaderID]
	if !ok {
		return ""
	}
	return addr
}
