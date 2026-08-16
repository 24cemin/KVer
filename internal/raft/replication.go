// Package raft — replication.go
// Log replication: leader'dan follower'lara AppendEntries gönderimi.
// Heartbeat ve log sync bu dosyada yönetilir.
package raft

import (
	"context"
	"errors"
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
		if r.fatalErr != nil {
			err := r.fatalErr
			r.mu.RUnlock()
			return err
		}
		if r.state != Leader {
			r.mu.RUnlock()
			return ErrNotLeader
		}
		term := r.currentTerm
		lastIndex := r.log.LastIndex()
		baseIndex, _, _ := r.log.Boundary()
		if nextIndex == 0 {
			nextIndex = 1
		}
		if nextIndex > lastIndex+1 {
			nextIndex = lastIndex + 1
		}
		if nextIndex <= baseIndex {
			r.mu.RUnlock()
			return r.sendSnapshot(ctx, peerID)
		}

		prevLogIndex := nextIndex - 1
		prevLogTerm, err := r.log.TermAt(prevLogIndex)
		if err != nil {
			r.mu.RUnlock()
			if errors.Is(err, ErrLogCompacted) {
				return r.sendSnapshot(ctx, peerID)
			}
			return err
		}
		entries, err := r.log.GetEntriesFrom(nextIndex)
		if err != nil {
			r.mu.RUnlock()
			if errors.Is(err, ErrLogCompacted) {
				return r.sendSnapshot(ctx, peerID)
			}
			return err
		}
		if limit := r.config.MaxLogEntriesPerRPC; limit > 0 && len(entries) > limit {
			entries = entries[:limit]
		}
		commitIndex := r.commitIndex
		r.mu.RUnlock()

		request := &AppendEntriesRequest{
			Term:         term,
			LeaderID:     r.config.NodeID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}

		rpcContext, cancel := context.WithTimeout(ctx, 2*time.Second)
		response, err := r.transport.AppendEntries(rpcContext, peerID, request)
		cancel()
		if err != nil {
			return err
		}
		if response == nil {
			return errors.New("AppendEntries returned a nil response")
		}

		r.mu.Lock()
		if response.Term > r.currentTerm {
			r.stepDown(response.Term)
			r.mu.Unlock()
			return ErrNotLeader
		}
		if r.state != Leader || r.currentTerm != term {
			r.mu.Unlock()
			return ErrNotLeader
		}

		if response.Success {
			newMatchIndex := prevLogIndex + uint64(len(entries))
			newNextIndex := newMatchIndex + 1
			if newNextIndex > r.nextIndex[peerID] {
				r.nextIndex[peerID] = newNextIndex
			}
			if newMatchIndex > r.matchIndex[peerID] {
				r.matchIndex[peerID] = newMatchIndex
			}
			r.advanceCommitIndex()
			hasMore := newNextIndex <= r.log.LastIndex()
			r.mu.Unlock()
			if hasMore {
				nextIndex = newNextIndex
				continue
			}
			return nil
		}

		if response.ConflictIndex > 0 {
			nextIndex = response.ConflictIndex
		} else if nextIndex > 1 {
			nextIndex--
		}
		if nextIndex == 0 {
			nextIndex = 1
		}
		r.nextIndex[peerID] = nextIndex
		r.mu.Unlock()
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
	response, _ := r.processAppendEntries(req)
	return response
}

func (r *RaftNode) processAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.fatalErr != nil {
		return &AppendEntriesResponse{Term: r.currentTerm, Success: false}, r.fatalErr
	}
	if req == nil {
		return &AppendEntriesResponse{Term: r.currentTerm, Success: false}, nil
	}
	if req.Term < r.currentTerm {
		return &AppendEntriesResponse{Term: r.currentTerm, Success: false}, nil
	}
	if req.Term > r.currentTerm || r.state != Follower {
		r.stepDown(req.Term)
	}
	r.leaderID = req.LeaderID
	r.lastHeartbeat = time.Now()

	if len(req.Entries) > 0 {
		if req.PrevLogIndex == ^uint64(0) {
			return &AppendEntriesResponse{Term: r.currentTerm, Success: false}, ErrNonContiguousLog
		}
		if err := validateEntrySequence(req.Entries, req.PrevLogIndex+1); err != nil {
			return &AppendEntriesResponse{Term: r.currentTerm, Success: false}, err
		}
	}

	if r.log == nil {
		if req.PrevLogIndex > 0 || len(req.Entries) > 0 {
			return &AppendEntriesResponse{Term: r.currentTerm, Success: false}, ErrOutOfRange
		}
		return &AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
	}

	if req.PrevLogIndex == 0 {
		baseIndex, _, _ := r.log.Boundary()
		if baseIndex > 0 {
			return &AppendEntriesResponse{
				Term:          r.currentTerm,
				Success:       false,
				ConflictIndex: r.log.FirstIndex(),
			}, nil
		}
	} else {
		previousTerm, err := r.log.TermAt(req.PrevLogIndex)
		if err != nil {
			conflictIndex := r.log.LastIndex() + 1
			if errors.Is(err, ErrLogCompacted) {
				conflictIndex = r.log.FirstIndex()
			}
			return &AppendEntriesResponse{
				Term:          r.currentTerm,
				Success:       false,
				ConflictIndex: conflictIndex,
			}, nil
		}
		if previousTerm != req.PrevLogTerm {
			baseIndex, _, _ := r.log.Boundary()
			conflictIndex := req.PrevLogIndex
			for conflictIndex > baseIndex+1 {
				term, err := r.log.TermAt(conflictIndex - 1)
				if err != nil || term != previousTerm {
					break
				}
				conflictIndex--
			}
			return &AppendEntriesResponse{
				Term:          r.currentTerm,
				Success:       false,
				ConflictIndex: conflictIndex,
				ConflictTerm:  previousTerm,
			}, nil
		}
	}

	for position, entry := range req.Entries {
		existingTerm, err := r.log.TermAt(entry.Index)
		switch {
		case err == nil && existingTerm == entry.Term:
			continue
		case err == nil:
			if err := r.log.TruncateAfter(entry.Index - 1); err != nil {
				r.markStorageFailureLocked(err)
				return &AppendEntriesResponse{Term: r.currentTerm, Success: false}, err
			}
			if err := r.log.Append(req.Entries[position:]...); err != nil {
				r.markStorageFailureLocked(err)
				return &AppendEntriesResponse{Term: r.currentTerm, Success: false}, err
			}
			goto entriesApplied
		case errors.Is(err, ErrOutOfRange):
			if err := r.log.Append(req.Entries[position:]...); err != nil {
				r.markStorageFailureLocked(err)
				return &AppendEntriesResponse{Term: r.currentTerm, Success: false}, err
			}
			goto entriesApplied
		default:
			return &AppendEntriesResponse{
				Term:          r.currentTerm,
				Success:       false,
				ConflictIndex: r.log.FirstIndex(),
			}, nil
		}
	}

entriesApplied:
	if req.LeaderCommit > r.commitIndex {
		lastIndex := r.log.LastIndex()
		r.commitIndex = min(req.LeaderCommit, lastIndex)
		select {
		case r.commitCh <- struct{}{}:
		default:
		}
	}
	return &AppendEntriesResponse{Term: r.currentTerm, Success: true}, nil
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
	if r.fatalErr != nil {
		err := r.fatalErr
		r.mu.RUnlock()
		return 0, err
	}
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

func (r *RaftNode) markStorageFailureLocked(err error) {
	if errors.Is(err, ErrStorageUnavailable) {
		r.markFatalLocked(err)
	}
}
