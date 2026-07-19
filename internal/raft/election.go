// Package raft — election.go
// Leader election algoritması: election timeout, candidate durumu, oy toplama.
// Ağ iletişimi Transport interface üzerinden yapılır — gRPC burada görünmez.
package raft

import (
	"context"
	"log"
	"math/rand/v2"
	"sync"
	"time"
)

// startElection, mevcut node'u candidate yapar ve oy toplar.
// Yeterli oy alınırsa node leader olur.

// startPreVote, gerçek bir seçime gitmeden önce diğer node'ları yoklar.
// Term artırılmaz. Çoğunluk onay verirse startElection() çağrılır.
func (r *RaftNode) startPreVote() {
	r.mu.Lock()
	if r.state == Leader {
		r.mu.Unlock()
		return
	}

	r.state = PreCandidate
	nextTerm := r.currentTerm + 1
	r.lastHeartbeat = time.Now()

	var lastLogIndex, lastLogTerm uint64
	if r.log != nil {
		lastLogIndex = r.log.LastIndex()
		lastLogTerm = r.log.LastTerm()
	}

	peers := make([]string, 0)
	for peerID := range r.clusterConfig.Peers {
		if peerID == r.config.NodeID {
			continue
		}
		peers = append(peers, peerID)
	}
	majority := r.clusterConfig.Majority()
	r.mu.Unlock()

	votes := 1 // self vote for pre-vote

	if votes >= majority {
		r.startElection()
		return
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var once sync.Once

	for _, peerID := range peers {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), r.config.HeartbeatInterval*2)
			defer cancel()

			req := &RequestVoteRequest{
				Term:         nextTerm,
				CandidateID:  r.config.NodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				PreVote:      true,
			}
			resp, err := r.transport.RequestVote(ctx, id, req)
			
			if err != nil {
				log.Printf("[%s] PreVote to %s failed: %v", r.config.NodeID, id, err)
				return
			}
			
			if resp.Term > nextTerm - 1 {
				r.mu.Lock()
				r.stepDown(resp.Term)
				r.mu.Unlock()
				return
			}

			if !resp.VoteGranted {
				return
			}

			mu.Lock()
			votes++
			currentVotes := votes
			mu.Unlock()

			if currentVotes >= majority {
				once.Do(func() {
					r.mu.Lock()
					if r.state == PreCandidate && r.currentTerm == nextTerm-1 {
						r.mu.Unlock()
						log.Printf("[%s] PreVote succeeded, starting real election", r.config.NodeID)
						go r.startElection()
					} else {
						r.mu.Unlock()
					}
				})
			}
		}(peerID)
	}
	wg.Wait()
}

func (r *RaftNode) startElection() {
	r.mu.Lock()
	if r.state == Leader {
		r.mu.Unlock()
		return
	}

	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.config.NodeID
	term := r.currentTerm
	r.lastHeartbeat = time.Now() // Reset timer when starting election

	if r.persistentState != nil {
		termToSave := r.currentTerm
		votedForToSave := r.votedFor
		r.mu.Unlock()
		_ = r.persistentState.SaveTermAndVote(termToSave, votedForToSave)
		r.mu.Lock()
		
		// Kilit serbestken state veya term değişmiş olabilir, kontrol et
		if r.state != Candidate || r.currentTerm != termToSave {
			r.mu.Unlock()
			return
		}
	}

	var lastLogIndex, lastLogTerm uint64
	if r.log != nil {
		lastLogIndex = r.log.LastIndex()
		lastLogTerm = r.log.LastTerm()
	}

	peers := make([]string, 0)
	for peerID := range r.clusterConfig.Peers {
		if peerID == r.config.NodeID {
			continue
		}
		peers = append(peers, peerID)
	}
	majority := r.clusterConfig.Majority()
	r.mu.Unlock()

	votes := 1 // self vote

	if votes >= majority {
		r.mu.Lock()
		if r.state == Candidate && r.currentTerm == term {
			r.state = Leader
			log.Printf("[%s] became leader, term=%d", r.config.NodeID, term)
			r.initLeaderState()
			r.wg.Add(1)
			go r.sendHeartbeats(context.Background())
		}
		r.mu.Unlock()
		return
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var once sync.Once
	for _, peerID := range peers {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(
				context.Background(), r.config.HeartbeatInterval*2)
			defer cancel()

			granted, err := r.sendRequestVote(ctx, id, term, lastLogIndex, lastLogTerm)
			if err != nil {
				log.Printf("[%s] RequestVote to %s failed: %v",
					r.config.NodeID, id, err)
			} else {
				log.Printf("[%s] RequestVote to %s: granted=%v",
					r.config.NodeID, id, granted)
			}
			if err != nil || !granted {
				return
			}

			mu.Lock()
			votes++
			currentVotes := votes
			mu.Unlock()

			if currentVotes >= majority {
				once.Do(func() {
					r.mu.Lock()
					if r.state == Candidate && r.currentTerm == term {
						r.state = Leader
						log.Printf("[%s] became leader, term=%d", r.config.NodeID, term)
						r.initLeaderState()
						r.wg.Add(1)
						go r.sendHeartbeats(context.Background())
					}
					r.mu.Unlock()
				})
			}
		}(peerID)
	}

	wg.Wait()
}

// sendRequestVote, tek bir peer'a RequestVote RPC gönderir ve yanıtı döndürür.

func (r *RaftNode) sendRequestVote(ctx context.Context, peerID string, term, lastLogIndex, lastLogTerm uint64) (bool, error) {
	req := &RequestVoteRequest{
		Term:         term,
		CandidateID:  r.config.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	resp, err := r.transport.RequestVote(ctx, peerID, req)
	if err != nil {
		return false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.Term > r.currentTerm {
		r.stepDown(resp.Term)
		return false, nil
	}

	if r.currentTerm != term || r.state != Candidate {
		return false, nil
	}

	if resp.Term < term {
		return false, nil
	}

	return resp.VoteGranted, nil
}

// handleRequestVote, gelen RequestVote RPC'yi işler.

func (r *RaftNode) handleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req == nil {
		return &RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	}

	if req.Term < r.currentTerm {
		return &RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	}

	// Disruptive server protection (Raft Paper Section 6.4)
	// If a server receives a RequestVote request within the minimum election timeout
	// of hearing from a current leader, it does not update its term or grant its vote.
	isLeaderAlive := false
	if r.state == Leader {
		isLeaderAlive = true
	} else if r.state == Follower && r.leaderID != "" {
		if time.Since(r.lastHeartbeat) < r.config.ElectionTimeoutMin {
			isLeaderAlive = true
		}
	}

	if isLeaderAlive {
		log.Printf("[%s] Ignoring RequestVote/PreVote from %s (term %d) because leader is alive", r.config.NodeID, req.CandidateID, req.Term)
		return &RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	}

	// Eğer gerçek seçimse ve term yüksekse stepDown yap. (Pre-Vote'da yapmıyoruz)
	if !req.PreVote && req.Term > r.currentTerm {
		r.stepDown(req.Term)
	}

	var myLastLogIndex, myLastLogTerm uint64
	if r.log != nil {
		myLastLogIndex = r.log.LastIndex()
		myLastLogTerm = r.log.LastTerm()
	}

	upToDate := req.LastLogTerm > myLastLogTerm ||
		(req.LastLogTerm == myLastLogTerm && req.LastLogIndex >= myLastLogIndex)

	// Pre-Vote ise ve log güncelse direkt oy ver (state değiştirmez, diske yazmaz)
	if req.PreVote {
		if upToDate {
			return &RequestVoteResponse{Term: r.currentTerm, VoteGranted: true}
		}
		return &RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
	}

	canVote := r.votedFor == "" || r.votedFor == req.CandidateID
	if canVote && upToDate {
		r.votedFor = req.CandidateID
		r.lastHeartbeat = time.Now()
		if r.persistentState != nil {
			termToSave := r.currentTerm
			votedForToSave := r.votedFor
			r.mu.Unlock()
			_ = r.persistentState.SaveTermAndVote(termToSave, votedForToSave)
			r.mu.Lock()
		}
		return &RequestVoteResponse{Term: r.currentTerm, VoteGranted: true}
	}

	return &RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}
}

// runElectionTimer, election timeout'u izler ve gerektiğinde election başlatır.

func (r *RaftNode) runElectionTimer(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		timeout := randomizedElectionTimeout(r.config.ElectionTimeoutMin, r.config.ElectionTimeoutMax)
		timer := time.NewTimer(timeout)

		select {
		case <-r.stopCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			r.mu.RLock()
			state := r.state
			lastHeartbeat := r.lastHeartbeat
			r.mu.RUnlock()

			if state == Leader {
				continue
			}

			elapsed := time.Since(lastHeartbeat)

			if elapsed >= timeout {
				log.Printf("[%s] election timer fired, state=%s, elapsed=%v, timeout=%v",
					r.config.NodeID, state, elapsed, timeout)
				log.Printf("[%s] starting pre-vote", r.config.NodeID)
				go r.startPreVote()
			}
		}
	}
}

func randomizedElectionTimeout(minTimeout, maxTimeout time.Duration) time.Duration {
	if maxTimeout <= minTimeout {
		return minTimeout
	}
	delta := maxTimeout - minTimeout
	return minTimeout + time.Duration(rand.Int64N(int64(delta)+1))
}
