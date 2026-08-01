// Package raft — raft.go
// Raft node'unun ana koordinasyon dosyasıdır.
// Node durumu (Follower/Candidate/Leader), term yönetimi ve ana döngü buradadır.
package raft

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// NodeState, bir Raft node'unun olası durumlarını temsil eder.
type NodeState int

const (
	Follower NodeState = iota
	PreCandidate
	Candidate
	Leader
)

// String, NodeState'in insan-okunabilir temsilini döndürür.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case PreCandidate:
		return "PreCandidate"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RaftNode, tek bir Raft consensus node'unu temsil eder.
type RaftNode struct {
	mu sync.RWMutex

	// config, bu node'un Raft parametrelerini içerir.
	config *Config

	// state, mevcut node durumudur (Follower/Candidate/Leader).
	state NodeState

	// currentTerm, kalıcı olarak saklanması gereken mevcut Raft term'idir.
	currentTerm uint64

	// votedFor, mevcut term'de oy verilen candidate ID'sidir ("" = oy verilmedi).
	votedFor string

	// log, Raft log kayıtlarını yönetir (log.go).
	log *RaftLog

	// persistentState, term ve votedFor'u diske yazar (persistent_state.go).
	persistentState *PersistentState

	// stateMachine, commit edilen entry'leri uygular (StateMachine).
	stateMachine StateMachine

	// transport, peer iletişimi için kullanılır (transport.go).
	transport Transport

	// commitIndex, commit edilmiş en yüksek log index'i.
	commitIndex uint64

	// lastApplied, state machine'e uygulanmış en yüksek log index'i.
	lastApplied uint64

	// Leader-only state (nil iken Leader değil)
	// nextIndex ve matchIndex election.go ya da replication.go'da tutulacak.
	nextIndex  map[string]uint64 // leader: her peer için sonraki gönderilecek index
	matchIndex map[string]uint64 // leader: her peer için bilinen en yüksek index

	clusterConfig     *ClusterConfig // dinamik cluster config
	pendingMembership bool           // bekleyen membership var mı

	// Snapshot state
	snapshotMeta SnapshotMeta
	snapshotData []byte

	// stopCh, node'u durdurmak için kapatılan kanal.
	stopCh chan struct{}

	// lastHeartbeat, son heartbeat zamanıdır.
	lastHeartbeat time.Time

	// leaderID, bu node'un bildiği güncel lider ID'sidir.
	// Follower'dan gelen AppendEntries ile güncellenir.
	// stepDown'da sıfırlanır.
	leaderID string

	// noopCommitted, liderin kendi term'indeki no-op entry'sini commit edip etmediğini gösterir.
	// ReadIndex protokolü bu bayrak true olmadan güvenli okuma yapamaz.
	noopCommitted bool

	// commitCh, commitIndex değiştiğinde applyEntriesLoop'u uyandırır.
	commitCh chan struct{}

	// applyCh, bir log entry uygulandığında waitForApply'a sinyal gönderir.
	applyCh chan struct{}

	// proposeCh, batch buffer kanalıdır.
	// Propose() komutları buraya gönderir, batch loop buradan okur.
	proposeCh chan *proposeRequest
	waiters   map[uint64]chan error

	// wg, node goroutine lifecycle yönetimi için kullanılır.
	wg sync.WaitGroup
}

type proposeRequest struct {
	cmd    []byte
	waitCh chan error
}

// NewRaftNode, yeni bir RaftNode oluşturur ve başlatır.
func NewRaftNode(cfg *Config, sm StateMachine, transport Transport) (*RaftNode, error) {
	ps, err := newPersistentState(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	term := ps.Term()
	votedFor := ps.VotedFor()
	raftLog, err := newRaftLogWithWAL(cfg.DataDir, cfg.NodeID, cfg.SyncWrites)
	if err != nil {
		return nil, err
	}

	clusterConfig := &ClusterConfig{
		Peers: make(map[string]string),
	}
	for k, v := range cfg.Peers {
		clusterConfig.Peers[k] = v
	}
	clusterConfig.Peers[cfg.NodeID] = ""

	r := &RaftNode{
		config:          cfg,
		clusterConfig:   clusterConfig,
		state:           Follower,
		currentTerm:     term,
		votedFor:        votedFor,
		log:             raftLog,
		persistentState: ps,
		stateMachine:    sm,
		transport:       transport,
		nextIndex:       make(map[string]uint64),
		matchIndex:      make(map[string]uint64),
		stopCh:          make(chan struct{}),
		commitCh:        make(chan struct{}, 1),
		applyCh:         make(chan struct{}, 1),
		proposeCh:       make(chan *proposeRequest, 1000),
		waiters:         make(map[uint64]chan error),
		// InitialElectionDelay: Docker bridge network ve gRPC bağlantılarının
		// kurulması için production'da 1s ayarlanır. Testlerde 0 olarak bırakılır.
		lastHeartbeat: time.Now().Add(cfg.InitialElectionDelay),
	}
	// Disk'ten snapshot yükle (varsa)
	if err := r.loadSnapshotFromDisk(); err != nil {
		return nil, err
	}

	r.wg.Add(1)
	go r.runElectionTimer(&r.wg)
	r.wg.Add(1)
	go r.applyEntriesLoop()
	r.wg.Add(1)
	go r.batchLoop()
	return r, nil
}

// Stop, Raft düğümünü ve arkasındaki döngüleri güvenlice durdurur.
func (r *RaftNode) Stop() {
	select {
	case <-r.stopCh:
		// Zaten durduruldu
	default:
		close(r.stopCh)
	}
	r.wg.Wait()

	if r.log != nil {
		if err := r.log.Close(); err != nil {
			log.Printf("[%s] failed to close Raft log: %v", r.config.NodeID, err)
		}
	}
}

func (r *RaftNode) replicateToAsync(ctx context.Context, peerID string, nextIndex uint64) {
	if err := r.replicateTo(ctx, peerID, nextIndex); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[%s] replication to %s failed: %v", r.config.NodeID, peerID, err)
	}
}

func (r *RaftNode) resetHeartbeat() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastHeartbeat = time.Now()
}

// initLeaderState, Raft §5.3 gereği node leader olduğunda çağrılır.
// nextIndex ve matchIndex'i sıfırlar.
// ÇAĞIRAN r.mu.Lock() tutmalıdır.
func (r *RaftNode) initLeaderState() {
	lastIndex := r.log.LastIndex()
	for peerID := range r.clusterConfig.Peers {
		if peerID == r.config.NodeID {
			continue
		}
		r.nextIndex[peerID] = lastIndex + 1
		r.matchIndex[peerID] = 0
	}

	entry := LogEntry{
		Index: r.log.LastIndex() + 1,
		Term:  r.currentTerm,
		Type:  EntryNoop,
	}
	_ = r.log.Append(entry)

	// Replikasyonu tetikle (Raft §5.4.2)
	ctx := context.Background()
	for peerID := range r.clusterConfig.Peers {
		if peerID == r.config.NodeID {
			continue
		}
		nextIndex := r.nextIndex[peerID]
		go r.replicateToAsync(ctx, peerID, nextIndex)
	}

	r.advanceCommitIndex()
}

// stepDown, node'u follower'a düşürür ve term günceller.
// Yüksek term görüldüğünde election.go ve replication.go'dan çağrılır.
// ÇAĞIRAN mu.Lock() tutmalıdır.
func (r *RaftNode) stepDown(term uint64) {
	r.state = Follower
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
	}
	r.leaderID = ""         // lider bilinmiyor artık
	r.noopCommitted = false // yeni liderlik döneminde sıfırla
	r.lastHeartbeat = time.Now()
	if r.persistentState != nil {
		termToSave := r.currentTerm
		votedForToSave := r.votedFor
		r.mu.Unlock()
		_ = r.persistentState.SaveTermAndVote(termToSave, votedForToSave)
		r.mu.Lock()
	}
	for _, ch := range r.waiters {
		ch <- ErrNotLeader
	}
	r.waiters = make(map[uint64]chan error)
}

// batchLoop, gelen komutları biriktirip toplu halde replike eder.
func (r *RaftNode) batchLoop() {
	defer r.wg.Done()
	var batch []*proposeRequest
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		r.mu.Lock()
		if r.state != Leader {
			for _, req := range batch {
				req.waitCh <- ErrNotLeader
			}
			batch = nil
			r.mu.Unlock()
			return
		}

		lastIndex := r.log.LastIndex()
		var entries []LogEntry
		for i, req := range batch {
			entries = append(entries, LogEntry{
				Index:   lastIndex + uint64(i+1),
				Term:    r.currentTerm,
				Type:    EntryKV,
				Command: req.cmd,
			})
		}

		// Disk I/O sırasında RaftNode'un bloke olmaması için kilidi geçici olarak açıyoruz.
		r.mu.Unlock()
		err := r.log.Append(entries...)
		r.mu.Lock()

		// Eğer biz diske yazarken liderliğimiz düştüyse, gelen istekleri iptal et
		if r.state != Leader {
			for _, req := range batch {
				req.waitCh <- ErrNotLeader
			}
			batch = nil
			r.mu.Unlock()
			return
		}

		if err != nil {
			// Hata durumunda log'a yazılamadı, batch'i temizle
			for _, req := range batch {
				req.waitCh <- err
			}
			batch = nil
			r.mu.Unlock()
			return
		}

		for i, req := range batch {
			r.waiters[entries[i].Index] = req.waitCh
		}

		// Peer'ların nextIndex'ini ayarla
		for peerID := range r.clusterConfig.Peers {
			if peerID == r.config.NodeID {
				continue
			}
			if r.nextIndex[peerID] == 0 {
				r.nextIndex[peerID] = entries[0].Index
			}
		}

		peers := r.clusterConfig.Clone()
		r.advanceCommitIndex()
		r.mu.Unlock()

		// Replikasyonu tetikle
		ctx := context.Background()
		for peerID := range peers.Peers {
			if peerID == r.config.NodeID {
				continue
			}
			go func(id string) {
				r.mu.RLock()
				ni := r.nextIndex[id]
				r.mu.RUnlock()
				r.replicateToAsync(ctx, id, ni)
			}(peerID)
		}

		batch = nil
	}

	for {
		select {
		case <-r.stopCh:
			flush()
			return
		case cmd := <-r.proposeCh:
			batch = append(batch, cmd)
			if len(batch) >= 100 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// applyEntriesLoop, commit edilmiş entry'leri periyodik olarak
// state machine'e uygular.
func (r *RaftNode) applyEntriesLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.commitCh:
			r.mu.Lock()
			for r.commitIndex > r.lastApplied {
				nextToApply := r.lastApplied + 1
				entry, err := r.log.GetEntry(nextToApply)
				if err != nil {
					r.mu.Unlock()
					break
				}
				r.mu.Unlock()
				if entry.Type == EntryMembership {
					if err := r.applyMembership(entry); err != nil {
						_ = err
					}
				} else {
					_ = r.stateMachine.Apply(entry)
				}
				r.mu.Lock()
				r.lastApplied = nextToApply
				// No-Op entry commit edildiğinde ReadIndex protokolünü kilitle aç
				if entry.Type == EntryNoop && r.state == Leader && entry.Term == r.currentTerm {
					r.noopCommitted = true
				}
				if ch, ok := r.waiters[entry.Index]; ok {
					ch <- nil
					delete(r.waiters, entry.Index)
				}

				// Zaten uygulandı, uyandırmaya gerek yok
				select {
				case r.applyCh <- struct{}{}:
				default:
				}

				if r.config.SnapshotThreshold > 0 && r.lastApplied%r.config.SnapshotThreshold == 0 {
					go func() {
						_ = r.takeSnapshot()
					}()
				}
			}
			r.mu.Unlock()
		}
	}
}

// State, node'un mevcut durumunu thread-safe döndürür.
func (r *RaftNode) State() NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// CurrentTerm, node'un mevcut term'ini thread-safe döndürür.
func (r *RaftNode) CurrentTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

// IsLeader, bu node'un leader olup olmadığını döndürür.
func (r *RaftNode) IsLeader() bool {
	return r.State() == Leader
}

// LastLogIndex, log'daki son entry index'ini thread-safe döndürür.
func (r *RaftNode) LastLogIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.log.LastIndex()
}

// CommitIndex, commit edilmiş en yüksek log index'ini thread-safe döndürür.
func (r *RaftNode) CommitIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commitIndex
}

// Peers, cluster'daki tüm peer'ların (nodeID -> address) kopyasını thread-safe döndürür.
func (r *RaftNode) Peers() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.clusterConfig == nil {
		return nil
	}
	res := make(map[string]string, len(r.clusterConfig.Peers))
	for k, v := range r.clusterConfig.Peers {
		res[k] = v
	}
	return res
}

// HandleAppendEntries, gelen AppendEntries RPC'yi işler (server katmanı için public wrapper).
func (r *RaftNode) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	return r.handleAppendEntries(req)
}

// HandleRequestVote, gelen RequestVote RPC'yi işler (server katmanı için public wrapper).
func (r *RaftNode) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	return r.handleRequestVote(req)
}

// HandleInstallSnapshot, gelen InstallSnapshot RPC'yi işler (server katmanı için public wrapper).
func (r *RaftNode) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	return r.handleInstallSnapshot(req)
}

// Propose, leader olarak yeni bir komut önerir ve batch buffer'a gönderir.
func (r *RaftNode) Propose(cmd []byte) error {
	r.mu.RLock()
	isLeader := r.state == Leader
	r.mu.RUnlock()

	if !isLeader {
		return ErrNotLeader
	}

	req := &proposeRequest{
		cmd:    cmd,
		waitCh: make(chan error, 1),
	}

	// Batch kanalına gönder
	select {
	case r.proposeCh <- req:
		return <-req.waitCh
	default:
		return ErrProposeChannelFull
	}
}

// Term returns the current term of the Raft node.
func (r *RaftNode) Term() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

// NodeID returns the ID of the Raft node.
func (r *RaftNode) NodeID() string {
	return r.config.NodeID
}
