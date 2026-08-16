package raft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type snapshotMockStateMachine struct {
	mu           sync.Mutex
	snapshotData []byte
	restoredData []byte
	applyCount   int
	applyStarted chan struct{}
	releaseApply chan struct{}
	restoreErr   error
	restoreCount int
}

func (m *snapshotMockStateMachine) Apply(entry LogEntry) error {
	if m.applyStarted != nil {
		select {
		case m.applyStarted <- struct{}{}:
		default:
		}
	}
	if m.releaseApply != nil {
		<-m.releaseApply
	}
	m.mu.Lock()
	m.applyCount++
	m.snapshotData = append([]byte(nil), entry.Command...)
	m.mu.Unlock()
	return nil
}

func (m *snapshotMockStateMachine) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.snapshotData...), nil
}

func (m *snapshotMockStateMachine) Restore(data []byte) error {
	if m.restoreErr != nil {
		return m.restoreErr
	}
	m.mu.Lock()
	m.restoreCount++
	m.restoredData = append([]byte(nil), data...)
	m.snapshotData = append([]byte(nil), data...)
	m.mu.Unlock()
	return nil
}

func (m *snapshotMockStateMachine) restored() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.restoredData...)
}

func newInstallRequest(term, index, boundaryTerm uint64, data []byte) *InstallSnapshotRequest {
	checksum := sha256.Sum256(data)
	return &InstallSnapshotRequest{
		Term:              term,
		LeaderID:          "node2",
		LastIncludedIndex: index,
		LastIncludedTerm:  boundaryTerm,
		Done:              true,
		TotalSize:         uint64(len(data)),
		Checksum:          append([]byte(nil), checksum[:]...),
		ClusterConfig: map[string]string{
			"node1": "",
			"node2": "localhost:7002",
		},
		Data: append([]byte(nil), data...),
	}
}

func snapshotChunk(request *InstallSnapshotRequest, offset, end uint64) *InstallSnapshotRequest {
	chunk := *request
	chunk.Offset = offset
	chunk.Done = end == request.TotalSize
	chunk.Data = append([]byte(nil), request.Data[offset:end]...)
	return &chunk
}

func TestSnapshot_TakeSnapshotUsesBoundaryTerm(t *testing.T) {
	cfg := testConfig("node1", nil)
	stateMachine := &snapshotMockStateMachine{snapshotData: []byte("snap-data")}
	node, err := NewRaftNode(cfg, stateMachine, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	requireNoError(t, node.log.Append(
		LogEntry{Index: 1, Term: 1},
		LogEntry{Index: 2, Term: 1},
		LogEntry{Index: 3, Term: 2},
		LogEntry{Index: 4, Term: 2},
		LogEntry{Index: 5, Term: 2},
	))
	node.mu.Lock()
	node.lastApplied = 5
	node.mu.Unlock()

	requireNoError(t, node.takeSnapshot())

	if node.log.FirstIndex() != 6 {
		t.Fatalf("expected FirstIndex 6, got %d", node.log.FirstIndex())
	}
	if node.log.LastIndex() != 5 || node.log.LastTerm() != 2 {
		t.Fatalf("expected compacted boundary 5/2, got %d/%d", node.log.LastIndex(), node.log.LastTerm())
	}
	term, err := node.log.TermAt(5)
	requireNoError(t, err)
	if term != 2 {
		t.Fatalf("expected boundary term 2, got %d", term)
	}
	if _, err := node.log.GetEntry(5); !errors.Is(err, ErrLogCompacted) {
		t.Fatalf("expected ErrLogCompacted, got %v", err)
	}
	requireNoError(t, node.log.Append(LogEntry{Index: 6, Term: 2}))
}

func TestSnapshot_TakeSnapshotNoAppliedEntry(t *testing.T) {
	node, err := NewRaftNode(testConfig("node1", nil), &snapshotMockStateMachine{}, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	requireNoError(t, node.takeSnapshot())
	if node.snapshotMeta.LastIncludedIndex != 0 {
		t.Fatalf("expected no snapshot, got index %d", node.snapshotMeta.LastIncludedIndex)
	}
}

func TestSnapshot_InstallSingleChunk(t *testing.T) {
	stateMachine := &snapshotMockStateMachine{}
	node, err := NewRaftNode(testConfig("node1", nil), stateMachine, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	request := newInstallRequest(1, 10, 2, []byte("new-snapshot"))
	response, err := node.handleInstallSnapshot(request)
	requireNoError(t, err)
	if response.Term != 1 {
		t.Fatalf("expected response term 1, got %d", response.Term)
	}
	if node.CommitIndex() != 10 {
		t.Fatalf("expected commit index 10, got %d", node.CommitIndex())
	}
	if node.log.LastIndex() != 10 || node.log.LastTerm() != 2 {
		t.Fatalf("expected boundary 10/2, got %d/%d", node.log.LastIndex(), node.log.LastTerm())
	}
	if string(stateMachine.restored()) != "new-snapshot" {
		t.Fatalf("snapshot was not restored")
	}
	if node.Peers()["node2"] != "localhost:7002" {
		t.Fatalf("snapshot cluster configuration was not restored")
	}
}

func TestSnapshot_InstallRejectsOldTerm(t *testing.T) {
	node, err := NewRaftNode(testConfig("node1", nil), &snapshotMockStateMachine{}, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()
	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()

	response, err := node.handleInstallSnapshot(newInstallRequest(3, 10, 2, []byte("old")))
	requireNoError(t, err)
	if response.Term != 5 {
		t.Fatalf("expected current term 5, got %d", response.Term)
	}
	if node.snapshotMeta.LastIncludedIndex != 0 {
		t.Fatalf("old-term snapshot must not be installed")
	}
}

func TestSnapshot_InstallRetainsMatchingSuffix(t *testing.T) {
	node, err := NewRaftNode(testConfig("node1", nil), &snapshotMockStateMachine{}, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	for index := uint64(1); index <= 12; index++ {
		term := uint64(1)
		if index >= 10 {
			term = 2
		}
		if index >= 11 {
			term = 3
		}
		requireNoError(t, node.log.Append(LogEntry{Index: index, Term: term}))
	}

	_, err = node.handleInstallSnapshot(newInstallRequest(3, 10, 2, []byte("snapshot")))
	requireNoError(t, err)
	if node.log.FirstIndex() != 11 || node.log.LastIndex() != 12 {
		t.Fatalf("expected suffix 11..12, got first=%d last=%d", node.log.FirstIndex(), node.log.LastIndex())
	}
	entry, err := node.log.GetEntry(11)
	requireNoError(t, err)
	if entry.Term != 3 {
		t.Fatalf("expected retained suffix term 3, got %d", entry.Term)
	}
}

func TestSnapshot_InstallDiscardsMismatchedSuffix(t *testing.T) {
	node, err := NewRaftNode(testConfig("node1", nil), &snapshotMockStateMachine{}, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	for index := uint64(1); index <= 12; index++ {
		requireNoError(t, node.log.Append(LogEntry{Index: index, Term: 1}))
	}

	_, err = node.handleInstallSnapshot(newInstallRequest(3, 10, 2, []byte("snapshot")))
	requireNoError(t, err)
	if node.log.LastIndex() != 10 || node.log.LastTerm() != 2 {
		t.Fatalf("expected mismatched suffix to be discarded, got %d/%d", node.log.LastIndex(), node.log.LastTerm())
	}
	requireNoError(t, node.log.Append(LogEntry{Index: 11, Term: 3}))
}

func TestSnapshot_InstallChunksAreIdempotent(t *testing.T) {
	stateMachine := &snapshotMockStateMachine{}
	node, err := NewRaftNode(testConfig("node1", nil), stateMachine, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	fullRequest := newInstallRequest(2, 20, 4, []byte("abcdefghij"))
	first := snapshotChunk(fullRequest, 0, 4)
	second := snapshotChunk(fullRequest, 4, 8)
	last := snapshotChunk(fullRequest, 8, 10)

	_, err = node.handleInstallSnapshot(first)
	requireNoError(t, err)
	_, err = node.handleInstallSnapshot(first)
	requireNoError(t, err)
	_, err = node.handleInstallSnapshot(second)
	requireNoError(t, err)

	if node.snapshotMeta.LastIncludedIndex != 0 {
		t.Fatalf("partial snapshot became active")
	}
	_, err = node.handleInstallSnapshot(last)
	requireNoError(t, err)
	if string(stateMachine.restored()) != "abcdefghij" {
		t.Fatalf("chunked snapshot restored incorrect data")
	}
}

func TestSnapshot_InstallRejectsOutOfOrderChunk(t *testing.T) {
	node, err := NewRaftNode(testConfig("node1", nil), &snapshotMockStateMachine{}, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	request := newInstallRequest(2, 20, 4, []byte("abcdefgh"))
	_, err = node.handleInstallSnapshot(snapshotChunk(request, 4, 8))
	if !errors.Is(err, ErrSnapshotOffset) {
		t.Fatalf("expected ErrSnapshotOffset, got %v", err)
	}
	if node.snapshotMeta.LastIncludedIndex != 0 {
		t.Fatalf("out-of-order chunk changed active snapshot")
	}
}

func TestSnapshot_InstallRejectsConflictingDuplicate(t *testing.T) {
	node, err := NewRaftNode(testConfig("node1", nil), &snapshotMockStateMachine{}, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	request := newInstallRequest(2, 20, 4, []byte("abcdefgh"))
	first := snapshotChunk(request, 0, 4)
	_, err = node.handleInstallSnapshot(first)
	requireNoError(t, err)

	conflict := snapshotChunk(request, 0, 4)
	conflict.Data[0] = 'z'
	_, err = node.handleInstallSnapshot(conflict)
	if !errors.Is(err, ErrSnapshotChunkMismatch) {
		t.Fatalf("expected ErrSnapshotChunkMismatch, got %v", err)
	}
}

func TestSnapshot_InstallRejectsChecksumMismatch(t *testing.T) {
	node, err := NewRaftNode(testConfig("node1", nil), &snapshotMockStateMachine{}, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	request := newInstallRequest(2, 20, 4, []byte("abcdefgh"))
	request.Checksum[0] ^= 0xff
	_, err = node.handleInstallSnapshot(request)
	if !errors.Is(err, ErrSnapshotChecksum) {
		t.Fatalf("expected ErrSnapshotChecksum, got %v", err)
	}
	if node.snapshotMeta.LastIncludedIndex != 0 {
		t.Fatalf("corrupt snapshot became active")
	}
}

func TestSnapshot_LeaderAdvancesIndexesOnlyAfterFinalChunk(t *testing.T) {
	var node *RaftNode
	requestCount := 0
	transport := &mockTransportFull{
		installSnapshotFn: func(_ context.Context, peerID string, request *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
			requestCount++
			node.mu.RLock()
			nextIndex := node.nextIndex[peerID]
			matchIndex := node.matchIndex[peerID]
			node.mu.RUnlock()
			if !request.Done && (nextIndex != 1 || matchIndex != 0) {
				t.Fatalf("intermediate chunk advanced replication indexes")
			}
			return &InstallSnapshotResponse{Term: request.Term}, nil
		},
	}
	node = newLeaderNode(t, map[string]string{"node2": "addr2"}, transport)
	defer node.Stop()
	node.config.SnapshotChunkSize = 3
	node.mu.Lock()
	node.snapshotMeta = SnapshotMeta{
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		ClusterConfig: map[string]string{
			"node1": "",
			"node2": "addr2",
		},
	}
	node.snapshotData = []byte("abcdefgh")
	node.mu.Unlock()

	requireNoError(t, node.sendSnapshot(context.Background(), "node2"))
	if requestCount != 3 {
		t.Fatalf("expected 3 chunks, got %d", requestCount)
	}
	node.mu.RLock()
	defer node.mu.RUnlock()
	if node.nextIndex["node2"] != 11 || node.matchIndex["node2"] != 10 {
		t.Fatalf("final chunk did not advance indexes: next=%d match=%d", node.nextIndex["node2"], node.matchIndex["node2"])
	}
}

func TestSnapshot_ApplyAndSnapshotUseSameCut(t *testing.T) {
	stateMachine := &snapshotMockStateMachine{
		applyStarted: make(chan struct{}, 1),
		releaseApply: make(chan struct{}),
	}
	node, err := NewRaftNode(testConfig("node1", nil), stateMachine, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	requireNoError(t, node.log.Append(LogEntry{Index: 1, Term: 1, Command: []byte("after-apply")}))
	node.mu.Lock()
	node.commitIndex = 1
	node.mu.Unlock()
	node.commitCh <- struct{}{}

	select {
	case <-stateMachine.applyStarted:
	case <-time.After(time.Second):
		t.Fatal("apply did not start")
	}

	snapshotDone := make(chan error, 1)
	go func() {
		snapshotDone <- node.takeSnapshot()
	}()

	select {
	case err := <-snapshotDone:
		t.Fatalf("snapshot completed during in-flight apply: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(stateMachine.releaseApply)
	select {
	case err := <-snapshotDone:
		requireNoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("snapshot did not complete")
	}

	if node.snapshotMeta.LastIncludedIndex != 1 || string(node.snapshotData) != "after-apply" {
		t.Fatalf("snapshot cut does not match applied state")
	}
}

func TestSnapshot_ArtifactDetectsCorruption(t *testing.T) {
	meta := SnapshotMeta{
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		ClusterConfig:     map[string]string{"node1": ""},
	}
	artifact, err := encodeSnapshotArtifact(meta, []byte("snapshot"))
	requireNoError(t, err)
	artifact[len(artifact)-1] ^= 0xff

	if _, _, err := decodeSnapshotArtifact(artifact); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected invalid snapshot error, got %v", err)
	}
}

func TestSnapshot_PersistLoadAndAppend(t *testing.T) {
	cfg := testConfig("node1", nil)
	cfg.DataDir = t.TempDir()
	cfg.SyncWrites = true

	stateMachine := &snapshotMockStateMachine{snapshotData: []byte("disk-snapshot")}
	node, err := NewRaftNode(cfg, stateMachine, &mockTransport{})
	requireNoError(t, err)
	for index := uint64(1); index <= 5; index++ {
		requireNoError(t, node.log.Append(LogEntry{Index: index, Term: 2}))
	}
	node.mu.Lock()
	node.lastApplied = 5
	node.mu.Unlock()
	requireNoError(t, node.takeSnapshot())
	node.Stop()

	restoredStateMachine := &snapshotMockStateMachine{}
	restarted, err := NewRaftNode(cfg, restoredStateMachine, &mockTransport{})
	requireNoError(t, err)
	defer restarted.Stop()

	if restarted.log.LastIndex() != 5 || restarted.log.LastTerm() != 2 {
		t.Fatalf("expected restarted boundary 5/2, got %d/%d", restarted.log.LastIndex(), restarted.log.LastTerm())
	}
	if string(restoredStateMachine.restored()) != "disk-snapshot" {
		t.Fatalf("snapshot data was not restored")
	}
	requireNoError(t, restarted.log.Append(LogEntry{Index: 6, Term: 3}))
}

func TestSnapshot_LoadsAndMigratesLegacyFiles(t *testing.T) {
	cfg := testConfig("node1", nil)
	cfg.DataDir = t.TempDir()
	meta := SnapshotMeta{
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
		ClusterConfig:     map[string]string{"node1": ""},
	}
	metadata, err := json.Marshal(meta)
	requireNoError(t, err)
	requireNoError(t, os.WriteFile(filepath.Join(cfg.DataDir, "node1_snapshot_meta.json"), metadata, 0o644))
	requireNoError(t, os.WriteFile(filepath.Join(cfg.DataDir, "node1_snapshot.bin"), []byte("legacy-data"), 0o644))

	stateMachine := &snapshotMockStateMachine{}
	node, err := NewRaftNode(cfg, stateMachine, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	artifact, err := os.ReadFile(node.snapshotDataPath())
	requireNoError(t, err)
	if !bytes.HasPrefix(artifact, snapshotMagic[:]) {
		t.Fatalf("legacy snapshot was not migrated")
	}
	if string(stateMachine.restored()) != "legacy-data" {
		t.Fatalf("legacy snapshot was not restored")
	}
}

func TestSnapshot_FinalPersistenceFailureDoesNotActivateSnapshot(t *testing.T) {
	cfg := testConfig("node1", nil)
	cfg.DataDir = t.TempDir()
	stateMachine := &snapshotMockStateMachine{}
	node, err := NewRaftNode(cfg, stateMachine, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	request := newInstallRequest(2, 10, 2, []byte("abcdefgh"))
	_, err = node.handleInstallSnapshot(snapshotChunk(request, 0, 4))
	requireNoError(t, err)

	cfg.DataDir = filepath.Join(cfg.DataDir, "missing", "nested")
	_, err = node.handleInstallSnapshot(snapshotChunk(request, 4, 8))
	if err == nil {
		t.Fatalf("expected final snapshot persistence failure")
	}
	if node.snapshotMeta.LastIncludedIndex != 0 || node.log.LastIndex() != 0 {
		t.Fatalf("failed persistence activated snapshot or changed log")
	}
	if len(stateMachine.restored()) != 0 {
		t.Fatalf("failed persistence restored state machine")
	}
}

func TestSnapshot_RestoreFailureQuarantinesNodeAndRecoversOnRestart(t *testing.T) {
	cfg := testConfig("node1", nil)
	cfg.DataDir = t.TempDir()
	stateMachine := &snapshotMockStateMachine{restoreErr: errors.New("restore failed")}
	node, err := NewRaftNode(cfg, stateMachine, &mockTransport{})
	requireNoError(t, err)

	_, err = node.handleInstallSnapshot(newInstallRequest(2, 10, 2, []byte("snapshot")))
	if err == nil {
		t.Fatalf("expected restore failure")
	}
	node.mu.RLock()
	fatalErr := node.fatalErr
	node.mu.RUnlock()
	if !errors.Is(fatalErr, ErrNodeUnhealthy) {
		t.Fatalf("restore failure did not quarantine node: %v", fatalErr)
	}
	if err := node.Propose([]byte("must-fail")); !errors.Is(err, ErrNodeUnhealthy) {
		t.Fatalf("unhealthy node accepted proposal: %v", err)
	}
	node.Stop()

	recoveredStateMachine := &snapshotMockStateMachine{}
	restarted, err := NewRaftNode(cfg, recoveredStateMachine, &mockTransport{})
	requireNoError(t, err)
	defer restarted.Stop()
	if string(recoveredStateMachine.restored()) != "snapshot" {
		t.Fatalf("durable snapshot did not recover after restart")
	}
	requireNoError(t, restarted.log.Append(LogEntry{Index: 11, Term: 3}))
}

func TestSnapshot_LeaderDoesNotAdvanceIndexesOnInstallError(t *testing.T) {
	transport := &mockTransportFull{
		installSnapshotFn: func(context.Context, string, *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
			return nil, errors.New("network failure")
		},
	}
	node := newLeaderNode(t, map[string]string{"node2": "addr2"}, transport)
	defer node.Stop()
	node.mu.Lock()
	node.snapshotMeta = SnapshotMeta{
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		ClusterConfig: map[string]string{
			"node1": "",
			"node2": "addr2",
		},
	}
	node.snapshotData = []byte("snapshot")
	node.mu.Unlock()

	if err := node.sendSnapshot(context.Background(), "node2"); err == nil {
		t.Fatalf("expected InstallSnapshot transport error")
	}
	node.mu.RLock()
	defer node.mu.RUnlock()
	if node.nextIndex["node2"] != 1 || node.matchIndex["node2"] != 0 {
		t.Fatalf("failed InstallSnapshot advanced indexes")
	}
}

func TestSnapshot_StartupRemovesIncompleteStagingFile(t *testing.T) {
	cfg := testConfig("node1", nil)
	cfg.DataDir = t.TempDir()
	stagingPath := filepath.Join(cfg.DataDir, "node1_snapshot_install.tmp")
	requireNoError(t, os.WriteFile(stagingPath, []byte("partial"), 0o644))

	node, err := NewRaftNode(cfg, &snapshotMockStateMachine{}, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()
	if _, err := os.Stat(stagingPath); !os.IsNotExist(err) {
		t.Fatalf("incomplete staging file survived restart: %v", err)
	}
}

func TestSnapshot_RestartReconcilesPersistedSnapshotWithOldWAL(t *testing.T) {
	t.Run("matching boundary retains suffix", func(t *testing.T) {
		cfg := testConfig("node1", nil)
		cfg.DataDir = t.TempDir()
		cfg.SyncWrites = true
		node, err := NewRaftNode(cfg, &snapshotMockStateMachine{}, &mockTransport{})
		requireNoError(t, err)
		for index := uint64(1); index <= 6; index++ {
			term := uint64(1)
			if index >= 5 {
				term = 2
			}
			requireNoError(t, node.log.Append(LogEntry{Index: index, Term: term}))
		}
		meta := SnapshotMeta{
			LastIncludedIndex: 5,
			LastIncludedTerm:  2,
			ClusterConfig:     map[string]string{"node1": ""},
		}
		requireNoError(t, node.persistSnapshot(meta, []byte("snapshot")))
		node.Stop()

		restarted, err := NewRaftNode(cfg, &snapshotMockStateMachine{}, &mockTransport{})
		requireNoError(t, err)
		defer restarted.Stop()
		if restarted.log.FirstIndex() != 6 || restarted.log.LastIndex() != 6 {
			t.Fatalf("matching suffix was not retained after crash-window recovery")
		}
	})

	t.Run("mismatched boundary discards suffix", func(t *testing.T) {
		cfg := testConfig("node1", nil)
		cfg.DataDir = t.TempDir()
		cfg.SyncWrites = true
		node, err := NewRaftNode(cfg, &snapshotMockStateMachine{}, &mockTransport{})
		requireNoError(t, err)
		for index := uint64(1); index <= 6; index++ {
			requireNoError(t, node.log.Append(LogEntry{Index: index, Term: 1}))
		}
		meta := SnapshotMeta{
			LastIncludedIndex: 5,
			LastIncludedTerm:  2,
			ClusterConfig:     map[string]string{"node1": ""},
		}
		requireNoError(t, node.persistSnapshot(meta, []byte("snapshot")))
		node.Stop()

		restarted, err := NewRaftNode(cfg, &snapshotMockStateMachine{}, &mockTransport{})
		requireNoError(t, err)
		defer restarted.Stop()
		if restarted.log.FirstIndex() != 6 || restarted.log.LastIndex() != 5 || restarted.log.LastTerm() != 2 {
			t.Fatalf("mismatched suffix survived crash-window recovery")
		}
		requireNoError(t, restarted.log.Append(LogEntry{Index: 6, Term: 3}))
	})
}

func TestSnapshot_RepeatedInstallIsIdempotent(t *testing.T) {
	stateMachine := &snapshotMockStateMachine{}
	node, err := NewRaftNode(testConfig("node1", nil), stateMachine, &mockTransport{})
	requireNoError(t, err)
	defer node.Stop()

	request := newInstallRequest(2, 10, 2, []byte("snapshot"))
	_, err = node.handleInstallSnapshot(request)
	requireNoError(t, err)
	_, err = node.handleInstallSnapshot(request)
	requireNoError(t, err)

	stateMachine.mu.Lock()
	restoreCount := stateMachine.restoreCount
	stateMachine.mu.Unlock()
	if restoreCount != 1 {
		t.Fatalf("repeated snapshot restored state machine %d times", restoreCount)
	}
	if node.log.LastIndex() != 10 || node.log.LastTerm() != 2 {
		t.Fatalf("repeated snapshot changed boundary")
	}
}
