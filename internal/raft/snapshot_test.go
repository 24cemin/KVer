package raft

import (
	"os"
	"testing"
)

type snapshotMockStateMachine struct {
	snapshotData []byte
	restoredData []byte
	applyCount   int
}

func (m *snapshotMockStateMachine) Apply(entry LogEntry) error {
	m.applyCount++
	return nil
}

func (m *snapshotMockStateMachine) Snapshot() ([]byte, error) {
	return m.snapshotData, nil
}

func (m *snapshotMockStateMachine) Restore(data []byte) error {
	m.restoredData = append([]byte(nil), data...)
	return nil
}

func TestSnapshot_TakeSnapshot(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		cfg := testConfig("node1", nil)
		sm := &snapshotMockStateMachine{snapshotData: []byte("snap-data")}
		node, _ := NewRaftNode(cfg, sm, &mockTransport{})
		node.mu.Lock()
		node.state = Leader
		node.lastApplied = 5
		node.mu.Unlock()

		// log'a 5 entry append et
		for i := 1; i <= 5; i++ {
			_ = node.log.Append(LogEntry{Index: uint64(i), Term: 1})
		}

		err := node.takeSnapshot()
		if err != nil {
			t.Fatalf("takeSnapshot failed: %v", err)
		}

		node.mu.RLock()
		defer node.mu.RUnlock()

		if node.snapshotMeta.LastIncludedIndex != 5 {
			t.Errorf("expected LastIncludedIndex 5, got %d", node.snapshotMeta.LastIncludedIndex)
		}
		if string(node.snapshotData) != "snap-data" {
			t.Errorf("expected snapshotData 'snap-data', got %s", node.snapshotData)
		}
		if node.log.FirstIndex() != 6 {
			t.Errorf("expected log.FirstIndex 6, got %d", node.log.FirstIndex())
		}
	})

	t.Run("NoApplied", func(t *testing.T) {
		cfg := testConfig("node1", nil)
		sm := &snapshotMockStateMachine{}
		node, _ := NewRaftNode(cfg, sm, &mockTransport{})

		err := node.takeSnapshot()
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}

		node.mu.RLock()
		defer node.mu.RUnlock()
		if node.snapshotMeta.LastIncludedIndex != 0 {
			t.Errorf("expected 0, got %d", node.snapshotMeta.LastIncludedIndex)
		}
	})
}

func TestSnapshot_HandleInstallSnapshot(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		cfg := testConfig("node1", nil)
		sm := &snapshotMockStateMachine{}
		node, _ := NewRaftNode(cfg, sm, &mockTransport{})

		req := &InstallSnapshotRequest{
			Term:              1,
			LeaderID:          "node2",
			LastIncludedIndex: 10,
			LastIncludedTerm:  1,
			Data:              []byte("new-snap"),
		}

		resp := node.handleInstallSnapshot(req)
		if resp.Term != req.Term {
			t.Errorf("expected response term %d, got %d", req.Term, resp.Term)
		}

		node.mu.RLock()
		defer node.mu.RUnlock()

		if node.commitIndex != 10 {
			t.Errorf("expected commitIndex 10, got %d", node.commitIndex)
		}
		if node.lastApplied != 10 {
			t.Errorf("expected lastApplied 10, got %d", node.lastApplied)
		}
		if node.snapshotMeta.LastIncludedIndex != 10 {
			t.Errorf("expected LastIncludedIndex 10, got %d", node.snapshotMeta.LastIncludedIndex)
		}
		if string(node.snapshotData) != "new-snap" {
			t.Errorf("expected 'new-snap', got %s", node.snapshotData)
		}
		if string(sm.restoredData) != "new-snap" {
			t.Errorf("expected restoredData 'new-snap', got %s", sm.restoredData)
		}
	})

	t.Run("OldTerm", func(t *testing.T) {
		cfg := testConfig("node1", nil)
		node, _ := NewRaftNode(cfg, &snapshotMockStateMachine{}, &mockTransport{})
		node.mu.Lock()
		node.currentTerm = 5
		node.mu.Unlock()

		req := &InstallSnapshotRequest{
			Term:              3,
			LastIncludedIndex: 10,
		}

		_ = node.handleInstallSnapshot(req)

		node.mu.RLock()
		defer node.mu.RUnlock()
		if node.snapshotMeta.LastIncludedIndex == 10 {
			t.Errorf("should not apply snapshot with older term")
		}
	})

	t.Run("OldSnapshot", func(t *testing.T) {
		cfg := testConfig("node1", nil)
		node, _ := NewRaftNode(cfg, &snapshotMockStateMachine{}, &mockTransport{})
		node.mu.Lock()
		node.snapshotMeta.LastIncludedIndex = 10
		node.mu.Unlock()

		req := &InstallSnapshotRequest{
			Term:              0,
			LastIncludedIndex: 5,
		}

		_ = node.handleInstallSnapshot(req)

		node.mu.RLock()
		defer node.mu.RUnlock()
		if node.snapshotMeta.LastIncludedIndex != 10 {
			t.Errorf("should not apply older snapshot")
		}
	})
}

func TestSnapshot_CompactUpTo(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		l := newRaftLog()
		for i := 1; i <= 10; i++ {
			_ = l.Append(LogEntry{Index: uint64(i)})
		}

		err := l.CompactUpTo(5)
		if err != nil {
			t.Fatalf("CompactUpTo failed: %v", err)
		}

		if l.FirstIndex() != 6 {
			t.Errorf("expected FirstIndex 6, got %d", l.FirstIndex())
		}

		_, err = l.GetEntry(5)
		if err != ErrOutOfRange {
			t.Errorf("expected ErrOutOfRange for index 5, got %v", err)
		}

		entry, err := l.GetEntry(6)
		if err != nil || entry.Index != 6 {
			t.Errorf("expected entry 6, got err %v", err)
		}
	})

	t.Run("AlreadyCompacted", func(t *testing.T) {
		l := newRaftLog()
		for i := 1; i <= 5; i++ {
			_ = l.Append(LogEntry{Index: uint64(i)})
		}

		_ = l.CompactUpTo(3)
		if l.FirstIndex() != 4 {
			t.Errorf("expected FirstIndex 4, got %d", l.FirstIndex())
		}

		err := l.CompactUpTo(2)
		if err != nil {
			t.Fatalf("expected nil from already compacted")
		}
		if l.FirstIndex() != 4 {
			t.Errorf("expected FirstIndex to remain 4, got %d", l.FirstIndex())
		}
	})
}

func TestSnapshot_PersistAndLoad(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		cfg := testConfig("node1", nil)
		cfg.DataDir = t.TempDir()

		sm := &snapshotMockStateMachine{snapshotData: []byte("disk-snap")}
		node, _ := NewRaftNode(cfg, sm, &mockTransport{})
		node.mu.Lock()
		node.state = Leader
		node.lastApplied = 5
		node.mu.Unlock()

		for i := 1; i <= 5; i++ {
			_ = node.log.Append(LogEntry{Index: uint64(i), Term: 1})
		}

		if err := node.takeSnapshot(); err != nil {
			t.Fatalf("takeSnapshot err: %v", err)
		}

		// snapshotMeta içinde clusterConfig var mı kontrol et
		if len(node.snapshotMeta.ClusterConfig) == 0 {
			t.Error("snapshotMeta should contain clusterConfig")
		}

		// Dosya kontrolü
		if _, err := os.Stat(node.snapshotMetaPath()); os.IsNotExist(err) {
			t.Errorf("meta file not created")
		}
		if _, err := os.Stat(node.snapshotDataPath()); os.IsNotExist(err) {
			t.Errorf("data file not created")
		}

		// Yeni node (restart)
		node.Stop()
		newNode, _ := NewRaftNode(cfg, &snapshotMockStateMachine{}, &mockTransport{})
		newNode.mu.RLock()
		defer newNode.mu.RUnlock()

		// Yeni node yüklenince clusterConfig restore edildi mi kontrol et
		if newNode.clusterConfig.Size() == 0 {
			t.Error("clusterConfig should be restored from snapshot")
		}

		if newNode.snapshotMeta.LastIncludedIndex != 5 {
			t.Errorf("loaded index expected 5, got %d", newNode.snapshotMeta.LastIncludedIndex)
		}
		if newNode.log.FirstIndex() != 6 {
			t.Errorf("loaded firstIndex expected 6, got %d", newNode.log.FirstIndex())
		}
		if string(newNode.snapshotData) != "disk-snap" {
			t.Errorf("loaded data expected 'disk-snap', got %s", newNode.snapshotData)
		}
	})
}

func TestSnapshot_WAL_PersistAndReload(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		dir := t.TempDir()
		l, _ := newRaftLogWithWAL(dir, "test1", false)

		for i := 1; i <= 3; i++ {
			_ = l.Append(LogEntry{Index: uint64(i), Term: uint64(i)})
		}

		// Yeniden yarat -> diskten yükle
		l2, _ := newRaftLogWithWAL(dir, "test1", false)
		if l2.LastIndex() != 3 {
			t.Errorf("expected LastIndex 3, got %d", l2.LastIndex())
		}

		entry, _ := l2.GetEntry(2)
		if entry.Term != 2 {
			t.Errorf("expected term 2, got %d", entry.Term)
		}
	})
}

func TestSnapshot_WAL_TruncateAndReload(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		dir := t.TempDir()
		l, _ := newRaftLogWithWAL(dir, "test1", false)

		for i := 1; i <= 5; i++ {
			_ = l.Append(LogEntry{Index: uint64(i), Term: uint64(i)})
		}

		_ = l.TruncateAfter(3) // 4 ve 5 silindi

		l2, _ := newRaftLogWithWAL(dir, "test1", false)
		if l2.LastIndex() != 3 {
			t.Errorf("expected LastIndex 3, got %d", l2.LastIndex())
		}

		_, err := l2.GetEntry(4)
		if err != ErrOutOfRange {
			t.Errorf("expected ErrOutOfRange for entry 4, got %v", err)
		}
	})
}
