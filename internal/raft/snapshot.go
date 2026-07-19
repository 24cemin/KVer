// Package raft — snapshot.go
// Snapshot alma, gönderme ve uygulama işlemleri.
// Log compaction bu mekanizma ile sağlanır.
package raft

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// SnapshotMeta, bir snapshot'ın meta bilgilerini tutar.
type SnapshotMeta struct {
	// LastIncludedIndex, snapshot'ın kapsadığı son log index'i.
	LastIncludedIndex uint64            `json:"last_included_index"`
	// LastIncludedTerm, LastIncludedIndex'teki entry'nin term'i.
	LastIncludedTerm  uint64            `json:"last_included_term"`
	ClusterConfig     map[string]string `json:"cluster_config"`
}

// snapshotDataPath, snapshot verisinin dosya yolunu döndürür.
func (r *RaftNode) snapshotDataPath() string {
	return filepath.Join(r.config.DataDir, r.config.NodeID+"_snapshot.bin")
}

// snapshotMetaPath, snapshot meta dosyasının yolunu döndürür.
func (r *RaftNode) snapshotMetaPath() string {
	return filepath.Join(r.config.DataDir, r.config.NodeID+"_snapshot_meta.json")
}

// persistSnapshot, state machine snapshot'ını atomic olarak diske yazar.
func (r *RaftNode) persistSnapshot(meta SnapshotMeta, data []byte) error {
	if r.config.DataDir == "" {
		return nil // in-memory mod
	}
	// Meta yaz
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := atomicWrite(r.snapshotMetaPath(), metaBytes); err != nil {
		return err
	}
	// Data yaz
	return atomicWrite(r.snapshotDataPath(), data)
}

// atomicWrite, veriyi atomic olarak diske yazar.
func atomicWrite(path string, data []byte) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// loadSnapshotFromDisk, diskteki snapshot'ı yükler.
func (r *RaftNode) loadSnapshotFromDisk() error {
	if r.config.DataDir == "" {
		return nil // in-memory mod
	}
	metaPath := r.snapshotMetaPath()
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // snapshot yok
		}
		return err
	}
	var meta SnapshotMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return err
	}
	data, err := os.ReadFile(r.snapshotDataPath())
	if err != nil {
		return err
	}
	r.snapshotMeta = meta
	r.snapshotData = data
	// State machine'i restore et
	if err := r.stateMachine.Restore(data); err != nil {
		return err
	}
	// commitIndex ve lastApplied güncelle
	if meta.LastIncludedIndex > r.commitIndex {
		r.commitIndex = meta.LastIncludedIndex
	}
	if meta.LastIncludedIndex > r.lastApplied {
		r.lastApplied = meta.LastIncludedIndex
	}

	if len(meta.ClusterConfig) > 0 {
		r.clusterConfig = &ClusterConfig{
			Peers: meta.ClusterConfig,
		}
	}

	// [Kritik Düzenleme Onaylı]: WAL içerisinde compaction yüzünden sadece [] varsa,
	// FirstIndex değerini 1 zanneder. Snapshot ile senkronize etmek lazım:
	if meta.LastIncludedIndex >= r.log.FirstIndex() {
		r.log.SetFirstIndex(meta.LastIncludedIndex + 1)
	}

	return nil
}

// takeSnapshot, state machine'den snapshot alır ve log'u compact eder.
func (r *RaftNode) takeSnapshot() error {
	r.mu.RLock()
	lastApplied := r.lastApplied
	r.mu.RUnlock()

	if lastApplied == 0 {
		return nil // henüz hiçbir şey apply edilmedi
	}

	// State machine'den snapshot al
	data, err := r.stateMachine.Snapshot()
	if err != nil {
		return err
	}

	r.mu.Lock()

	// Eğer lock beklerken başka bir snapshot alınmışsa veya lastApplied geride kaldıysa abort
	if lastApplied <= r.snapshotMeta.LastIncludedIndex {
		r.mu.Unlock()
		return nil
	}

	// Snapshot meta bilgisi
	entry, err := r.log.GetEntry(lastApplied)
	if err != nil {
		r.mu.Unlock()
		return err
	}

	clusterConfigSnapshot := make(map[string]string)
	for k, v := range r.clusterConfig.Peers {
		clusterConfigSnapshot[k] = v
	}

	meta := SnapshotMeta{
		LastIncludedIndex: lastApplied,
		LastIncludedTerm:  entry.Term,
		ClusterConfig:     clusterConfigSnapshot,
	}
	r.snapshotMeta = meta
	r.snapshotData = append([]byte(nil), data...)
	r.mu.Unlock()

	if err := r.persistSnapshot(meta, data); err != nil {
		return err
	}

	// Log'u compactle
	return r.log.CompactUpTo(lastApplied)
}

// sendSnapshot, geride kalan bir follower'a InstallSnapshot RPC gönderir.
// Bu follower'ın log'u çok geride kaldığında AppendEntries yerine kullanılır.
func (r *RaftNode) sendSnapshot(ctx context.Context, peerID string) error {
	r.mu.RLock()
	term := r.currentTerm
	meta := r.snapshotMeta
	data := r.snapshotData
	r.mu.RUnlock()

	if data == nil {
		return nil // snapshot yok
	}

	req := &InstallSnapshotRequest{
		Term:              term,
		LeaderID:          r.config.NodeID,
		LastIncludedIndex: meta.LastIncludedIndex,
		LastIncludedTerm:  meta.LastIncludedTerm,
		Data:              data,
	}

	rpcCtx, cancel := context.WithTimeout(ctx, r.config.HeartbeatInterval*5)
	defer cancel()

	resp, err := r.transport.InstallSnapshot(rpcCtx, peerID, req)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if resp.Term > r.currentTerm {
		r.stepDown(resp.Term)
		return nil
	}

	// Başarılıysa nextIndex güncelle
	if meta.LastIncludedIndex+1 > r.nextIndex[peerID] {
		r.nextIndex[peerID] = meta.LastIncludedIndex + 1
	}
	if meta.LastIncludedIndex > r.matchIndex[peerID] {
		r.matchIndex[peerID] = meta.LastIncludedIndex
	}

	return nil
}

// handleInstallSnapshot, gelen InstallSnapshot RPC'yi işler (follower tarafı).
func (r *RaftNode) handleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	r.mu.Lock()

	if req == nil {
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: r.currentTerm}
	}

	if req.Term < r.currentTerm {
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: r.currentTerm}
	}

	if req.Term > r.currentTerm {
		r.stepDown(req.Term)
	}
	r.lastHeartbeat = time.Now()

	// Snapshot zaten elimizdekinden eski ise yoksay
	if req.LastIncludedIndex <= r.snapshotMeta.LastIncludedIndex {
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: r.currentTerm}
	}

	// Log'u güncelle
	if req.LastIncludedIndex <= r.log.LastIndex() {
		_ = r.log.CompactUpTo(req.LastIncludedIndex)
	} else {
		// Tüm log'u sil, firstIndex'i güncelle
		_ = r.log.CompactUpTo(r.log.LastIndex())
		r.log.SetFirstIndex(req.LastIncludedIndex + 1)
	}

	// Snapshot meta ve data'yı güncelle
	r.snapshotMeta = SnapshotMeta{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
	}
	r.snapshotData = append([]byte(nil), req.Data...)

	if len(r.snapshotMeta.ClusterConfig) > 0 {
		r.clusterConfig = &ClusterConfig{
			Peers: r.snapshotMeta.ClusterConfig,
		}
	}

	if err := r.persistSnapshot(r.snapshotMeta, r.snapshotData); err != nil {
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: r.currentTerm}
	}

	// commitIndex ve lastApplied güncelle
	if req.LastIncludedIndex > r.commitIndex {
		r.commitIndex = req.LastIncludedIndex
	}
	if req.LastIncludedIndex > r.lastApplied {
		r.lastApplied = req.LastIncludedIndex
	}

	// State machine'i restore et
	r.mu.Unlock()
	err := r.stateMachine.Restore(req.Data)
	r.mu.Lock()
	if err != nil {
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: r.currentTerm}
	}

	r.mu.Unlock()
	return &InstallSnapshotResponse{Term: r.currentTerm}
}
