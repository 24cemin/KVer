// Package raft — snapshot.go
// Snapshot alma, gönderme ve uygulama işlemleri.
// Log compaction bu mekanizma ile sağlanır.
package raft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

var snapshotMagic = [8]byte{'K', 'V', 'E', 'R', 'S', 'N', 'P', '1'}

const (
	snapshotFormatVersion    = uint32(1)
	defaultSnapshotChunkSize = 256 << 10
	defaultMaxSnapshotSize   = uint64(4 << 30)
	maxSnapshotMetadataSize  = 16 << 20
)

// SnapshotMeta, bir snapshot'ın meta bilgilerini tutar.
type SnapshotMeta struct {
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	ClusterConfig     map[string]string `json:"cluster_config"`
}

type installSnapshotState struct {
	meta       SnapshotMeta
	checksum   [sha256.Size]byte
	totalSize  uint64
	nextOffset uint64
	file       *os.File
	data       []byte
	path       string
}

func (r *RaftNode) snapshotDataPath() string {
	return filepath.Join(r.config.DataDir, r.config.NodeID+"_snapshot.bin")
}

func (r *RaftNode) snapshotMetaPath() string {
	return filepath.Join(r.config.DataDir, r.config.NodeID+"_snapshot_meta.json")
}

func (r *RaftNode) snapshotInstallPath() string {
	return filepath.Join(r.config.DataDir, r.config.NodeID+"_snapshot_install.tmp")
}

func (r *RaftNode) persistSnapshot(meta SnapshotMeta, data []byte) error {
	if r.config.DataDir == "" {
		return nil
	}
	artifact, err := encodeSnapshotArtifact(meta, data)
	if err != nil {
		return err
	}
	return atomicWrite(r.snapshotDataPath(), artifact)
}

func encodeSnapshotArtifact(meta SnapshotMeta, data []byte) ([]byte, error) {
	meta.ClusterConfig = cloneStringMap(meta.ClusterConfig)
	metadata, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	if len(metadata) > maxSnapshotMetadataSize {
		return nil, fmt.Errorf("%w: metadata exceeds %d bytes", ErrInvalidSnapshot, maxSnapshotMetadataSize)
	}

	checksum := sha256.New()
	_, _ = checksum.Write(metadata)
	_, _ = checksum.Write(data)

	var buffer bytes.Buffer
	_, _ = buffer.Write(snapshotMagic[:])
	if err := binary.Write(&buffer, binary.LittleEndian, snapshotFormatVersion); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, uint32(len(metadata))); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, uint64(len(data))); err != nil {
		return nil, err
	}
	_, _ = buffer.Write(checksum.Sum(nil))
	_, _ = buffer.Write(metadata)
	_, _ = buffer.Write(data)
	return buffer.Bytes(), nil
}

func decodeSnapshotArtifact(artifact []byte) (SnapshotMeta, []byte, error) {
	const headerSize = 8 + 4 + 4 + 8 + sha256.Size
	if len(artifact) < headerSize || !bytes.Equal(artifact[:8], snapshotMagic[:]) {
		return SnapshotMeta{}, nil, ErrInvalidSnapshot
	}
	version := binary.LittleEndian.Uint32(artifact[8:12])
	if version != snapshotFormatVersion {
		return SnapshotMeta{}, nil, fmt.Errorf("%w: unsupported format version %d", ErrInvalidSnapshot, version)
	}
	metadataLength := binary.LittleEndian.Uint32(artifact[12:16])
	dataLength := binary.LittleEndian.Uint64(artifact[16:24])
	if metadataLength > maxSnapshotMetadataSize {
		return SnapshotMeta{}, nil, fmt.Errorf("%w: metadata exceeds limit", ErrInvalidSnapshot)
	}
	expectedSize := uint64(headerSize) + uint64(metadataLength) + dataLength
	if expectedSize != uint64(len(artifact)) {
		return SnapshotMeta{}, nil, fmt.Errorf("%w: artifact length mismatch", ErrInvalidSnapshot)
	}

	expectedChecksum := artifact[24:headerSize]
	metadataEnd := headerSize + int(metadataLength)
	metadata := artifact[headerSize:metadataEnd]
	data := artifact[metadataEnd:]

	checksum := sha256.New()
	_, _ = checksum.Write(metadata)
	_, _ = checksum.Write(data)
	if !bytes.Equal(expectedChecksum, checksum.Sum(nil)) {
		return SnapshotMeta{}, nil, fmt.Errorf("%w: checksum mismatch", ErrInvalidSnapshot)
	}

	var meta SnapshotMeta
	if err := json.Unmarshal(metadata, &meta); err != nil {
		return SnapshotMeta{}, nil, fmt.Errorf("%w: invalid metadata: %v", ErrInvalidSnapshot, err)
	}
	if err := validateSnapshotMeta(meta); err != nil {
		return SnapshotMeta{}, nil, err
	}
	return meta, append([]byte(nil), data...), nil
}

func validateSnapshotMeta(meta SnapshotMeta) error {
	if meta.LastIncludedIndex == 0 || meta.LastIncludedTerm == 0 {
		return fmt.Errorf("%w: snapshot boundary must be non-zero", ErrInvalidSnapshot)
	}
	if len(meta.ClusterConfig) == 0 {
		return fmt.Errorf("%w: cluster configuration is missing", ErrInvalidSnapshot)
	}
	return nil
}

func atomicWrite(path string, data []byte) error {
	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	closeWithError := func(writeErr error) error {
		closeErr := file.Close()
		_ = os.Remove(tempPath)
		return errors.Join(writeErr, closeErr)
	}
	if _, err := file.Write(data); err != nil {
		return closeWithError(err)
	}
	if err := file.Sync(); err != nil {
		return closeWithError(err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func (r *RaftNode) loadSnapshotArtifact() (SnapshotMeta, []byte, bool, bool, error) {
	if r.config.DataDir == "" {
		return SnapshotMeta{}, nil, false, false, nil
	}
	artifact, err := os.ReadFile(r.snapshotDataPath())
	if err != nil {
		if os.IsNotExist(err) {
			if _, metaErr := os.Stat(r.snapshotMetaPath()); metaErr == nil {
				return SnapshotMeta{}, nil, false, false, fmt.Errorf("%w: snapshot metadata exists without data", ErrInvalidSnapshot)
			}
			return SnapshotMeta{}, nil, false, false, nil
		}
		return SnapshotMeta{}, nil, false, false, err
	}
	if len(artifact) >= len(snapshotMagic) && bytes.Equal(artifact[:len(snapshotMagic)], snapshotMagic[:]) {
		meta, data, decodeErr := decodeSnapshotArtifact(artifact)
		return meta, data, true, false, decodeErr
	}

	metadata, err := os.ReadFile(r.snapshotMetaPath())
	if err != nil {
		return SnapshotMeta{}, nil, false, false, fmt.Errorf("%w: legacy snapshot metadata is missing: %v", ErrInvalidSnapshot, err)
	}
	var meta SnapshotMeta
	if err := json.Unmarshal(metadata, &meta); err != nil {
		return SnapshotMeta{}, nil, false, false, fmt.Errorf("%w: invalid legacy metadata: %v", ErrInvalidSnapshot, err)
	}
	if len(meta.ClusterConfig) == 0 {
		r.mu.RLock()
		meta.ClusterConfig = cloneStringMap(r.clusterConfig.Peers)
		r.mu.RUnlock()
	}
	if err := validateSnapshotMeta(meta); err != nil {
		return SnapshotMeta{}, nil, false, false, err
	}
	return meta, artifact, true, true, nil
}

func (r *RaftNode) loadSnapshotFromDisk() error {
	if r.config.DataDir != "" {
		for _, path := range []string{r.snapshotInstallPath(), r.snapshotDataPath() + ".tmp"} {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	meta, data, found, legacy, err := r.loadSnapshotArtifact()
	if err != nil {
		return err
	}
	if !found {
		baseIndex, _, boundaryKnown := r.log.Boundary()
		if baseIndex > 0 && !boundaryKnown {
			return fmt.Errorf("%w: compacted legacy WAL has no snapshot metadata", ErrCorruptLog)
		}
		return nil
	}

	if err := r.log.RestoreSnapshotBoundary(meta.LastIncludedIndex, meta.LastIncludedTerm); err != nil {
		return err
	}
	r.stateMachineMu.Lock()
	restoreErr := r.stateMachine.Restore(data)
	r.stateMachineMu.Unlock()
	if restoreErr != nil {
		return restoreErr
	}

	r.mu.Lock()
	oldConfig := cloneStringMap(r.clusterConfig.Peers)
	r.snapshotMeta = copySnapshotMeta(meta)
	r.snapshotData = append([]byte(nil), data...)
	r.commitIndex = max(r.commitIndex, meta.LastIncludedIndex)
	r.lastApplied = max(r.lastApplied, meta.LastIncludedIndex)
	r.clusterConfig = &ClusterConfig{Peers: cloneStringMap(meta.ClusterConfig)}
	r.mu.Unlock()
	r.reconcileTransportPeers(oldConfig, meta.ClusterConfig)

	if legacy {
		if err := r.persistSnapshot(meta, data); err != nil {
			return err
		}
	}
	return nil
}

func (r *RaftNode) takeSnapshot() error {
	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()

	r.stateMachineMu.Lock()
	r.mu.RLock()
	if r.fatalErr != nil {
		err := r.fatalErr
		r.mu.RUnlock()
		r.stateMachineMu.Unlock()
		return err
	}
	lastApplied := r.lastApplied
	currentSnapshotIndex := r.snapshotMeta.LastIncludedIndex
	clusterConfig := cloneStringMap(r.clusterConfig.Peers)
	r.mu.RUnlock()

	if lastApplied == 0 || lastApplied <= currentSnapshotIndex {
		r.stateMachineMu.Unlock()
		return nil
	}
	lastIncludedTerm, err := r.log.TermAt(lastApplied)
	if err != nil {
		r.stateMachineMu.Unlock()
		return err
	}
	data, err := r.stateMachine.Snapshot()
	r.stateMachineMu.Unlock()
	if err != nil {
		return err
	}

	meta := SnapshotMeta{
		LastIncludedIndex: lastApplied,
		LastIncludedTerm:  lastIncludedTerm,
		ClusterConfig:     clusterConfig,
	}
	if err := r.persistSnapshot(meta, data); err != nil {
		return err
	}

	r.mu.Lock()
	r.snapshotMeta = copySnapshotMeta(meta)
	r.snapshotData = append([]byte(nil), data...)
	r.mu.Unlock()

	if err := r.log.CompactUpTo(lastApplied); err != nil {
		r.markFatal(err)
		return err
	}
	return nil
}

func (r *RaftNode) sendSnapshot(ctx context.Context, peerID string) error {
	r.mu.RLock()
	if r.state != Leader {
		r.mu.RUnlock()
		return ErrNotLeader
	}
	term := r.currentTerm
	meta := copySnapshotMeta(r.snapshotMeta)
	data := append([]byte(nil), r.snapshotData...)
	r.mu.RUnlock()

	if meta.LastIncludedIndex == 0 {
		return ErrSnapshotUnavailable
	}
	checksum := sha256.Sum256(data)
	chunkSize := r.config.SnapshotChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultSnapshotChunkSize
	}

	totalSize := uint64(len(data))
	for offset, firstChunk := uint64(0), true; firstChunk || offset < totalSize; firstChunk = false {
		end := offset + uint64(chunkSize)
		if end > totalSize {
			end = totalSize
		}
		done := end == totalSize
		request := &InstallSnapshotRequest{
			Term:              term,
			LeaderID:          r.config.NodeID,
			LastIncludedIndex: meta.LastIncludedIndex,
			LastIncludedTerm:  meta.LastIncludedTerm,
			Offset:            offset,
			Done:              done,
			TotalSize:         totalSize,
			Checksum:          append([]byte(nil), checksum[:]...),
			ClusterConfig:     cloneStringMap(meta.ClusterConfig),
			Data:              append([]byte(nil), data[offset:end]...),
		}

		timeout := r.config.HeartbeatInterval * 5
		if timeout <= 0 {
			timeout = 2 * time.Second
		}
		rpcContext, cancel := context.WithTimeout(ctx, timeout)
		response, err := r.transport.InstallSnapshot(rpcContext, peerID, request)
		cancel()
		if err != nil {
			return err
		}
		if response == nil {
			return errors.New("InstallSnapshot returned a nil response")
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
		r.mu.Unlock()

		if done {
			r.mu.Lock()
			if meta.LastIncludedIndex+1 > r.nextIndex[peerID] {
				r.nextIndex[peerID] = meta.LastIncludedIndex + 1
			}
			if meta.LastIncludedIndex > r.matchIndex[peerID] {
				r.matchIndex[peerID] = meta.LastIncludedIndex
			}
			r.mu.Unlock()
			return nil
		}
		offset = end
	}
	return nil
}

func (r *RaftNode) handleInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	r.mu.Lock()
	if req == nil {
		term := r.currentTerm
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: term}, ErrInvalidSnapshot
	}
	if r.fatalErr != nil {
		term := r.currentTerm
		err := r.fatalErr
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: term}, err
	}
	if req.Term < r.currentTerm {
		term := r.currentTerm
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: term}, nil
	}
	if req.Term > r.currentTerm || r.state != Follower {
		r.stepDown(req.Term)
	}
	r.leaderID = req.LeaderID
	r.lastHeartbeat = time.Now()
	currentTerm := r.currentTerm
	if req.LastIncludedIndex < r.snapshotMeta.LastIncludedIndex ||
		req.LastIncludedIndex <= r.commitIndex {
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: currentTerm}, nil
	}
	if req.LastIncludedIndex == r.snapshotMeta.LastIncludedIndex {
		if req.LastIncludedTerm != r.snapshotMeta.LastIncludedTerm {
			r.mu.Unlock()
			return &InstallSnapshotResponse{Term: currentTerm}, ErrSnapshotBoundaryMismatch
		}
		r.mu.Unlock()
		return &InstallSnapshotResponse{Term: currentTerm}, nil
	}
	r.mu.Unlock()

	if err := r.validateInstallSnapshotRequest(req); err != nil {
		return &InstallSnapshotResponse{Term: currentTerm}, err
	}

	r.snapshotMu.Lock()
	defer r.snapshotMu.Unlock()

	r.mu.RLock()
	if req.Term < r.currentTerm {
		term := r.currentTerm
		r.mu.RUnlock()
		return &InstallSnapshotResponse{Term: term}, nil
	}
	if req.LastIncludedIndex <= r.commitIndex {
		term := r.currentTerm
		r.mu.RUnlock()
		return &InstallSnapshotResponse{Term: term}, nil
	}
	r.mu.RUnlock()

	var checksum [sha256.Size]byte
	copy(checksum[:], req.Checksum)
	if r.installSnapshot == nil || !r.installSnapshot.matches(req, checksum) {
		if req.Offset != 0 {
			return &InstallSnapshotResponse{Term: currentTerm}, ErrSnapshotOffset
		}
		r.discardInstallSnapshotLocked()
		state, err := r.newInstallSnapshotState(req, checksum)
		if err != nil {
			return &InstallSnapshotResponse{Term: currentTerm}, err
		}
		r.installSnapshot = state
	}

	if err := r.installSnapshot.appendChunk(req.Offset, req.Data); err != nil {
		return &InstallSnapshotResponse{Term: currentTerm}, err
	}
	if !req.Done {
		return &InstallSnapshotResponse{Term: currentTerm}, nil
	}
	if r.installSnapshot.nextOffset != r.installSnapshot.totalSize {
		return &InstallSnapshotResponse{Term: currentTerm}, ErrSnapshotOffset
	}

	data, err := r.installSnapshot.bytes()
	if err != nil {
		return &InstallSnapshotResponse{Term: currentTerm}, err
	}
	if actual := sha256.Sum256(data); actual != r.installSnapshot.checksum {
		r.discardInstallSnapshotLocked()
		return &InstallSnapshotResponse{Term: currentTerm}, ErrSnapshotChecksum
	}

	meta := copySnapshotMeta(r.installSnapshot.meta)
	if err := r.finalizeInstalledSnapshot(meta, data); err != nil {
		return &InstallSnapshotResponse{Term: r.currentTermValue()}, err
	}
	r.discardInstallSnapshotLocked()
	return &InstallSnapshotResponse{Term: r.currentTermValue()}, nil
}

func (r *RaftNode) validateInstallSnapshotRequest(req *InstallSnapshotRequest) error {
	if req.LastIncludedIndex == 0 || req.LastIncludedTerm == 0 {
		return fmt.Errorf("%w: snapshot boundary must be non-zero", ErrInvalidSnapshot)
	}
	if len(req.Checksum) != sha256.Size {
		return fmt.Errorf("%w: checksum must be %d bytes", ErrInvalidSnapshot, sha256.Size)
	}
	if len(req.ClusterConfig) == 0 {
		return fmt.Errorf("%w: cluster configuration is missing", ErrInvalidSnapshot)
	}
	maxSize := r.config.MaxSnapshotSize
	if maxSize == 0 {
		maxSize = defaultMaxSnapshotSize
	}
	if req.TotalSize > maxSize {
		return fmt.Errorf("%w: snapshot size %d exceeds limit %d", ErrInvalidSnapshot, req.TotalSize, maxSize)
	}
	chunkEnd := req.Offset + uint64(len(req.Data))
	if chunkEnd < req.Offset || chunkEnd > req.TotalSize {
		return fmt.Errorf("%w: chunk exceeds snapshot size", ErrInvalidSnapshot)
	}
	if req.Done != (chunkEnd == req.TotalSize) {
		return fmt.Errorf("%w: done flag does not match chunk boundary", ErrInvalidSnapshot)
	}
	return nil
}

func (r *RaftNode) newInstallSnapshotState(req *InstallSnapshotRequest, checksum [sha256.Size]byte) (*installSnapshotState, error) {
	state := &installSnapshotState{
		meta: SnapshotMeta{
			LastIncludedIndex: req.LastIncludedIndex,
			LastIncludedTerm:  req.LastIncludedTerm,
			ClusterConfig:     cloneStringMap(req.ClusterConfig),
		},
		checksum:  checksum,
		totalSize: req.TotalSize,
	}
	if r.config.DataDir == "" {
		if req.TotalSize > uint64(maxInt()) {
			return nil, ErrInvalidSnapshot
		}
		state.data = make([]byte, 0, int(req.TotalSize))
		return state, nil
	}
	state.path = r.snapshotInstallPath()
	file, err := os.OpenFile(state.path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	state.file = file
	return state, nil
}

func (s *installSnapshotState) matches(req *InstallSnapshotRequest, checksum [sha256.Size]byte) bool {
	return s.meta.LastIncludedIndex == req.LastIncludedIndex &&
		s.meta.LastIncludedTerm == req.LastIncludedTerm &&
		s.totalSize == req.TotalSize &&
		s.checksum == checksum &&
		mapsEqual(s.meta.ClusterConfig, req.ClusterConfig)
}

func (s *installSnapshotState) appendChunk(offset uint64, data []byte) error {
	chunkEnd := offset + uint64(len(data))
	if offset > s.nextOffset || chunkEnd > s.nextOffset && offset < s.nextOffset {
		return ErrSnapshotOffset
	}
	if offset < s.nextOffset {
		existing := make([]byte, len(data))
		if s.file != nil {
			if _, err := s.file.ReadAt(existing, int64(offset)); err != nil {
				return err
			}
		} else {
			copy(existing, s.data[int(offset):int(chunkEnd)])
		}
		if !bytes.Equal(existing, data) {
			return ErrSnapshotChunkMismatch
		}
		return nil
	}

	if s.file != nil {
		if _, err := s.file.WriteAt(data, int64(offset)); err != nil {
			return err
		}
	} else {
		s.data = append(s.data, data...)
	}
	s.nextOffset = chunkEnd
	return nil
}

func (s *installSnapshotState) bytes() ([]byte, error) {
	if s.totalSize > uint64(maxInt()) {
		return nil, ErrInvalidSnapshot
	}
	if s.file == nil {
		return append([]byte(nil), s.data...), nil
	}
	if err := s.file.Sync(); err != nil {
		return nil, err
	}
	data := make([]byte, int(s.totalSize))
	if _, err := s.file.ReadAt(data, 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return data, nil
}

func (r *RaftNode) discardInstallSnapshotLocked() {
	if r.installSnapshot == nil {
		return
	}
	if r.installSnapshot.file != nil {
		_ = r.installSnapshot.file.Close()
	}
	if r.installSnapshot.path != "" {
		_ = os.Remove(r.installSnapshot.path)
	}
	r.installSnapshot = nil
}

func (r *RaftNode) finalizeInstalledSnapshot(meta SnapshotMeta, data []byte) error {
	if err := r.persistSnapshot(meta, data); err != nil {
		return err
	}

	r.stateMachineMu.Lock()
	defer r.stateMachineMu.Unlock()

	if err := r.log.RestoreSnapshotBoundary(meta.LastIncludedIndex, meta.LastIncludedTerm); err != nil {
		r.markFatal(err)
		return err
	}
	if err := r.stateMachine.Restore(data); err != nil {
		r.markFatal(err)
		return err
	}

	r.mu.Lock()
	oldConfig := cloneStringMap(r.clusterConfig.Peers)
	r.snapshotMeta = copySnapshotMeta(meta)
	r.snapshotData = append([]byte(nil), data...)
	r.commitIndex = max(r.commitIndex, meta.LastIncludedIndex)
	r.lastApplied = max(r.lastApplied, meta.LastIncludedIndex)
	r.clusterConfig = &ClusterConfig{Peers: cloneStringMap(meta.ClusterConfig)}
	needsApply := r.commitIndex > r.lastApplied
	r.mu.Unlock()
	if needsApply {
		select {
		case r.commitCh <- struct{}{}:
		default:
		}
	}

	r.reconcileTransportPeers(oldConfig, meta.ClusterConfig)
	return nil
}

func (r *RaftNode) reconcileTransportPeers(oldConfig, newConfig map[string]string) {
	if r.transport == nil {
		return
	}
	for nodeID := range oldConfig {
		if nodeID == r.config.NodeID {
			continue
		}
		if _, exists := newConfig[nodeID]; !exists {
			r.transport.RemovePeer(nodeID)
		}
	}
	for nodeID, address := range newConfig {
		if nodeID == r.config.NodeID {
			continue
		}
		if oldAddress, exists := oldConfig[nodeID]; !exists || oldAddress != address {
			r.transport.AddPeer(nodeID, address)
		}
	}
}

func (r *RaftNode) currentTermValue() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

func (r *RaftNode) markFatal(err error) {
	r.mu.Lock()
	r.markFatalLocked(err)
	r.mu.Unlock()
}

func (r *RaftNode) markFatalLocked(err error) {
	if r.fatalErr == nil {
		r.fatalErr = fmt.Errorf("%w: %v", ErrNodeUnhealthy, err)
	}
	r.state = Follower
	r.noopCommitted = false
}
func (r *RaftNode) closeInstallSnapshot() {
	r.snapshotMu.Lock()
	r.discardInstallSnapshotLocked()
	r.snapshotMu.Unlock()
}

func copySnapshotMeta(meta SnapshotMeta) SnapshotMeta {
	meta.ClusterConfig = cloneStringMap(meta.ClusterConfig)
	return meta
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
