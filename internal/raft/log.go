// Package raft — log.go
// Raft log yönetimi: append, lookup, truncate ve persistence.
// WAL (Write-Ahead Log) entegrasyonu bu dosyada yapılır.
package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	raftpb "github.com/24cemin/KVer/proto/raft/gen"
	"google.golang.org/protobuf/proto"
)

var walMagic = [8]byte{'K', 'V', 'E', 'R', 'W', 'A', 'L', '1'}

const maxWALRecordSize = 64 << 20

// RaftLog, Raft log kayıtlarını yönetir.
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry

	dataDir string
	nodeID  string
	walFile *os.File

	baseIndex     uint64
	baseTerm      uint64
	boundaryKnown bool

	syncWrites bool
	storageErr error

	walHasHeader bool
	legacyWAL    bool
}

// newRaftLog, boş bir RaftLog oluşturur.
func newRaftLog() *RaftLog {
	return &RaftLog{
		entries:       make([]LogEntry, 0),
		boundaryKnown: true,
	}
}

// newRaftLogWithWAL, disk destekli bir RaftLog oluşturur.
func newRaftLogWithWAL(dataDir, nodeID string, syncWrites bool) (*RaftLog, error) {
	log := &RaftLog{
		entries:       make([]LogEntry, 0),
		dataDir:       dataDir,
		nodeID:        nodeID,
		syncWrites:    syncWrites,
		boundaryKnown: true,
	}
	if dataDir == "" {
		return log, nil
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	if err := log.loadFromDisk(); err != nil {
		return nil, err
	}

	if !log.walHasHeader && !log.legacyWAL {
		if err := log.persistStateToDiskLocked(log.entries, log.baseIndex, log.baseTerm); err != nil {
			return nil, err
		}
	} else if log.legacyWAL && log.baseIndex == 0 {
		if err := log.persistStateToDiskLocked(log.entries, 0, 0); err != nil {
			return nil, err
		}
	} else {
		file, err := os.OpenFile(log.walPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		log.walFile = file
	}
	return log, nil
}

func (l *RaftLog) walPath() string {
	return filepath.Join(l.dataDir, l.nodeID+"_wal.bin")
}

// Close, açık olan WAL dosyasını kapatır.
func (l *RaftLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.walFile == nil {
		return nil
	}
	syncErr := l.walFile.Sync()
	closeErr := l.walFile.Close()
	l.walFile = nil
	return errors.Join(syncErr, closeErr)
}

func (l *RaftLog) loadFromDisk() (err error) {
	file, err := os.OpenFile(l.walPath(), os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	var prefix [8]byte
	read, err := io.ReadFull(file, prefix[:])
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if read > 0 {
				if truncateErr := truncateWALTail(file, 0); truncateErr != nil {
					return errors.Join(err, truncateErr)
				}
			}
			return nil
		}
		return err
	}

	if bytes.Equal(prefix[:], walMagic[:]) {
		l.walHasHeader = true
		l.boundaryKnown = true
		if err := l.readWALHeader(file); err != nil {
			return err
		}
	} else {
		l.legacyWAL = true
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}

	entries, err := readWALRecords(file)
	if err != nil {
		return err
	}
	if l.legacyWAL && len(entries) > 0 {
		if entries[0].Index == 0 {
			return fmt.Errorf("%w: legacy WAL starts at index 0", ErrCorruptLog)
		}
		l.baseIndex = entries[0].Index - 1
		l.baseTerm = 0
		l.boundaryKnown = l.baseIndex == 0
	}
	if err := validateEntrySequence(entries, l.baseIndex+1); err != nil {
		return err
	}
	l.entries = entries
	return nil
}

func (l *RaftLog) readWALHeader(reader io.Reader) error {
	var boundary [16]byte
	if _, err := io.ReadFull(reader, boundary[:]); err != nil {
		return fmt.Errorf("%w: incomplete WAL boundary: %v", ErrCorruptLog, err)
	}
	var expectedChecksum uint32
	if err := binary.Read(reader, binary.LittleEndian, &expectedChecksum); err != nil {
		return fmt.Errorf("%w: incomplete WAL header checksum: %v", ErrCorruptLog, err)
	}
	if actual := crc32.ChecksumIEEE(boundary[:]); actual != expectedChecksum {
		return fmt.Errorf("%w: WAL header checksum mismatch", ErrCorruptLog)
	}
	l.baseIndex = binary.LittleEndian.Uint64(boundary[:8])
	l.baseTerm = binary.LittleEndian.Uint64(boundary[8:])
	if l.baseIndex == 0 && l.baseTerm != 0 {
		return fmt.Errorf("%w: zero WAL boundary has non-zero term", ErrCorruptLog)
	}
	return nil
}

func readWALRecords(file *os.File) ([]LogEntry, error) {
	entries := make([]LogEntry, 0)
	for {
		recordStart, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		var length uint32
		if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				if truncateErr := truncateWALTail(file, recordStart); truncateErr != nil {
					return nil, errors.Join(err, truncateErr)
				}
				break
			}
			return nil, err
		}
		if length > maxWALRecordSize {
			return nil, fmt.Errorf("%w: WAL record length %d exceeds limit", ErrCorruptLog, length)
		}
		var expectedChecksum uint32
		if err := binary.Read(file, binary.LittleEndian, &expectedChecksum); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if truncateErr := truncateWALTail(file, recordStart); truncateErr != nil {
					return nil, errors.Join(err, truncateErr)
				}
				break
			}
			return nil, err
		}
		buffer := make([]byte, length)
		if _, err := io.ReadFull(file, buffer); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if truncateErr := truncateWALTail(file, recordStart); truncateErr != nil {
					return nil, errors.Join(err, truncateErr)
				}
				break
			}
			return nil, err
		}
		if actualChecksum := crc32.ChecksumIEEE(buffer); actualChecksum != expectedChecksum {
			return nil, fmt.Errorf("%w: WAL checksum mismatch", ErrCorruptLog)
		}
		var entry raftpb.LogEntry
		if err := proto.Unmarshal(buffer, &entry); err != nil {
			return nil, fmt.Errorf("%w: invalid WAL entry: %v", ErrCorruptLog, err)
		}
		entries = append(entries, LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Type:    EntryType(entry.Type),
			Command: append([]byte(nil), entry.Command...),
		})
	}
	return entries, nil
}

func truncateWALTail(file *os.File, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return err
	}
	return file.Sync()
}

func validateEntrySequence(entries []LogEntry, expected uint64) error {
	for _, entry := range entries {
		if entry.Index != expected {
			return fmt.Errorf("%w: expected index %d, got %d", ErrNonContiguousLog, expected, entry.Index)
		}
		expected++
	}
	return nil
}

func writeWALHeader(writer io.Writer, baseIndex, baseTerm uint64) error {
	if _, err := writer.Write(walMagic[:]); err != nil {
		return err
	}
	var boundary [16]byte
	binary.LittleEndian.PutUint64(boundary[:8], baseIndex)
	binary.LittleEndian.PutUint64(boundary[8:], baseTerm)
	if _, err := writer.Write(boundary[:]); err != nil {
		return err
	}
	return binary.Write(writer, binary.LittleEndian, crc32.ChecksumIEEE(boundary[:]))
}

func writeWALRecord(writer io.Writer, entry LogEntry) error {
	protobufEntry := &raftpb.LogEntry{
		Index:   entry.Index,
		Term:    entry.Term,
		Type:    raftpb.EntryType(entry.Type),
		Command: entry.Command,
	}
	data, err := proto.Marshal(protobufEntry)
	if err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, crc32.ChecksumIEEE(data)); err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}

func (l *RaftLog) persistStateToDiskLocked(entries []LogEntry, baseIndex, baseTerm uint64) error {
	if l.dataDir == "" {
		return nil
	}
	if l.storageErr != nil {
		return l.storageErr
	}

	walPath := l.walPath()
	tempPath := walPath + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return l.failStorageLocked(err)
	}
	closeWithError := func(writeErr error) error {
		closeErr := file.Close()
		_ = os.Remove(tempPath)
		return l.failStorageLocked(errors.Join(writeErr, closeErr))
	}

	if err := writeWALHeader(file, baseIndex, baseTerm); err != nil {
		return closeWithError(err)
	}
	for _, entry := range entries {
		if err := writeWALRecord(file, entry); err != nil {
			return closeWithError(err)
		}
	}
	if err := file.Sync(); err != nil {
		return closeWithError(err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return l.failStorageLocked(err)
	}
	if err := os.Rename(tempPath, walPath); err != nil {
		_ = os.Remove(tempPath)
		return l.failStorageLocked(err)
	}
	if err := syncDirectory(l.dataDir); err != nil {
		return l.failStorageLocked(err)
	}

	newFile, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return l.failStorageLocked(err)
	}
	oldFile := l.walFile
	l.walFile = newFile
	if oldFile != nil {
		if err := oldFile.Close(); err != nil {
			return l.failStorageLocked(err)
		}
	}
	l.walHasHeader = true
	l.legacyWAL = false
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (l *RaftLog) appendWALToDiskLocked(entries []LogEntry) error {
	if l.dataDir == "" || len(entries) == 0 {
		return nil
	}
	if l.storageErr != nil {
		return l.storageErr
	}
	if l.walFile == nil {
		return l.failStorageLocked(errors.New("WAL file is not open"))
	}

	startOffset, err := l.walFile.Seek(0, io.SeekEnd)
	if err != nil {
		return l.failStorageLocked(err)
	}
	for _, entry := range entries {
		if err := writeWALRecord(l.walFile, entry); err != nil {
			return l.failAppendLocked(startOffset, err)
		}
	}
	if l.syncWrites {
		if err := l.walFile.Sync(); err != nil {
			return l.failAppendLocked(startOffset, err)
		}
	}
	return nil
}

func (l *RaftLog) failAppendLocked(offset int64, cause error) error {
	truncateErr := l.walFile.Truncate(offset)
	if truncateErr == nil {
		_, truncateErr = l.walFile.Seek(0, io.SeekEnd)
	}
	return l.failStorageLocked(errors.Join(cause, truncateErr))
}

func (l *RaftLog) failStorageLocked(cause error) error {
	if cause == nil {
		return nil
	}
	if l.storageErr == nil {
		l.storageErr = fmt.Errorf("%w: %v", ErrStorageUnavailable, cause)
	}
	return l.storageErr
}

// LastIndex, log'daki son entry'nin veya snapshot boundary'nin index'ini döndürür.
func (l *RaftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return l.baseIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm, log'daki son entry'nin veya snapshot boundary'nin term'ini döndürür.
func (l *RaftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return l.baseTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// Boundary, son compact edilmiş index ve term'i döndürür.
func (l *RaftLog) Boundary() (uint64, uint64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.baseIndex, l.baseTerm, l.boundaryKnown
}

// TermAt, bir live entry'nin veya snapshot boundary'nin term'ini döndürür.
func (l *RaftLog) TermAt(index uint64) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.termAtLocked(index)
}

func (l *RaftLog) termAtLocked(index uint64) (uint64, error) {
	if index == l.baseIndex {
		if index == 0 {
			return 0, nil
		}
		if !l.boundaryKnown {
			return 0, ErrLogCompacted
		}
		return l.baseTerm, nil
	}
	if index < l.baseIndex {
		return 0, ErrLogCompacted
	}
	offset := index - l.baseIndex - 1
	if offset >= uint64(len(l.entries)) {
		return 0, ErrOutOfRange
	}
	return l.entries[offset].Term, nil
}

// GetEntry, belirtilen live log entry'sini döndürür.
func (l *RaftLog) GetEntry(index uint64) (LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index == 0 && l.baseIndex == 0 {
		return LogEntry{}, ErrOutOfRange
	}
	if index <= l.baseIndex {
		return LogEntry{}, ErrLogCompacted
	}
	offset := index - l.baseIndex - 1
	if offset >= uint64(len(l.entries)) {
		return LogEntry{}, ErrOutOfRange
	}
	return l.entries[offset], nil
}

// GetEntriesFrom, belirtilen index'ten itibaren live entry'leri döndürür.
func (l *RaftLog) GetEntriesFrom(index uint64) ([]LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index == 0 && l.baseIndex == 0 {
		return nil, ErrOutOfRange
	}
	if index <= l.baseIndex {
		return nil, ErrLogCompacted
	}
	offset := index - l.baseIndex - 1
	if offset >= uint64(len(l.entries)) {
		return []LogEntry{}, nil
	}
	result := make([]LogEntry, len(l.entries)-int(offset))
	copy(result, l.entries[offset:])
	return result, nil
}

// Append, kesintisiz yeni entry'leri log'a ekler.
func (l *RaftLog) Append(entries ...LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	if l.storageErr != nil {
		return l.storageErr
	}
	expected := l.baseIndex + uint64(len(l.entries)) + 1
	if err := validateEntrySequence(entries, expected); err != nil {
		return err
	}
	if err := l.appendWALToDiskLocked(entries); err != nil {
		return err
	}
	l.entries = append(l.entries, entries...)
	return nil
}

// TruncateAfter, belirtilen index'ten sonraki tüm live entry'leri siler.
func (l *RaftLog) TruncateAfter(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index < l.baseIndex {
		return ErrLogCompacted
	}
	lastIndex := l.baseIndex + uint64(len(l.entries))
	if index >= lastIndex {
		return nil
	}
	retain := index - l.baseIndex
	candidate := append([]LogEntry(nil), l.entries[:retain]...)
	if err := l.persistStateToDiskLocked(candidate, l.baseIndex, l.baseTerm); err != nil {
		return err
	}
	l.entries = candidate
	return nil
}

// CompactUpTo, snapshot kalıcılaştırıldıktan sonra log'u index'e kadar compact eder.
func (l *RaftLog) CompactUpTo(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index <= l.baseIndex {
		return nil
	}
	lastIndex := l.baseIndex + uint64(len(l.entries))
	if index > lastIndex {
		return ErrOutOfRange
	}
	term, err := l.termAtLocked(index)
	if err != nil {
		return err
	}
	offset := index - l.baseIndex
	candidate := append([]LogEntry(nil), l.entries[offset:]...)
	if err := l.persistStateToDiskLocked(candidate, index, term); err != nil {
		return err
	}
	l.entries = candidate
	l.baseIndex = index
	l.baseTerm = term
	l.boundaryKnown = true
	return nil
}

// RestoreSnapshotBoundary, snapshot boundary'sini kurar ve yalnız eşleşen suffix'i korur.
func (l *RaftLog) RestoreSnapshotBoundary(index, term uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index < l.baseIndex {
		return ErrSnapshotStale
	}

	candidate := l.entries
	if index == l.baseIndex {
		if l.boundaryKnown && l.baseTerm != term {
			return ErrSnapshotBoundaryMismatch
		}
	} else {
		candidate = nil
		lastIndex := l.baseIndex + uint64(len(l.entries))
		if index <= lastIndex {
			offset := index - l.baseIndex - 1
			if l.entries[offset].Term == term {
				candidate = l.entries[offset+1:]
			}
		}
	}
	candidate = append([]LogEntry(nil), candidate...)
	if err := validateEntrySequence(candidate, index+1); err != nil {
		return err
	}
	if err := l.persistStateToDiskLocked(candidate, index, term); err != nil {
		return err
	}
	l.entries = candidate
	l.baseIndex = index
	l.baseTerm = term
	l.boundaryKnown = true
	return nil
}

// FirstIndex, snapshot boundary'den sonraki ilk live index'i döndürür.
func (l *RaftLog) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.baseIndex + 1
}

// persistToDisk, benchmark ve bakım yolları için mevcut log durumunu yeniden yazar.
func (l *RaftLog) persistToDisk() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.persistStateToDiskLocked(l.entries, l.baseIndex, l.baseTerm)
}
