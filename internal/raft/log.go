// Package raft — log.go
// Raft log yönetimi: append, lookup, truncate ve persistence.
// WAL (Write-Ahead Log) entegrasyonu bu dosyada yapılır.
package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	raftpb "github.com/emin/kver/proto/raft/gen"
	"google.golang.org/protobuf/proto"
)

// RaftLog, Raft log kayıtlarını yönetir.
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry

	dataDir string   // WAL dosyasının saklandığı dizin
	nodeID  string   // WAL dosyası adı için
	walFile *os.File // Açık tutulan WAL dosyası

	// firstIndex, log'daki ilk entry'nin index'idir.
	// Snapshot sonrası bu değer artar (log compaction).
	firstIndex uint64

	syncWrites bool // True ise her append işleminde fsync yapar.
}

// newRaftLog, boş bir RaftLog oluşturur.
func newRaftLog() *RaftLog {
	return &RaftLog{
		entries:    make([]LogEntry, 0),
		firstIndex: 1,
	}
}

// newRaftLogWithWAL, disk destekli bir RaftLog oluşturur.
func newRaftLogWithWAL(dataDir, nodeID string, syncWrites bool) (*RaftLog, error) {
	l := &RaftLog{
		entries:    make([]LogEntry, 0),
		firstIndex: 1,
		dataDir:    dataDir,
		nodeID:     nodeID,
		syncWrites: syncWrites,
	}
	if dataDir == "" {
		return l, nil // in-memory mod
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	// Mevcut WAL'ı yükle
	if err := l.loadFromDisk(); err != nil {
		return nil, err
	}
	// Dosyayı sürekli Append-Only modunda açık tut (Performans için)
	f, err := os.OpenFile(l.walPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l.walFile = f
	return l, nil
}

// walPath, WAL dosyasının tam yolunu döndürür.
func (l *RaftLog) walPath() string {
	return filepath.Join(l.dataDir, l.nodeID+"_wal.bin")
}

// Close, açık olan WAL dosyasını kapatır.
func (l *RaftLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.walFile != nil {
		syncErr := l.walFile.Sync()
		closeErr := l.walFile.Close()
		l.walFile = nil
		return errors.Join(syncErr, closeErr)
	}
	return nil
}

// loadFromDisk, WAL dosyasını diskten okur ve entries alanını doldurur.
func (l *RaftLog) loadFromDisk() (err error) {
	f, err := os.Open(l.walPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil // ilk başlatma
		}
		return err
	}
	defer func() {
		err = errors.Join(err, f.Close())
	}()

	l.entries = nil
	for {
		var length uint32
		if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		var expectedChecksum uint32
		if err := binary.Read(f, binary.LittleEndian, &expectedChecksum); err != nil {
			return err
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(f, buf); err != nil {
			return err
		}
		if actualChecksum := crc32.ChecksumIEEE(buf); actualChecksum != expectedChecksum {
			return fmt.Errorf("WAL corruption: checksum mismatch (expected %d, got %d)", expectedChecksum, actualChecksum)
		}
		var e raftpb.LogEntry
		if err := proto.Unmarshal(buf, &e); err != nil {
			return err
		}
		l.entries = append(l.entries, LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Type:    EntryType(e.Type),
			Command: e.Command,
		})
	}

	if len(l.entries) > 0 {
		l.firstIndex = l.entries[0].Index
	}
	return nil
}

// persistToDisk, tüm log'u baştan yazar (truncate işlemleri için).
func (l *RaftLog) persistToDisk() error {
	if l.dataDir == "" {
		return nil
	}
	walPath := l.walPath()
	tmpPath := walPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	for _, e := range l.entries {
		pbe := &raftpb.LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Type:    raftpb.EntryType(e.Type),
			Command: e.Command,
		}
		b, err := proto.Marshal(pbe)
		if err != nil {
			_ = f.Close()
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(len(b))); err != nil {
			_ = f.Close()
			return err
		}
		checksum := crc32.ChecksumIEEE(b)
		if err := binary.Write(f, binary.LittleEndian, checksum); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(b); err != nil {
			_ = f.Close()
			return err
		}
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, walPath); err != nil {
		return err
	}

	// Eski açık dosyayı kapatıp yeni truncate edilmiş dosyayı açalım
	if l.walFile != nil {
		_ = l.walFile.Close()
	}
	fNew, err := os.OpenFile(walPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.walFile = fNew
	return nil
}

// appendWALToDisk, yalnızca yeni gelen log'ları dosyanın sonuna ekler (O(1) Append).
func (l *RaftLog) appendWALToDisk(entries []LogEntry) error {
	if l.dataDir == "" || len(entries) == 0 || l.walFile == nil {
		return nil
	}

	for _, e := range entries {
		pbe := &raftpb.LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Type:    raftpb.EntryType(e.Type),
			Command: e.Command,
		}
		b, err := proto.Marshal(pbe)
		if err != nil {
			return err
		}
		if err := binary.Write(l.walFile, binary.LittleEndian, uint32(len(b))); err != nil {
			return err
		}
		checksum := crc32.ChecksumIEEE(b)
		if err := binary.Write(l.walFile, binary.LittleEndian, checksum); err != nil {
			return err
		}
		if _, err := l.walFile.Write(b); err != nil {
			return err
		}
	}
	if l.syncWrites {
		return l.walFile.Sync()
	}
	return nil
}

// LastIndex, log'daki son entry'nin index'ini döndürür.
func (l *RaftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.firstIndex + uint64(len(l.entries)) - 1
}

// LastTerm, log'daki son entry'nin term'ini döndürür.
func (l *RaftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// GetEntry, belirtilen index'teki log entry'sini döndürür.
func (l *RaftLog) GetEntry(index uint64) (LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index < l.firstIndex {
		return LogEntry{}, ErrOutOfRange
	}
	offset := index - l.firstIndex
	if offset >= uint64(len(l.entries)) {
		return LogEntry{}, ErrOutOfRange
	}
	return l.entries[offset], nil
}

// GetEntriesFrom, belirtilen index'ten itibaren tüm entry'leri döndürür.
func (l *RaftLog) GetEntriesFrom(index uint64) ([]LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index < l.firstIndex {
		return nil, ErrOutOfRange
	}
	offset := index - l.firstIndex
	if offset >= uint64(len(l.entries)) {
		return []LogEntry{}, nil
	}
	result := make([]LogEntry, len(l.entries)-int(offset))
	copy(result, l.entries[offset:])
	return result, nil
}

// Append, yeni entry'leri log'a ekler.
func (l *RaftLog) Append(entries ...LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)

	// Disk'e yalnızca yeni logları ekle (Append-Only WAL)
	return l.appendWALToDisk(entries)
}

// TruncateAfter, belirtilen index'ten sonraki tüm entry'leri siler.
// Conflict durumunda follower log'u temizlemek için kullanılır.
func (l *RaftLog) TruncateAfter(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index < l.firstIndex {
		l.entries = l.entries[:0]
		return nil
	}
	offset := index - l.firstIndex + 1
	if offset >= uint64(len(l.entries)) {
		return nil
	}
	l.entries = l.entries[:offset]
	return l.persistToDisk()
}

// CompactUpTo, snapshot alındıktan sonra log'u index'e kadar sıkıştırır.
func (l *RaftLog) CompactUpTo(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index < l.firstIndex {
		return nil // zaten compacted
	}

	lastIndex := l.firstIndex + uint64(len(l.entries)) - 1
	if index > lastIndex {
		index = lastIndex
	}

	offset := index - l.firstIndex + 1
	l.entries = l.entries[offset:]
	l.firstIndex = index + 1
	return l.persistToDisk()
}

// FirstIndex, log'daki ilk entry'nin index'ini döndürür.
func (l *RaftLog) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndex
}

// SetFirstIndex, snapshot sonrası firstIndex'i günceller.
func (l *RaftLog) SetFirstIndex(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.firstIndex = index
}
