package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRaftLog_AppendAndGet(t *testing.T) {
	t.Run("AppendAndGetSuccess", func(t *testing.T) {
		l := newRaftLog()
		requireNoError(t, l.Append(
			LogEntry{Index: 1, Term: 1},
			LogEntry{Index: 2, Term: 1},
			LogEntry{Index: 3, Term: 2},
		))

		tests := []struct {
			index uint64
			term  uint64
		}{
			{1, 1},
			{2, 1},
			{3, 2},
		}

		for _, tt := range tests {
			e, err := l.GetEntry(tt.index)
			if err != nil {
				t.Errorf("GetEntry(%d) failed: %v", tt.index, err)
			}
			if e.Term != tt.term {
				t.Errorf("GetEntry(%d) term mismatch: expected %d, got %d", tt.index, tt.term, e.Term)
			}
		}

		_, err := l.GetEntry(0)
		if err != ErrOutOfRange {
			t.Error("expected ErrOutOfRange for index 0")
		}

		_, err = l.GetEntry(99)
		if err != ErrOutOfRange {
			t.Error("expected ErrOutOfRange for index 99")
		}
	})
}

func TestRaftLog_TruncateAfter(t *testing.T) {
	t.Run("TruncateCorrectly", func(t *testing.T) {
		l := newRaftLog()
		for i := uint64(1); i <= 5; i++ {
			requireNoError(t, l.Append(LogEntry{Index: i, Term: 1}))
		}

		requireNoError(t, l.TruncateAfter(3))
		if l.LastIndex() != 3 {
			t.Errorf("expected LastIndex 3, got %d", l.LastIndex())
		}

		_, err := l.GetEntry(4)
		if err != ErrOutOfRange {
			t.Error("expected index 4 to be out of range after truncate")
		}

		_, err = l.GetEntry(3)
		if err != nil {
			t.Errorf("expected index 3 to be present, got err: %v", err)
		}
	})
}

func TestRaftLog_LastIndexAndTerm(t *testing.T) {
	t.Run("LastIndexAndTermConsistency", func(t *testing.T) {
		l := newRaftLog()
		if l.LastIndex() != 0 || l.LastTerm() != 0 {
			t.Errorf("empty log: expected index 0, term 0; got index %d, term %d", l.LastIndex(), l.LastTerm())
		}

		requireNoError(t, l.Append(LogEntry{Index: 1, Term: 1}))
		if l.LastIndex() != 1 || l.LastTerm() != 1 {
			t.Errorf("1 entry: expected index 1, term 1; got index %d, term %d", l.LastIndex(), l.LastTerm())
		}

		requireNoError(t, l.Append(LogEntry{Index: 2, Term: 2}, LogEntry{Index: 3, Term: 2}))
		if l.LastIndex() != 3 || l.LastTerm() != 2 {
			t.Errorf("3 entries: expected index 3, term 2; got index %d, term %d", l.LastIndex(), l.LastTerm())
		}
	})
}

func TestRaftLog_GetEntriesFrom(t *testing.T) {
	t.Run("GetEntriesFromSuccess", func(t *testing.T) {
		l := newRaftLog()
		for i := uint64(1); i <= 5; i++ {
			requireNoError(t, l.Append(LogEntry{Index: i, Term: 1}))
		}

		entries, err := l.GetEntriesFrom(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(entries) != 3 {
			t.Errorf("expected 3 entries, got %d", len(entries))
		}
		if entries[0].Index != 3 || entries[2].Index != 5 {
			t.Errorf("incorrect entries returned: start %d, end %d", entries[0].Index, entries[2].Index)
		}

		entries, err = l.GetEntriesFrom(6)
		if err != nil {
			t.Errorf("expected no error for index beyond last, got %v", err)
		}
		if len(entries) != 0 {
			t.Errorf("expected 0 entries for index 6, got %d", len(entries))
		}

		_, err = l.GetEntriesFrom(0)
		if err != ErrOutOfRange {
			t.Error("expected ErrOutOfRange for index 0")
		}
	})
}

func TestRaftLog_RejectsNonContiguousAppendWithoutMutation(t *testing.T) {
	log := newRaftLog()
	requireNoError(t, log.Append(LogEntry{Index: 1, Term: 1}))

	if err := log.Append(LogEntry{Index: 1, Term: 1}); !errors.Is(err, ErrNonContiguousLog) {
		t.Fatalf("expected duplicate append rejection, got %v", err)
	}
	if err := log.Append(LogEntry{Index: 3, Term: 1}); !errors.Is(err, ErrNonContiguousLog) {
		t.Fatalf("expected gap append rejection, got %v", err)
	}
	if err := log.Append(
		LogEntry{Index: 2, Term: 1},
		LogEntry{Index: 4, Term: 1},
	); !errors.Is(err, ErrNonContiguousLog) {
		t.Fatalf("expected discontinuous batch rejection, got %v", err)
	}
	if log.LastIndex() != 1 {
		t.Fatalf("failed append mutated log: last index %d", log.LastIndex())
	}
	if _, err := log.GetEntry(2); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("failed batch partially mutated log: %v", err)
	}
}

func TestRaftLog_FullCompactionPreservesBoundary(t *testing.T) {
	log := newRaftLog()
	requireNoError(t, log.Append(
		LogEntry{Index: 1, Term: 1},
		LogEntry{Index: 2, Term: 2},
		LogEntry{Index: 3, Term: 2},
	))
	requireNoError(t, log.CompactUpTo(3))

	if log.FirstIndex() != 4 || log.LastIndex() != 3 || log.LastTerm() != 2 {
		t.Fatalf("unexpected compacted boundary: first=%d last=%d term=%d", log.FirstIndex(), log.LastIndex(), log.LastTerm())
	}
	term, err := log.TermAt(3)
	requireNoError(t, err)
	if term != 2 {
		t.Fatalf("expected boundary term 2, got %d", term)
	}
	if _, err := log.GetEntry(3); !errors.Is(err, ErrLogCompacted) {
		t.Fatalf("expected compacted entry error, got %v", err)
	}
	requireNoError(t, log.Append(LogEntry{Index: 4, Term: 3}))
}

func TestRaftLog_RestoreSnapshotBoundaryRetainsOnlyMatchingSuffix(t *testing.T) {
	t.Run("matching term", func(t *testing.T) {
		log := newRaftLog()
		requireNoError(t, log.Append(
			LogEntry{Index: 1, Term: 1},
			LogEntry{Index: 2, Term: 2},
			LogEntry{Index: 3, Term: 3},
		))
		requireNoError(t, log.RestoreSnapshotBoundary(2, 2))
		if log.FirstIndex() != 3 || log.LastIndex() != 3 {
			t.Fatalf("matching suffix was not retained")
		}
	})

	t.Run("mismatched term", func(t *testing.T) {
		log := newRaftLog()
		requireNoError(t, log.Append(
			LogEntry{Index: 1, Term: 1},
			LogEntry{Index: 2, Term: 1},
			LogEntry{Index: 3, Term: 3},
		))
		requireNoError(t, log.RestoreSnapshotBoundary(2, 2))
		if log.FirstIndex() != 3 || log.LastIndex() != 2 || log.LastTerm() != 2 {
			t.Fatalf("mismatched suffix was not discarded")
		}
	})
}

func TestRaftLog_WALReloadPreservesBoundary(t *testing.T) {
	directory := t.TempDir()
	log, err := newRaftLogWithWAL(directory, "node1", true)
	requireNoError(t, err)
	requireNoError(t, log.Append(
		LogEntry{Index: 1, Term: 1},
		LogEntry{Index: 2, Term: 2},
		LogEntry{Index: 3, Term: 2},
	))
	requireNoError(t, log.CompactUpTo(3))
	requireNoError(t, log.Close())

	reloaded, err := newRaftLogWithWAL(directory, "node1", true)
	requireNoError(t, err)
	t.Cleanup(func() {
		if err := reloaded.Close(); err != nil {
			t.Errorf("close reloaded WAL: %v", err)
		}
	})
	if reloaded.LastIndex() != 3 || reloaded.LastTerm() != 2 || reloaded.FirstIndex() != 4 {
		t.Fatalf("reloaded boundary mismatch: first=%d last=%d term=%d", reloaded.FirstIndex(), reloaded.LastIndex(), reloaded.LastTerm())
	}
	requireNoError(t, reloaded.Append(LogEntry{Index: 4, Term: 3}))
}

func TestRaftLog_WALRejectsIndexCorruption(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "node1_wal.bin")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	requireNoError(t, err)
	requireNoError(t, writeWALHeader(file, 0, 0))
	requireNoError(t, writeWALRecord(file, LogEntry{Index: 1, Term: 1}))
	requireNoError(t, writeWALRecord(file, LogEntry{Index: 3, Term: 1}))
	requireNoError(t, file.Close())

	if _, err := newRaftLogWithWAL(directory, "node1", true); !errors.Is(err, ErrNonContiguousLog) {
		t.Fatalf("expected WAL continuity error, got %v", err)
	}
}

func TestRaftLog_WALRecoversPartialTail(t *testing.T) {
	directory := t.TempDir()
	log, err := newRaftLogWithWAL(directory, "node1", true)
	requireNoError(t, err)
	requireNoError(t, log.Append(LogEntry{Index: 1, Term: 1}))
	requireNoError(t, log.Close())

	file, err := os.OpenFile(filepath.Join(directory, "node1_wal.bin"), os.O_WRONLY|os.O_APPEND, 0o644)
	requireNoError(t, err)
	_, err = file.Write([]byte{1, 2, 3})
	requireNoError(t, err)
	requireNoError(t, file.Close())

	reloaded, err := newRaftLogWithWAL(directory, "node1", true)
	requireNoError(t, err)
	t.Cleanup(func() {
		if err := reloaded.Close(); err != nil {
			t.Errorf("close reloaded WAL: %v", err)
		}
	})
	if reloaded.LastIndex() != 1 {
		t.Fatalf("partial uncommitted tail changed recovered log")
	}
}

func TestRaftLog_SnapshotIndex1000AppendsAt1001(t *testing.T) {
	log := newRaftLog()
	requireNoError(t, log.RestoreSnapshotBoundary(1000, 7))
	if log.LastIndex() != 1000 || log.LastTerm() != 7 {
		t.Fatalf("expected snapshot boundary 1000/7, got %d/%d", log.LastIndex(), log.LastTerm())
	}
	requireNoError(t, log.Append(LogEntry{Index: 1001, Term: 8}))
	if log.LastIndex() != 1001 {
		t.Fatalf("expected new entry at 1001, got %d", log.LastIndex())
	}
}
