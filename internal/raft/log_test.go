package raft

import "testing"

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
