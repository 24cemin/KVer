package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersistentState_InMemory(t *testing.T) {
	t.Run("DefaultZeroValues", func(t *testing.T) {
		ps, err := newPersistentState("")
		if err != nil {
			t.Fatalf("newPersistentState failed: %v", err)
		}
		if ps.Term() != 0 {
			t.Errorf("expected term=0, got %d", ps.Term())
		}
		if ps.VotedFor() != "" {
			t.Errorf("expected votedFor='', got '%s'", ps.VotedFor())
		}
	})

	t.Run("SaveAndRead", func(t *testing.T) {
		ps, _ := newPersistentState("")
		if err := ps.SaveTermAndVote(5, "node2"); err != nil {
			t.Fatalf("SaveTermAndVote failed: %v", err)
		}
		if ps.Term() != 5 {
			t.Errorf("expected term=5, got %d", ps.Term())
		}
		if ps.VotedFor() != "node2" {
			t.Errorf("expected votedFor='node2', got '%s'", ps.VotedFor())
		}
	})
}

func TestPersistentState_DiskPersistence(t *testing.T) {
	dir := t.TempDir() // test sonunda otomatik silinir

	t.Run("WriteAndRead", func(t *testing.T) {
		ps, err := newPersistentState(dir)
		if err != nil {
			t.Fatalf("newPersistentState failed: %v", err)
		}
		if err := ps.SaveTermAndVote(7, "node3"); err != nil {
			t.Fatalf("SaveTermAndVote failed: %v", err)
		}

		// Aynı instance'dan oku
		if ps.Term() != 7 {
			t.Errorf("expected term=7, got %d", ps.Term())
		}
		if ps.VotedFor() != "node3" {
			t.Errorf("expected votedFor='node3', got '%s'", ps.VotedFor())
		}
	})

	t.Run("ReloadAfterRestart", func(t *testing.T) {
		// İlk instance yazar
		ps1, _ := newPersistentState(dir)
		_ = ps1.SaveTermAndVote(12, "node1")

		// Yeni instance aynı dir'den yükler — "restart" simülasyonu
		ps2, err := newPersistentState(dir)
		if err != nil {
			t.Fatalf("reload failed: %v", err)
		}
		if ps2.Term() != 12 {
			t.Errorf("expected term=12 after reload, got %d", ps2.Term())
		}
		if ps2.VotedFor() != "node1" {
			t.Errorf("expected votedFor='node1' after reload, got '%s'", ps2.VotedFor())
		}
	})

	t.Run("UpdateOverwritesPrevious", func(t *testing.T) {
		ps, _ := newPersistentState(dir)
		_ = ps.SaveTermAndVote(1, "nodeA")
		_ = ps.SaveTermAndVote(2, "nodeB")
		_ = ps.SaveTermAndVote(3, "nodeC")

		// Reload
		ps2, _ := newPersistentState(dir)
		if ps2.Term() != 3 {
			t.Errorf("expected term=3, got %d", ps2.Term())
		}
		if ps2.VotedFor() != "nodeC" {
			t.Errorf("expected votedFor='nodeC', got '%s'", ps2.VotedFor())
		}
	})

	t.Run("NoTmpFileLeftAfterWrite", func(t *testing.T) {
		ps, _ := newPersistentState(dir)
		_ = ps.SaveTermAndVote(99, "x")

		tmpPath := filepath.Join(dir, stateFileName+".tmp")
		if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
			t.Error("tmp file should not exist after successful write")
		}
	})

	t.Run("StateFileExistsAfterWrite", func(t *testing.T) {
		ps, _ := newPersistentState(dir)
		_ = ps.SaveTermAndVote(42, "node5")

		statePath := filepath.Join(dir, stateFileName)
		if _, err := os.Stat(statePath); err != nil {
			t.Errorf("state file should exist: %v", err)
		}
	})
}

func TestPersistentState_CorruptFile_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, stateFileName)

	// Bozuk JSON yaz
	if err := os.WriteFile(statePath, []byte("{corrupted json{{"), 0o644); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	_, err := newPersistentState(dir)
	if err == nil {
		t.Error("expected error on corrupt state file, got nil")
	}
}
