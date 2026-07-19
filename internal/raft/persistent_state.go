// Package raft — persistent_state.go
// Raft'ın kalıcı olarak saklanması gereken durumu: currentTerm ve votedFor.
// Bu iki değer her değişimde diske yazılmalı (crash-recovery için kritik).
//
// Tasarım: JSON encode + fsync garantisi ile atomic write (write-rename pattern).
// dataDir boş verilirse in-memory mod — test uyumluluğu için.
package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

const stateFileName = "raft_state.json"

// persistedData, diske yazılan JSON yapısı.
type persistedData struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// PersistentState, Raft'ın yeniden başlatma sonrası kurtarılması gereken
// durumunu yönetir.
type PersistentState struct {
	mu sync.Mutex

	// currentTerm, mevcut Raft term'idir. Her zaman monoton artar.
	currentTerm uint64

	// votedFor, mevcut term'de oy verilen candidate ID'sidir.
	// "" ise bu term'de oy verilmemiş demektir.
	votedFor string

	// dataDir, state dosyasının saklandığı dizin.
	// Boş ise in-memory mod (testler için).
	dataDir string
}

// newPersistentState, yeni bir PersistentState oluşturur.
// dataDir boş değilse mevcut state'i diskten yükler.
func newPersistentState(dataDir string) (*PersistentState, error) {
	ps := &PersistentState{dataDir: dataDir}

	if dataDir == "" {
		// In-memory mod — disk yok (testler)
		return ps, nil
	}

	// Dizini oluştur
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}

	// Mevcut state dosyasını yüklemeyi dene
	statePath := filepath.Join(dataDir, stateFileName)
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			// İlk başlatma — sıfır state
			return ps, nil
		}
		return nil, err
	}

	var pd persistedData
	if err := json.Unmarshal(data, &pd); err != nil {
		return nil, err
	}
	ps.currentTerm = pd.Term
	ps.votedFor = pd.VotedFor
	return ps, nil
}

// SaveTermAndVote, term ve votedFor'u atomik olarak kaydeder.
// Her term değişiminde veya oy verildiğinde çağrılmalı.
//
// Atomic write pattern:
//  1. Geçici dosyaya yaz
//  2. fsync — veri diske inmeden rename yapılmaz
//  3. Rename (atomic on POSIX)
func (ps *PersistentState) SaveTermAndVote(term uint64, votedFor string) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.currentTerm = term
	ps.votedFor = votedFor

	if ps.dataDir == "" {
		// In-memory mod — disk yazma yok
		return nil
	}

	pd := persistedData{Term: term, VotedFor: votedFor}
	b, err := json.Marshal(pd)
	if err != nil {
		return err
	}

	// Atomic write: geçici dosya → fsync → rename
	statePath := filepath.Join(ps.dataDir, stateFileName)
	tmpPath := statePath + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}

	// fsync: veriyi diske garantile
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Atomic rename (POSIX garantisi)
	return os.Rename(tmpPath, statePath)
}

// Term, mevcut term'i döndürür.
func (ps *PersistentState) Term() uint64 {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.currentTerm
}

// VotedFor, mevcut term'deki oy bilgisini döndürür.
func (ps *PersistentState) VotedFor() string {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.votedFor
}
