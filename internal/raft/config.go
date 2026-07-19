// Package raft implements the Raft consensus algorithm.
// Bu paket sadece consensus mantığını içerir; KV store'dan bağımsızdır.
//
// Kritik tasarım kararı: Raft, KV store'u sadece kvstore.StateMachine
// interface'i üzerinden görür. Doğrudan bağımlılık YOKTUR.
package raft

import "time"

// Config, Raft algoritmasının davranışını belirleyen parametreleri tutar.
// Tüm timeout ve boyut parametreleri buradan okunmalı — hardcode yasak.
type Config struct {
	// NodeID, bu node'un cluster içindeki benzersiz kimliğidir.
	NodeID string

	// Peers, diğer Raft node'larının adres listesidir (NodeID -> addr).
	Peers map[string]string

	// ElectionTimeout, leader'dan sinyal alamazsa adaylığa başlama süresi.
	// Tipik değer: 150ms–300ms arası rastgele.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval, leader'ın follower'lara heartbeat gönderme aralığı.
	// ElectionTimeoutMin'den küçük olmalı.
	HeartbeatInterval time.Duration

	// MaxLogEntriesPerRPC, tek bir AppendEntries RPC'de gönderilebilecek
	// maksimum log entry sayısı.
	MaxLogEntriesPerRPC int

	// SnapshotThreshold, snapshot tetiklemek için gereken minimum log entry sayısı.
	SnapshotThreshold uint64

	// DataDir, WAL ve snapshot dosyalarının saklanacağı dizin.
	DataDir string

	// SyncWrites, her log append işleminde diske anında (fsync) yazılıp yazılmayacağını belirler.
	// Testlerde hızı artırmak için false yapılabilir, ancak production'da true OLMALIDIR.
	SyncWrites bool

	// InitialElectionDelay, node ilk başladığında election timer'ının başlamadan
	// önce bekleyeceği süredir. Docker bridge network ve gRPC bağlantılarının
	// kurulması için production'da 1s ayarlanır. Testlerde 0 bırakılır.
	InitialElectionDelay time.Duration
}

// DefaultConfig, geliştirme ortamı için makul varsayılan değerler döndürür.
func DefaultConfig(nodeID string) *Config {
	return &Config{
		NodeID:              nodeID,
		Peers:               make(map[string]string),
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		MaxLogEntriesPerRPC: 100,
		SnapshotThreshold:   10000,
		DataDir:             "./data",
	}
}
