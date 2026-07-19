package raft

// EntryType, bir Raft log entry'sinin tipini belirler.
type EntryType int

const (
	// EntryKV — normal anahtar-değer operasyonu
	EntryKV EntryType = iota
	// EntryMembership — cluster üyelik değişikliği (Hafta 6)
	EntryMembership
	// EntryNoop — leader seçildiğinde commit index'i ilerletmek için (Raft §5.4)
	EntryNoop
)

// LogEntry, Raft log'una yazılan bir kaydı temsil eder.
// Raft katmanı bu yapıyı seri hale getirerek iletir; KV katmanı Apply ile çözümler.
type LogEntry struct {
	Index   uint64
	Term    uint64
	Type    EntryType
	Command []byte // marshalled KV command (protobuf)
}

// StateMachine, Raft'ın uygulama katmanıyla konuştuğu tek interface'dir.
// KV store veya diğer state machine'ler bu interface'i implement eder.
type StateMachine interface {
	// Apply, commit edilmiş bir log entry'yi state machine'e uygular.
	Apply(entry LogEntry) error

	// Snapshot, state machine'in anlık görüntüsünü döndürür.
	// Raft bu bayt dizisini disk + peer'lara gönderir.
	Snapshot() ([]byte, error)

	// Restore, bir snapshot bayt dizisinden state machine'i yeniden kurar.
	Restore([]byte) error
}
