package raft

import "errors"

var (
	ErrOutOfRange               = errors.New("log index out of range")
	ErrNonContiguousLog         = errors.New("log entries are not contiguous")
	ErrCorruptLog               = errors.New("raft log is corrupt")
	ErrStorageUnavailable       = errors.New("raft storage is unavailable")
	ErrNotLeader                = errors.New("node is not the leader")
	ErrLeaderNotReady           = errors.New("leader has not yet committed its no-op entry; read index not safe")
	ErrLogCompacted             = errors.New("log entry has been compacted")
	ErrSnapshotStale            = errors.New("snapshot is older than the installed boundary")
	ErrSnapshotBoundaryMismatch = errors.New("snapshot boundary term does not match")
	ErrInvalidSnapshot          = errors.New("snapshot is invalid")
	ErrSnapshotUnavailable      = errors.New("snapshot is unavailable")
	ErrSnapshotOffset           = errors.New("snapshot chunk offset is invalid")
	ErrSnapshotChecksum         = errors.New("snapshot checksum mismatch")
	ErrSnapshotChunkMismatch    = errors.New("snapshot chunk conflicts with staged data")
	ErrNodeUnhealthy            = errors.New("raft node is unhealthy")
	ErrMembershipChangePending  = errors.New("a membership change is already pending")
	ErrProposeChannelFull       = errors.New("propose channel is full")
)
