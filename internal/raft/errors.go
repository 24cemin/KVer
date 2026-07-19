package raft

import "errors"

var (
	ErrOutOfRange              = errors.New("log index out of range")
	ErrNotLeader               = errors.New("node is not the leader")
	ErrLeaderNotReady          = errors.New("leader has not yet committed its no-op entry; read index not safe")
	ErrLogCompacted            = errors.New("log entry has been compacted")
	ErrMembershipChangePending = errors.New("a membership change is already pending")
	ErrProposeChannelFull      = errors.New("propose channel is full")
)
