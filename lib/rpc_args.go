package gfs

import "time"

// HeartBeats

type ChunkInfo struct {
	Version ChunkVersion
	Handle  ChunkHandle
	Length  Length
}

type HeartBeatArgs struct {
	ServerInfo ServerInfo
	Chunks     []ChunkInfo
}

type HeartBeatReply struct {
	ExpiredChunks    []ChunkHandle
	RequireAllChunks bool
}

type GrantLeaseArgs struct {
	ServerInfo  ServerInfo
	ChunkHandle ChunkHandle
	LeaseExpire time.Time
}

type GrantLeaseReply struct {
	Accepted bool
}

type UpdateChunkArgs struct {
	ServerInfo      ServerInfo
	ChunkHandle     ChunkHandle
	OriginalVersion ChunkVersion
	NewVersion      ChunkVersion
}

type UpdateChunkReply struct {
	Accepted       bool
	CurrentVersion ChunkVersion // -1 if not exist
}

type ExtendLeaseArgs struct {
	ServerInfo  ServerInfo
	ChunkHandle ChunkHandle
}

type ExtendLeaseReply struct {
	Accepted  bool
	NewExpire time.Time
}

type RevokeLeaseArgs struct {
	ServerInfo  ServerInfo
	ChunkHandle ChunkHandle
}

type RevokeLeaseReply struct {
	Accepted bool
}
