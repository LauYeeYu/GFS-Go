package gfs

// HeartBeats

type ChunkInfo struct {
	Version ChunkVersion
	Handle  ChunkHandle
	Length  int64
}

type HeartBeatArgs struct {
	ServerInfo ServerInfo
	Chunks     []ChunkInfo
}

type HeartBeatReply struct{}
