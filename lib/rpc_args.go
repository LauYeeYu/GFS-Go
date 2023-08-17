package gfs

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
