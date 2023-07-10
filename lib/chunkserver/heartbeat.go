package chunkserver

import (
	"gfs"
	"gfs/utils"
)

type ChunkInfo struct {
	Version gfs.ChunkVersion
	Handle  gfs.ChunkHandle
	Length  int64
}

type HeartBeatArgs struct {
	ServerInfo gfs.ServerInfo
	Chunks     []ChunkInfo
}

func (chunkserver *Chunkserver) sendHeartBeat() error {
	chunks := make([]ChunkInfo, 0)
	chunkserver.chunksLock.RLock()
	for _, chunk := range chunkserver.chunks {
		chunk.RLock()
		chunks = append(
			chunks,
			ChunkInfo{
				Version: chunk.version,
				Handle:  chunk.handle,
				Length:  chunk.length,
			},
		)
		chunk.RUnlock()
	}
	chunkserver.chunksLock.RUnlock()
	return utils.RemoteCall(chunkserver.master, "Master.ReceiveHeartBeatRPC",
		HeartBeatArgs{ServerInfo: chunkserver.server, Chunks: chunks}, nil)
}
