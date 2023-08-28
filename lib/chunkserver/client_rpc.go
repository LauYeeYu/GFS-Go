package chunkserver

import "gfs"

func (chunkserver *Chunkserver) GetChunkSizeRPC(
	args gfs.GetChunkSizeArgs,
	reply *gfs.GetChunkSizeReply,
) error {
	chunkserver.chunksLock.Lock()
	chunk, exist := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exist {
		reply.Size = -1
		return nil
	}
	chunk.Lock()
	reply.Size = chunk.length()
	chunk.Unlock()
	return nil
}
