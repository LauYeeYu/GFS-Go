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

func (chunkserver *Chunkserver) WriteChunkRPC(
	args gfs.WriteChunkArgs,
	reply *gfs.WriteChunkReply,
) error {
	if args.Offset+gfs.Length(len(args.Data)) > gfs.ChunkSize {
		reply.Status = gfs.WriteExceedLengthOfChunk
		return nil
	}
	chunkserver.chunksLock.Lock()
	chunk, exist := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exist {
		reply.Status = gfs.WriteChunkNotExist
		return nil
	}
	chunk.Lock()
	if chunk.version != args.ChunkVersion {
		reply.Status = gfs.WriteChunkVersionNotMatch
		chunk.Unlock()
		return nil
	}
	chunk.flushLease()
	if !chunk.isPrimary {
		reply.Status = gfs.WriteNotPrimary
		chunk.Unlock()
		return nil
	}
	chunk.Unlock()
	writeRequest := WriteRequest{
		ChunkHandle:         args.ChunkHandle,
		Offset:              args.Offset,
		Data:                args.Data,
		ServersToWrite:      make([]gfs.ServerInfo, 0),
		ReturnChan:          make(chan error),
		ExceedLengthOfChunk: false,
	}
	chunk.writeChannel.In <- &writeRequest
	err := <-writeRequest.ReturnChan
	if err != nil {
		reply.Status = gfs.WriteFailed
	} else {
		reply.Status = gfs.WriteSuccessful
	}
	return nil
}
