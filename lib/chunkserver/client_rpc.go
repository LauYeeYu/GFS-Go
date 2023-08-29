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
	if chunkserver.server != args.ServerInfo {
		reply.Status = gfs.WrongServer
		return nil
	}
	if args.Offset+gfs.Length(len(args.Data)) > gfs.ChunkSize {
		reply.Status = gfs.ExceedLengthOfChunk
		return nil
	}
	chunkserver.chunksLock.Lock()
	chunk, exist := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exist {
		reply.Status = gfs.ChunkNotExist
		return nil
	}
	chunk.RLock()
	if chunk.version != args.ChunkVersion {
		reply.Status = gfs.ChunkVersionNotMatch
		chunk.RUnlock()
		return nil
	}
	if !chunk.IsPrimary() {
		reply.Status = gfs.NotPrimary
		chunk.RUnlock()
		return nil
	}
	chunk.RUnlock()
	writeRequest := WriteRequest{
		ChunkHandle:         args.ChunkHandle,
		Offset:              args.Offset,
		Data:                args.Data,
		ServersToWrite:      args.ServersToWrite,
		ReturnChan:          make(chan error),
		ExceedLengthOfChunk: false,
	}
	chunk.writeChannel.In <- &writeRequest
	err := <-writeRequest.ReturnChan
	if err != nil {
		gfs.Log(gfs.Error, err.Error())
		reply.Status = gfs.Failed
	} else {
		reply.Status = gfs.Successful
	}
	return nil
}

func (chunkserver *Chunkserver) RecordAppendChunkRPC(
	args gfs.RecordAppendChunkArgs,
	reply *gfs.RecordAppendChunkReply,
) error {
	if chunkserver.server != args.ServerInfo {
		reply.Status = gfs.WrongServer
		return nil
	}
	chunkserver.chunksLock.Lock()
	chunk, exist := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exist {
		reply.Status = gfs.ChunkNotExist
		return nil
	}
	chunk.RLock()
	if chunk.version != args.ChunkVersion {
		reply.Status = gfs.ChunkVersionNotMatch
		chunk.RUnlock()
		return nil
	}
	if !chunk.IsPrimary() {
		reply.Status = gfs.NotPrimary
		chunk.RUnlock()
		return nil
	}
	chunk.RUnlock()
	writeRequest := WriteRequest{
		ChunkHandle:         args.ChunkHandle,
		Offset:              -1,
		Data:                args.Data,
		ServersToWrite:      args.ServersToWrite,
		ReturnChan:          make(chan error),
		ExceedLengthOfChunk: false,
	}
	chunk.writeChannel.In <- &writeRequest
	err := <-writeRequest.ReturnChan
	if err != nil {
		gfs.Log(gfs.Error, err.Error())
		reply.Status = gfs.Failed
	} else {
		reply.Status = gfs.Successful
		reply.Offset = writeRequest.Offset
	}
	return nil
}

// ReadChunkRPC is used by the client to read data from a chunk. This
// operation guarantees that the data is complete and consistent. It
// happens without write operations doing at the same time on the same
// chunk.
//
// Note: Read chunk operation won't synchronise with write chunk operation.
// If you would like to make sure that the read operation happens after
// the write operation, you should read after the write operation returns.
func (chunkserver *Chunkserver) ReadChunkRPC(
	args gfs.ReadChunkArgs,
	reply *gfs.ReadChunkReply,
) error {
	if chunkserver.server != args.ServerInfo {
		reply.Status = gfs.WrongServer
		return nil
	}
	chunkserver.chunksLock.Lock()
	chunk, exist := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exist {
		reply.Status = gfs.ChunkNotExist
		return nil
	}
	chunk.RLock()
	if chunk.version != args.ChunkVersion {
		reply.Status = gfs.ChunkVersionNotMatch
		chunk.RUnlock()
		return nil
	}
	data := make([]byte, args.Length)
	err := chunk.rangedRead(args.Offset, data)
	chunk.RUnlock()
	if err != nil {
		reply.Status = gfs.Failed
		return nil
	}
	reply.Status = gfs.Successful
	reply.Data = data
	return nil
}
