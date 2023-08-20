package chunkserver

import (
	"errors"
	"gfs"
)

// Lease control

func (chunkserver *Chunkserver) ReceiveLeaseRPC(
	args gfs.GrantLeaseArgs,
	reply *gfs.GrantLeaseReply,
) error {
	if args.ServerInfo != chunkserver.server {
		reply.Accepted = false
		return errors.New("Chunkserver.ReceiveLeaseRPC: wrong server")
	}
	chunkserver.chunksLock.Lock()
	chunk, exist := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exist {
		reply.Accepted = false
		return errors.New("Chunkserver.ReceiveLeaseRPC: chunk not found")
	}
	chunk.Lock()
	chunk.isPrimary = true
	chunk.Unlock()
	reply.Accepted = true
	return nil
}
