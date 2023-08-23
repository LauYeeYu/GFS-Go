package chunkserver

import (
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
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

func (chunk *Chunk) extendLease(chunkserver *Chunkserver) error {
	chunk.Lock()
	defer chunk.Unlock()
	chunk.flushLease()
	reply := gfs.GrantLeaseReply{}
	gfs.Log(gfs.Info, fmt.Sprintf(
		"Chunkserver.extendLease: %v tries to extend lease of chunk %d",
		chunkserver.server.ServerAddr, chunk.handle,
	))
	err := utils.RemoteCall(
		chunkserver.master,
		"Master.ExtendLeaseRPC",
		gfs.ExtendLeaseArgs{
			ServerInfo:  chunkserver.server,
			ChunkHandle: chunk.handle,
		},
		&reply,
	)
	if err != nil {
		gfs.Log(gfs.Error, err.Error())
		return err
	}
	if !reply.Accepted {
		gfs.Log(gfs.Error, fmt.Sprintf(
			"Chunkserver.extendLease: lease extension for %d rejected",
			chunk.handle,
		))
		return errors.New("Chunkserver.extendLease: lease extension rejected")
	} else {
		gfs.Log(gfs.Info, fmt.Sprintf(
			"Chunkserver.extendLease: lease extension for %d accepted",
			chunk.handle,
		))
		return nil
	}
}

func (chunkserver *Chunkserver) RevokeLeaseRPC(
	args gfs.RevokeLeaseArgs,
	reply *gfs.RevokeLeaseReply,
) error {
	chunkserver.chunksLock.Lock()
	chunk, exist := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exist {
		reply.Accepted = false
		return errors.New("Chunkserver.RevokeLeaseRPC: chunk not found")
	}
	chunk.Lock()
	chunk.isPrimary = false
	chunk.Unlock()
	reply.Accepted = true
	return nil
}
