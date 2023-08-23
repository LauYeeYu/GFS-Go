package chunkserver

import (
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"time"
)

type WriteRequest struct {
	Offset              gfs.Length // -1 if is record append, and becomes the true offset after the write operation
	Data                []byte
	ReturnChan          chan error
	ExceedLengthOfChunk bool
}

func (chunk *Chunk) leaseAboutToExpire() bool {
	return time.Now().Add(gfs.LeaseExtendBefore).After(chunk.leaseExpireTime)
}

// Handle all write requests from the client as the primary chunkserver.
// Two kinds of write requests are supported:
// 1. Record append: the offset is -1, and the data is the content to be appended.
// 2. Ranged write: the offset is the offset to write, and the data is the content to be written.
// Return the result through the ReturnChan channel.
func (chunk *Chunk) handleWriteRequest(request *WriteRequest) {
	chunk.Lock()
	chunk.flushLease()
	if !chunk.isPrimary {
		chunk.Unlock()
		request.ReturnChan <- errors.New(
			"Chunk.handleWriteRequest: not primary",
		)
		return
	}
	if chunk.leaseAboutToExpire() {
		chunk.Unlock()
		_ = chunk.extendLease(chunk.chunkserver)
		chunk.Lock()
		chunk.flushLease()
		if !chunk.isPrimary {
			chunk.Unlock()
			request.ReturnChan <- errors.New(
				"Chunk.handleWriteRequest: not primary",
			)
			return
		}
	}
	if request.Offset == -1 {
		offset, err, exceed := chunk.append(request.Data)
		chunk.Unlock()
		request.Offset = offset
		request.ExceedLengthOfChunk = exceed
		request.ReturnChan <- err
	} else {
		err := chunk.rangedWrite(request.Offset, request.Data)
		chunk.Unlock()
		request.ReturnChan <- err
	}
}

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
	reply := gfs.ExtendLeaseReply{}
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
		chunk.leaseExpireTime = reply.NewExpire
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
