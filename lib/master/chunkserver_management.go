package master

import (
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"sync"
	"time"
)

type ChunkserverData struct {
	sync.RWMutex
	LastSeen time.Time
	Chunks   utils.Set[gfs.ChunkHandle]
	Lease    map[gfs.ChunkHandle]time.Time
}

func (master *Master) ReceiveHeartBeatRPC(
	args gfs.HeartBeatArgs,
	reply *gfs.HeartBeatReply,
) error {
	// register new chunkserver if needed
	master.chunkserversLock.Lock()
	chunkserver, ok := master.chunkservers[args.ServerInfo]
	if !ok {
		gfs.Log(gfs.Info, fmt.Sprintf("New chunkserver %v joined\n", args.ServerInfo))
		chunkserver = &ChunkserverData{
			LastSeen: time.Now(),
			Chunks:   utils.MakeSet[gfs.ChunkHandle](),
			Lease:    make(map[gfs.ChunkHandle]time.Time),
		}
		master.chunkservers[args.ServerInfo] = chunkserver
		reply.RequireAllChunks = true
	} else {
		chunkserver.LastSeen = time.Now()
		reply.RequireAllChunks = false
	}
	master.chunkserversLock.Unlock()

	// Update status of good chunks
	// This includes:
	// 1. Update the chunks' state
	// 2. Update the chunkserver's state
	expiredChunks := make([]gfs.ChunkHandle, 0)
	for _, chunk := range args.Chunks {
		master.chunksLock.Lock()
		chunkMeta, existChunk := master.chunks[chunk.Handle]
		master.chunksLock.Unlock()
		if existChunk {
			if chunk.Version < chunkMeta.Version {
				gfs.Log(gfs.Warning, fmt.Sprintf(
					"Chunk %v version %v is stale, ignore",
					chunk.Handle, chunk.Version))
				expiredChunks = append(expiredChunks, chunk.Handle)
				chunkMeta.removeChunkserver(args.ServerInfo)
			} else if chunk.Version > chunkMeta.Version {
				gfs.Log(gfs.Info, fmt.Sprintf(
					"Chunk %v version %v is newer, update",
					chunk.Handle, chunk.Version))
				err := master.appendLog(
					MakeOperationLogEntryHeader(UpdateChunkVersionOperation),
					&UpdateChunkVersionOperationLogEntry{
						Chunk:      chunk.Handle,
						NewVersion: chunk.Version,
					},
				)
				if err != nil {
					gfs.Log(gfs.Error, err.Error())
				}
				chunkMeta.Servers = utils.MakeSet[gfs.ServerInfo](gfs.ServerInfo{})
				// No need to inform all chunkserver to remove this chunk, they will
				// do it automatically through heartbeat
			} else {
				chunkMeta.addChunkserver(args.ServerInfo)
			}
		} else {
			gfs.Log(
				gfs.Warning,
				fmt.Sprintf("Chunk %v does not exist, ignore", chunk.Handle),
			)
		}
	}
	chunkserver.Lock()
	for _, chunk := range args.Chunks {
		chunkserver.Chunks.Add(chunk.Handle)
	}
	chunkserver.Unlock()

	// Handle the corrupted chunks
	for _, chunk := range args.CorruptedChunks {
		master.chunksLock.Lock()
		chunkMeta, existChunk := master.chunks[chunk]
		master.chunksLock.Unlock()
		if existChunk {
			chunkMeta.removeChunkserver(args.ServerInfo)
		}
	}
	chunkserver.Lock()
	for _, chunk := range args.CorruptedChunks {
		chunkserver.Chunks.RemoveIfExist(chunk)
		if _, exist := chunkserver.Lease[chunk]; exist {
			delete(chunkserver.Lease, chunk)
		}
	}
	chunkserver.Unlock()

	// return expired chunks
	reply.ExpiredChunks = expiredChunks
	return nil
}

func (master *Master) sendLease(args gfs.GrantLeaseArgs) error {
	reply := &gfs.GrantLeaseReply{}
	err := utils.RemoteCall(args.ServerInfo, "Chunkserver.ReceiveLeaseRPC",
		args, reply)
	if err != nil {
		return err
	}
	master.chunkserversLock.Lock()
	master.chunkservers[args.ServerInfo].LastSeen = time.Now()
	master.chunkserversLock.Unlock()
	if !reply.Accepted {
		return errors.New("Master.grantLease: lease not accepted")
	}
	return nil
}

// flushLease removes expired lease
func (chunkserverData *ChunkserverData) flushLease() {
	for chunk, expire := range chunkserverData.Lease {
		if expire.Before(time.Now()) {
			delete(chunkserverData.Lease, chunk)
		}
	}
}

func (master *Master) ExtendLeaseRPC(
	args gfs.ExtendLeaseArgs,
	reply *gfs.ExtendLeaseReply,
) error {
	chunkserverData, ok := master.chunkservers[args.ServerInfo]
	master.chunkserversLock.Unlock()
	if !ok {
		return errors.New("Master.ExtendLease: chunkserver not found")
	}
	master.chunksLock.Lock()
	chunk, ok := master.chunks[args.ChunkHandle]
	master.chunksLock.Unlock()
	if !ok {
		return errors.New("Master.ExtendLease: chunk not found")
	}
	chunk.Lock()
	chunkserverData.Lock()
	if !chunk.hasLeaseHolder() || chunk.Leaseholder == nil ||
		*chunk.Leaseholder != args.ServerInfo {
		reply.Accepted = false
	} else {
		newExpire := time.Now().Add(gfs.LeaseTimeout)
		chunkserverData.Lease[args.ChunkHandle] = newExpire
		reply.Accepted = true
		reply.NewExpire = newExpire
		chunk.LeaseExpire = newExpire
	}
	chunkserverData.Unlock()
	chunk.Unlock()
	return nil
}

// RemoveChunkMetaRPC is called by chunkserver to inform the master that
// this chunk is no longer stored on this chunkserver.
// The function will remove the chunkserver from the chunk's server list
// and automatically revoke the lease if the chunkserver is the leaseholder
// without further confirmation.
func (master *Master) RemoveChunkMetaRPC(
	args gfs.RemoveChunkMetaArgs,
	_ *gfs.RemoveChunkMetaReply,
) error {
	master.chunksLock.Lock()
	defer master.chunksLock.Unlock()
	chunk, ok := master.chunks[args.ChunkHandle]
	if !ok {
		return errors.New("Master.RemoveChunkMetaRPC: chunk not found")
	}
	gfs.Log(gfs.Warning, fmt.Sprintf(
		"Chunkserver %v ask master to remove it from chunk %d's list.",
		args.ServerInfo,
		args.ChunkHandle,
	))
	chunk.Lock()
	defer chunk.Unlock()
	chunk.removeChunkserver(args.ServerInfo)
	if chunk.hasLeaseHolder() && *chunk.Leaseholder == args.ServerInfo {
		chunk.Leaseholder = nil
	}
	return nil
}
