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
	chunkServer, ok := master.chunkservers[args.ServerInfo]
	if !ok {
		gfs.Log(gfs.Info, fmt.Sprintf("New chunkserver %v joined\n", args.ServerInfo))
		master.chunkservers[args.ServerInfo] = &ChunkserverData{
			LastSeen: time.Now(),
			Chunks:   utils.MakeSet[gfs.ChunkHandle](),
			Lease:    make(map[gfs.ChunkHandle]time.Time),
		}
	} else {
		chunkServer.LastSeen = time.Now()
	}
	master.chunkserversLock.Unlock()

	// update chunk status
	expiredChunks := make([]gfs.ChunkHandle, 0)
	for _, chunk := range args.Chunks {
		master.chunksLock.Lock()
		chunkMeta, existChunk := master.chunks[chunk.Handle]
		if existChunk {
			if chunk.Version < master.chunks[chunk.Handle].Version {
				gfs.Log(gfs.Warning, fmt.Sprintf(
					"Chunk %v version %v is stale, ignore",
					chunk.Handle, chunk.Version))
				expiredChunks = append(expiredChunks, chunk.Handle)
				chunkMeta.removeChunkserver(args.ServerInfo)
			} else if chunk.Version > master.chunks[chunk.Handle].Version {
				gfs.Log(gfs.Info, fmt.Sprintf(
					"Chunk %v version %v is newer, update",
					chunk.Handle, chunk.Version))
				chunkMeta.Version = chunk.Version
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
		master.chunksLock.Unlock()
	}

	// return expired chunks
	reply.ExpiredChunks = expiredChunks
	return nil
}

func (master *Master) sendLease(args gfs.GrantLeaseArgs) error {
	reply := &gfs.GrantLeaseReply{}
	err := utils.RemoteCall(args.ServerInfo, "ChunkServer.ReceiveLeaseRPC",
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

func (chunkserverData *ChunkserverData) flushLease() {
	for chunk, expire := range chunkserverData.Lease {
		if expire.Before(time.Now()) {
			delete(chunkserverData.Lease, chunk)
		}
	}
}
