package master

import (
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"sync"
	"time"
)

func (chunkMeta *ChunkMetadata) addChunkserver(server gfs.ServerInfo) {
	chunkMeta.Lock()
	defer chunkMeta.Unlock()
	chunkMeta.Servers.Add(server)
}

func (chunkMeta *ChunkMetadata) hasLeaseHolder() bool {
	chunkMeta.Lock()
	defer chunkMeta.Unlock()
	return chunkMeta.Leaseholder != nil && chunkMeta.LeaseExpire.After(time.Now())
}

func (master *Master) reduceChunkRef(chunk gfs.ChunkHandle) error {
	master.chunksLock.Lock()
	defer master.chunksLock.Unlock()
	chunkMeta, ok := master.chunks[chunk]
	if !ok {
		return errors.New(fmt.Sprintf("chunk %d does not exist", chunk))
	}
	chunkMeta.Lock()
	defer chunkMeta.Unlock()
	chunkMeta.RefCount--
	if chunkMeta.RefCount == 0 {
		delete(master.chunks, chunk)
	}
	return nil
}

func (master *Master) grantLease(chunkHandle gfs.ChunkHandle) error {
	master.chunksLock.Lock()
	chunk, ok := master.chunks[chunkHandle]
	if !ok {
		master.chunksLock.Unlock()
		return errors.New(fmt.Sprintf("chunk %d does not exist", chunkHandle))
	}

	// Inform all chunkservers to update the lease
	chunk.LeaseLock.Lock()
	defer chunk.LeaseLock.Unlock()
	chunk.RLock()
	servers := make(map[gfs.ServerInfo]*ChunkserverData)
	for server := range chunk.Servers {
		servers[server] = master.chunkservers[server]
	}
	master.chunksLock.Unlock()
	version := chunk.Version
	chunk.RUnlock()
	finished := make(chan struct{})
	var updateErr error
	updateErr = nil
	count := 0
	var updateLock sync.Mutex
	for info, _ := range servers {
		go func(server gfs.ServerInfo) {
			reply := gfs.UpdateChunkReply{}
			err := utils.RemoteCall(server, "ChunkServer.UpdateChunkRPC",
				gfs.UpdateChunkArgs{
					ServerInfo:      server,
					ChunkHandle:     chunkHandle,
					OriginalVersion: version,
					NewVersion:      version + 1,
				},
				&reply,
			)
			updateLock.Lock()
			defer updateLock.Unlock()
			count++
			if err != nil {
				chunk.Lock()
				chunk.Servers.Remove(server)
				chunk.Unlock()
			} else if !reply.Accepted {
				if reply.CurrentVersion > version { // Chunk is newer than recorded
					chunk.Lock()
					if chunk.Version < reply.CurrentVersion {
						chunk.Version = reply.CurrentVersion
					}
					chunk.Unlock()
					updateErr = errors.New("chunk is newer than recorded")
					finished <- struct{}{}
					return
				} else { // Other problems, remove the chunkserver
					chunk.Lock()
					chunk.Servers.Remove(server)
					chunk.Unlock()
				}
			}
			if updateErr == nil && count == len(servers) {
				finished <- struct{}{}
			}
		}(info)
	}
	<-finished
	if updateErr != nil {
		return updateErr
	}

	// Find a chunkserver to grant lease
	if len(servers) == 0 {
		return errors.New(fmt.Sprintf("chunk %d has no replica", chunkHandle))
	}
	var numberOfLeases int
	var leaseholder gfs.ServerInfo
	first := true
	for serverInfo, data := range servers {
		if first || len(data.Lease) < numberOfLeases {
			first = false
			data.flushLease()
			numberOfLeases = len(data.Lease)
			leaseholder = serverInfo
		}
	}

	// Send lease to the leaseholder
	// In current implementation, we only grant lease once
	now := time.Now()
	err := master.appendLog(
		MakeOperationLogEntryHeader(GrantLeaseOperation),
		&GrantLeaseOperationLogEntry{
			Chunk:          chunkHandle,
			Leaseholder:    leaseholder,
			LeaseGrantTime: now,
			LeaseExpire:    now.Add(gfs.LeaseTimeout),
			Version:        version + 1,
			Override:       false,
		},
	)
	return err
}
