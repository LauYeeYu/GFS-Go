package master

import (
	"container/heap"
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

// hasLeaseHolder returns true if the chunk has a leaseholder and the lease
// has not expired.
// Note: this function does not lock the chunkMeta, so it should be called
// with chunkMeta.Lock() held.
func (chunkMeta *ChunkMetadata) hasLeaseHolder() bool {
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

// grantLease grants lease for a chunk. If the function doesn't return error,
// the chunk will have a leaseholder. You may check the leaseholder field of
// the chunk to get the leaseholder.
func (master *Master) grantLease(chunkHandle gfs.ChunkHandle) error {
	master.chunksLock.Lock()
	chunk, ok := master.chunks[chunkHandle]
	if !ok {
		master.chunksLock.Unlock()
		return errors.New(fmt.Sprintf("chunk %d does not exist", chunkHandle))
	}

	// Inform all chunkservers to update the lease
	chunk.RLock()
	if chunk.hasLeaseHolder() {
		chunk.RUnlock()
		master.chunksLock.Unlock()
		return errors.New(fmt.Sprintf("chunk %d already has lease", chunkHandle))
	}
	if len(chunk.Servers) == 0 {
		chunk.RUnlock()
		master.chunksLock.Unlock()
		return errors.New(fmt.Sprintf("chunk %d has no replica", chunkHandle))
	}
	chunk.LeaseLock.Lock()
	master.chunkserversLock.RLock()
	defer chunk.LeaseLock.Unlock()
	servers := make(map[gfs.ServerInfo]*ChunkserverData)
	for server := range chunk.Servers {
		servers[server] = master.chunkservers[server]
	}
	master.chunkserversLock.RUnlock()
	master.chunksLock.Unlock()
	version := chunk.Version
	chunk.RUnlock()
	finished := make(chan struct{})
	var updateErr error
	updateErr = nil
	count := 0
	var updateLock sync.Mutex
	for info := range servers {
		go func(server gfs.ServerInfo) {
			reply := gfs.UpdateChunkReply{}
			err := utils.RemoteCall(server, "Chunkserver.UpdateChunkRPC",
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
		data.Lock()
		if first || len(data.Lease) < numberOfLeases {
			first = false
			data.flushLease()
			numberOfLeases = len(data.Lease)
			leaseholder = serverInfo
		}
		data.Unlock()
	}

	// Increment the chunk version
	err := master.appendLog(
		MakeOperationLogEntryHeader(IncrementChunkVersionOperation),
		&IncrementChunkVersionOperationLogEntry{chunkHandle},
	)
	if err != nil {
		return err
	}

	// Send lease to the leaseholder
	// In current implementation, we only grant lease once
	chunk.Lock()
	now := time.Now()
	expire := now.Add(gfs.LeaseTimeout)
	if chunk.hasLeaseHolder() {
		chunk.Unlock()
		return nil
	}
	chunk.Leaseholder = &leaseholder
	chunk.LeaseExpire = expire
	chunk.Unlock()
	master.chunkserversLock.Lock()
	master.chunkservers[leaseholder].Lock()
	master.chunkservers[leaseholder].Lease[chunkHandle] = expire
	master.chunkservers[leaseholder].Unlock()
	master.chunkserversLock.Unlock()
	return master.sendLease(gfs.GrantLeaseArgs{
		ServerInfo:  leaseholder,
		ChunkHandle: chunkHandle,
		LeaseExpire: expire,
	})
}

func (master *Master) revokeLease(chunkHandle gfs.ChunkHandle) error {
	master.chunksLock.Lock()
	defer master.chunksLock.Unlock()
	chunk, ok := master.chunks[chunkHandle]
	if !ok {
		return errors.New(fmt.Sprintf("chunk %d does not exist", chunkHandle))
	}
	chunk.Lock()
	if !chunk.hasLeaseHolder() {
		chunk.Unlock()
		return nil
	}
	leaseholder := *chunk.Leaseholder
	chunk.Unlock()
	reply := gfs.RevokeLeaseReply{}
	err := utils.RemoteCall(leaseholder, "Chunkserver.RevokeLeaseRPC",
		gfs.RevokeLeaseArgs{
			ServerInfo:  leaseholder,
			ChunkHandle: chunkHandle,
		},
		&reply,
	)
	if err != nil {
		return err
	}
	if !reply.Accepted {
		return errors.New("Master.revokeLease: lease not accepted")
	}
	chunk.Lock()
	chunk.Leaseholder = nil
	chunk.Unlock()
	master.chunkserversLock.Lock()
	chunkserver := master.chunkservers[leaseholder]
	master.chunkserversLock.Unlock()
	chunkserver.Lock()
	// To avoid removing the lease twice, we check if the lease is still
	// held by the chunkserver recorded in the master
	if _, ok := chunkserver.Lease[chunkHandle]; ok {
		delete(chunkserver.Lease, chunkHandle)
	}
	chunkserver.Unlock()
	return nil
}

type dispatchInfo struct {
	server          gfs.ServerInfo
	numberOfChunks  int
	chunkserverData *ChunkserverData
}
type dispatchQueue []*dispatchInfo

func (pq dispatchQueue) Len() int { return len(pq) }
func (pq dispatchQueue) Less(i, j int) bool {
	return pq[i].numberOfChunks < pq[j].numberOfChunks
}
func (pq dispatchQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }
func (pq *dispatchQueue) Push(x any) {
	*pq = append(*pq, x.(*dispatchInfo))
}
func (pq *dispatchQueue) Pop() any {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

// dispatchChunkToChunkserver dispatches a chunk to a couple of chunkservers.
// The chunk will be dispatched to the chunkservers with the least number of
// chunks. This implementation ignores a lot of factors.
func (master *Master) dispatchChunkToChunkserver(
	chunkHandle gfs.ChunkHandle, chunkMeta *ChunkMetadata,
) {
	master.chunkserversLock.RLock()
	var dispatchList dispatchQueue = make([]*dispatchInfo, 0, len(master.chunkservers))
	var dispatchListLock sync.Mutex
	numberOfChunkservers := len(master.chunkservers)
	for server, data := range master.chunkservers {
		data.Lock()
		dispatchList = append(dispatchList, &dispatchInfo{
			server:          server,
			numberOfChunks:  len(data.Chunks),
			chunkserverData: data,
		})
		data.Unlock()
	}
	master.chunkserversLock.RUnlock()
	heap.Init(&dispatchList)
	chunkMeta.Lock()
	numberOfReplicas := min(numberOfChunkservers, len(chunkMeta.Servers))
	for i := 0; i < numberOfReplicas; i++ {
		dispatchListLock.Lock()
		server := heap.Pop(&dispatchList).(*dispatchInfo)
		dispatchListLock.Unlock()
		gfs.Log(gfs.Info, fmt.Sprintf("dispatching chunk %d to %v", chunkHandle, server.server.ServerAddr))
		chunkMeta.Servers.Add(server.server)
		server.chunkserverData.Lock()
		server.chunkserverData.Chunks.Add(chunkHandle)
		server.chunkserverData.Unlock()
		go func(info *dispatchInfo) {
			reply := gfs.AddNewChunkReply{}
			err := utils.RemoteCall(info.server, "Chunkserver.AddNewChunkRPC",
				gfs.AddNewChunkArgs{ServerInfo: info.server, ChunkHandle: chunkHandle},
				&reply,
			)
			dispatchListLock.Lock()
			for (err != nil || !reply.Successful) && len(dispatchList) > 0 {
				newServer := heap.Pop(&dispatchList).(*dispatchInfo)
				dispatchListLock.Unlock()
				gfs.Log(gfs.Error,
					fmt.Sprintf(
						"dispatching chunk %d to %v failed",
						chunkHandle, server.server.ServerAddr,
					),
				)
				chunkMeta.Lock()
				chunkMeta.Servers.Remove(info.server)
				chunkMeta.Servers.Add(newServer.server)
				chunkMeta.Unlock()
				info.chunkserverData.Lock()
				info.chunkserverData.Chunks.Remove(chunkHandle)
				info.chunkserverData.Unlock()
				newServer.chunkserverData.Lock()
				newServer.chunkserverData.Chunks.Add(chunkHandle)
				newServer.chunkserverData.Unlock()
				err = utils.RemoteCall(newServer.server, "Chunkserver.AddNewChunkRPC",
					gfs.AddNewChunkArgs{ServerInfo: newServer.server, ChunkHandle: chunkHandle},
					&reply,
				)
				if err == nil && reply.Successful {
					return
				}
				info = newServer
				dispatchListLock.Lock()
			}
		}(server)
	}
	chunkMeta.Unlock()
}
