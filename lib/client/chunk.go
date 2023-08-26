package client

import (
	"gfs"
	"gfs/utils"
	"time"
)

// ReplicaInfo info caches the replicas of a chunk.
type ReplicaInfo struct {
	ChunkHandle       gfs.ChunkHandle
	Locations         []gfs.ServerInfo
	Primary           *gfs.ServerInfo
	PrimaryExpireTime time.Time
}

// getReplicaInfoCache returns the cached replica info of the chunk.
// The function will check whether the cached replica info is expired.
// If the cached replica info is expired, the function will delete the
// cached replica info and return false.
func (client *Client) getReplicaInfoCache(handle gfs.ChunkHandle) (*ReplicaInfo, bool) {
	replicaInfo, ok := client.replicaCache[handle]
	if ok && replicaInfo.Expired() {
		delete(client.replicaCache, handle)
		ok = false
	}
	return replicaInfo, ok
}

func (replicaInfo *ReplicaInfo) Expired() bool {
	return time.Now().After(replicaInfo.PrimaryExpireTime)
}

func (client *Client) getChunkReplicaInfo(handle gfs.ChunkHandle) (*ReplicaInfo, bool) {
	client.replicaLock.Lock()
	replicaInfo, ok := client.getReplicaInfoCache(handle)
	client.replicaLock.Unlock()
	if ok {
		return replicaInfo, true
	}
	reply := gfs.GetChunkReplicasReply{}
	err := utils.RemoteCall(client.master, "Master.GetChunkReplicasRPC",
		gfs.GetChunkReplicasArgs{ChunkHandle: handle},
		&reply,
	)
	if err != nil || !reply.Valid {
		return nil, false
	}
	replicaInfo = &ReplicaInfo{
		ChunkHandle:       handle,
		Locations:         reply.Locations,
		Primary:           &reply.Primary,
		PrimaryExpireTime: reply.PrimaryExpireTime,
	}
	client.replicaLock.Lock()
	client.replicaCache[handle] = replicaInfo
	client.replicaLock.Unlock()
	return replicaInfo, true
}
