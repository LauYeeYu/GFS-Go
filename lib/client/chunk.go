package client

import (
	"gfs"
	"gfs/utils"
	"math/rand"
	"time"
)

// ReplicaInfo info caches the replicas of a chunk.
type ReplicaInfo struct {
	ChunkHandle       gfs.ChunkHandle
	Version           gfs.ChunkVersion
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

// getChunkReplicaInfo returns the replica info of the chunk and whether the
// chunk is in good status or not. Good status means the chunk exists or is
// not orphan (the chunk has replica).
func (client *Client) getChunkReplicaInfo(
	handle gfs.ChunkHandle, readOnly bool,
) (*ReplicaInfo, bool) {
	client.replicaLock.Lock()
	replicaInfo, ok := client.getReplicaInfoCache(handle)
	client.replicaLock.Unlock()
	if ok {
		return replicaInfo, len(replicaInfo.Locations) > 0
	}
	reply := gfs.GetChunkReplicasReply{}
	err := utils.RemoteCall(client.master, "Master.GetChunkReplicasRPC",
		gfs.GetChunkReplicasArgs{ChunkHandle: handle, ReadOnly: readOnly},
		&reply,
	)
	if err != nil || !reply.Valid {
		return nil, false
	}
	if reply.Orphan {
		gfs.Log(gfs.Warning, "chunk %d is orphan", handle)
	}
	var primary *gfs.ServerInfo = nil
	if reply.HasPrimary {
		primary = &reply.Primary
	}
	replicaInfo = &ReplicaInfo{
		ChunkHandle:       handle,
		Version:           reply.Version,
		Locations:         reply.Locations,
		Primary:           primary,
		PrimaryExpireTime: reply.PrimaryExpireTime,
	}
	client.replicaLock.Lock()
	client.replicaCache[handle] = replicaInfo
	client.replicaLock.Unlock()
	return replicaInfo, reply.Orphan
}

// GetOneReplica returns a random replica of the chunk.
// If the chunk has no replica, the function will return the default
// serverInfo and false. Otherwise, the function will return a random
// replica and true. This function is useful when reading a chunk.
func (replicaInfo *ReplicaInfo) GetOneReplica() (gfs.ServerInfo, bool) {
	if len(replicaInfo.Locations) == 0 {
		return gfs.ServerInfo{}, false
	}
	return replicaInfo.Locations[rand.Intn(len(replicaInfo.Locations))], true
}
