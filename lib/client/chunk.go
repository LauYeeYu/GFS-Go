package client

import (
	"errors"
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
func (client *Client) getReplicaInfoCache(
	handle gfs.ChunkHandle, needPrimary bool,
) (*ReplicaInfo, bool) {
	replicaInfo, ok := client.replicaCache[handle]
	if ok {
		if needPrimary && replicaInfo.PrimaryExpired() {
			delete(client.replicaCache, handle)
			return nil, false
		}
		return replicaInfo, true
	}
	return nil, false
}

func (client *Client) removeReplicaInfoCache(handle gfs.ChunkHandle) {
	if _, ok := client.replicaCache[handle]; ok {
		delete(client.replicaCache, handle)
	}
}

func (replicaInfo *ReplicaInfo) PrimaryExpired() bool {
	if replicaInfo.Primary == nil {
		return true
	} else {
		return time.Now().After(replicaInfo.PrimaryExpireTime)
	}
}

// getChunkReplicaInfo returns the replica info of the chunk and whether the
// chunk is in good status or not. Good status means the chunk exists or is
// not orphan (the chunk has replica).
func (client *Client) getChunkReplicaInfo(
	handle gfs.ChunkHandle, readOnly bool,
) (*ReplicaInfo, bool) {
	client.replicaLock.Lock()
	replicaInfo, ok := client.getReplicaInfoCache(handle, !readOnly)
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

// WriteChunk writes data to the chunk.
// retries is the number of retries. If retries is no more than 0, the function
// will not retry. Otherwise, the function will retry until succeed or the
// number of retries is used up.
//
// Return the number of status defined in gfs package and error.
func (client *Client) WriteChunk(
	handle gfs.ChunkHandle, offset gfs.Length, data []byte, retries int,
) (int, error) {
	replicaInfo, ok := client.getChunkReplicaInfo(handle, false)
	if !ok {
		return -1, errors.New("chunk is not in good status")
	}
	if replicaInfo.Primary == nil {
		return -1, errors.New("chunk has no primary")
	}
	reply := gfs.WriteChunkReply{}
	err := utils.RemoteCall(*replicaInfo.Primary, "Chunkserver.WriteChunkRPC",
		gfs.WriteChunkArgs{
			ServerInfo:     *replicaInfo.Primary,
			ChunkHandle:    handle,
			Offset:         offset,
			Data:           data,
			ChunkVersion:   replicaInfo.Version,
			ServersToWrite: replicaInfo.Locations,
		},
		&reply,
	)
	if retries > 0 && (err != nil || reply.Status != gfs.Successful) {
		gfs.Log(gfs.Warning, "retrying write chunk %d", handle)
		client.replicaLock.Lock()
		client.removeReplicaInfoCache(handle)
		client.replicaLock.Unlock()
		return client.WriteChunk(handle, offset, data, retries-1)
	}
	if err != nil {
		return -1, err
	}
	return reply.Status, nil
}

// ReadChunk reads data from the chunk.
// retries is the number of retries. If retries is no more than 0, the function
// will not retry. Otherwise, the function will retry until succeed or the
// number of retries is used up.
//
// Return the number of status defined in gfs package, data and error.
func (client *Client) ReadChunk(
	handle gfs.ChunkHandle, offset gfs.Length, length gfs.Length, retries int,
) (int, []byte, error) {
	replicaInfo, ok := client.getChunkReplicaInfo(handle, true)
	if !ok {
		return -1, nil, errors.New("chunk is not in good status")
	}
	if replicaInfo.Primary == nil {
		return -1, nil, errors.New("chunk has no primary")
	}
	reply := gfs.ReadChunkReply{}
	err := utils.RemoteCall(*replicaInfo.Primary, "Chunkserver.ReadChunkRPC",
		gfs.ReadChunkArgs{
			ChunkHandle:  handle,
			Offset:       offset,
			Length:       length,
			ChunkVersion: replicaInfo.Version,
		},
		&reply,
	)
	if retries > 0 && (err != nil || reply.Status != gfs.Successful) {
		gfs.Log(gfs.Warning, "retrying read chunk %d", handle)
		client.replicaLock.Lock()
		client.removeReplicaInfoCache(handle)
		client.replicaLock.Unlock()
		return client.ReadChunk(handle, offset, length, retries-1)
	}
	if err != nil {
		return -1, nil, err
	}
	return reply.Status, reply.Data, nil
}

// RecordAppendChunk appends data to the chunk.
// retries is the number of retries. If retries is no more than 0, the function
// will not retry. Otherwise, the function will retry until succeed or the
// number of retries is used up.
//
// Return the number of status defined in gfs package, offset and error.
func (client *Client) RecordAppendChunk(
	handle gfs.ChunkHandle, data []byte, retries int,
) (int, gfs.Length, error) {
	if gfs.Length(len(data))*4 > gfs.ChunkSize {
		return gfs.TooLargeForRecordAppend, -1, nil
	}
	replicaInfo, ok := client.getChunkReplicaInfo(handle, false)
	if !ok {
		return -1, -1, errors.New("chunk is not in good status")
	}
	if replicaInfo.Primary == nil {
		return -1, -1, errors.New("chunk has no primary")
	}
	reply := gfs.RecordAppendChunkReply{}
	err := utils.RemoteCall(*replicaInfo.Primary, "Chunkserver.RecordAppendChunkRPC",
		gfs.RecordAppendChunkArgs{
			ServerInfo:     *replicaInfo.Primary,
			ChunkHandle:    handle,
			Data:           data,
			ChunkVersion:   replicaInfo.Version,
			ServersToWrite: replicaInfo.Locations,
		},
		&reply,
	)
	if retries > 0 && (err != nil || reply.Status != gfs.Successful) {
		gfs.Log(gfs.Warning, "retrying record append chunk %d", handle)
		client.replicaLock.Lock()
		client.removeReplicaInfoCache(handle)
		client.replicaLock.Unlock()
		return client.RecordAppendChunk(handle, data, retries-1)
	}
	if err != nil {
		return -1, -1, err
	}
	return reply.Status, reply.Offset, nil
}
