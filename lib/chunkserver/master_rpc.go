package chunkserver

import (
	"gfs"
	"gfs/utils"
)

// removeChunkInMaster let the master know that this chunk is no longer
// stored on this chunkserver.
func (chunkserver *Chunkserver) removeChunkInMaster(handle gfs.ChunkHandle) error {
	reply := gfs.RemoveChunkMetaReply{}
	return utils.RemoteCall(chunkserver.master, "Master.RemoveChunkMetaRPC",
		gfs.RemoveChunkMetaArgs{
			ServerInfo:  chunkserver.server,
			ChunkHandle: handle,
		},
		&reply,
	)
}
