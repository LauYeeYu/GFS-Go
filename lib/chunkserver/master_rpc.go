package chunkserver

import (
	"errors"
	"fmt"
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

func (chunkserver *Chunkserver) UpdateChunkRPC(
	args gfs.UpdateChunkArgs,
	reply *gfs.UpdateChunkReply,
) error {
	chunkserver.chunksLock.Lock()
	chunk, exists := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exists {
		reply.Accepted = false
		reply.CurrentVersion = -1
		gfs.Log(gfs.Error, "Chunkserver.UpdateChunkRPC: chunk does not exist")
		return errors.New("Chunkserver.UpdateChunkRPC: chunk does not exist")
	}
	chunk.Lock()
	defer chunk.Unlock()
	if args.OriginalVersion != chunk.version {
		reply.Accepted = false
		reply.CurrentVersion = chunk.version
		if args.OriginalVersion < chunk.version {
			gfs.Log(
				gfs.Info,
				fmt.Sprintf(
					"Chunkserver.UpdateChunkRPC: version too old, current version: %d, original version: %d",
					chunk.version,
					args.OriginalVersion,
				),
			)
		} else {
			gfs.Log(gfs.Error,
				fmt.Sprintf(
					"Chunkserver.UpdateChunkRPC: version too new, current version: %d, original version: %d",
					chunk.version,
					args.OriginalVersion,
				),
			)
			return errors.New("Chunkserver.UpdateChunkRPC: version too new")
		}
	} else {
		chunk.version = args.NewVersion
		reply.Accepted = true
		reply.CurrentVersion = chunk.version
		gfs.Log(gfs.Info,
			fmt.Sprintf(
				"Chunkserver.UpdateChunkRPC: version updated, current version: %d, original version: %d",
				chunk.version,
				args.OriginalVersion,
			),
		)
	}
	return nil
}
