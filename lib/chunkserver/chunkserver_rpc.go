package chunkserver

import (
	"gfs"
	"gfs/utils"
)

func (chunkserver *Chunkserver) WriteDataAndForwardRPC(
	args gfs.WriteDataAndForwardArgs,
	reply *gfs.WriteDataAndForwardReply,
) error {
	result := make(chan gfs.WriteDataAndForwardReply)
	go func() {
		if len(args.ServerToWrite) > 1 {
			serverToWrite := make([]gfs.ServerInfo, 0, len(args.ServerToWrite)-1)
			for _, server := range args.ServerToWrite {
				if server != chunkserver.server {
					serverToWrite = append(serverToWrite, server)
				}
			}
			reply := gfs.WriteDataAndForwardReply{}
			err := utils.RemoteCall(
				serverToWrite[0], "ChunkServer.WriteDataAndForwardRPC",
				gfs.WriteDataAndForwardArgs{
					ServerToWrite: serverToWrite,
					ChunkHandle:   args.ChunkHandle,
					Offset:        args.Offset,
					Data:          args.Data,
				},
				&reply,
			)
			if err != nil {
				gfs.Log(gfs.Error, err.Error())
				reply.Successful = false
				result <- reply
			} else {
				result <- reply
			}
		} else { // No other chunkservers to write
			result <- gfs.WriteDataAndForwardReply{Successful: true}
		}
	}()
	chunkserver.chunksLock.Lock()
	chunk, exist := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exist {
		go func() {
			_ = chunkserver.removeChunkInMaster(args.ChunkHandle)
		}()
		// Since the chunk is not stored on this chunkserver,
		// the consistency is still guaranteed.
		reply.Successful = true
	} else {
		chunk.Lock()
		err := chunk.rangedWrite(args.Offset, args.Data)
		chunk.Unlock()
		if err != nil {
			gfs.Log(gfs.Error, err.Error())
			_ = chunkserver.removeChunkAndMeta(args.ChunkHandle)
			_ = chunkserver.removeChunkInMaster(args.ChunkHandle)
		}
		reply.Successful = true
	}
	remoteReply := <-result
	reply.Successful = reply.Successful && remoteReply.Successful
	return nil
}
