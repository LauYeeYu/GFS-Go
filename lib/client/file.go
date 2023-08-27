package client

import (
	"errors"
	"gfs"
	"gfs/utils"
)

// File is a struct that contains the metadata of a file.
// No data and the chunks will be stored in this structure.
type File struct {
	Namespace gfs.Namespace
	Filename  string
}

func (client *Client) getFileChunks(file *File) ([]gfs.ChunkHandle, error) {
	reply := gfs.GetFileChunksReply{}
	err := utils.RemoteCall(
		client.master, "GetFileChunksRPC",
		gfs.GetFileChunksArgs{
			Namespace: file.Namespace,
			Filename:  file.Filename,
		},
		&reply,
	)
	if err != nil {
		gfs.Log(gfs.Error, "GetFileChunksRPC error: %v", err)
		return nil, err
	}
	if reply.Valid {
		return reply.Chunks, nil
	} else {
		return nil, errors.New("invalid request")
	}
}

// offsetToIndex converts the offset to the index of the chunk and the offset
// in the chunk. Return the index and the offset in the chunk.
func offsetToIndex(offset gfs.Length) (int, gfs.Length) {
	return int(offset / gfs.ChunkSize), offset % gfs.ChunkSize
}

// getChunkHandleAt returns the chunk handle, the offset in the chunk and
// the error (if exists) at the given offset.
func (client *Client) getChunkHandleAt(
	file *File, offset gfs.Length,
) (gfs.ChunkHandle, gfs.Length, error) {
	chunks, err := client.getFileChunks(file)
	if err != nil {
		return -1, -1, err
	}
	index, offsetInChunk := offsetToIndex(offset)
	return chunks[index], offsetInChunk, nil
}