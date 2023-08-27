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
