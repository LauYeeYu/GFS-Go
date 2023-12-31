package client

import (
	"gfs"
	"sync"
)

type Client struct {
	master       gfs.ServerInfo
	replicaCache map[gfs.ChunkHandle]*ReplicaInfo
	replicaLock  sync.Mutex
}

func MakeClient(master gfs.ServerInfo) *Client {
	return &Client{
		master:       master,
		replicaCache: make(map[gfs.ChunkHandle]*ReplicaInfo),
	}
}

type WorkingState struct {
	currentNamespace gfs.Namespace
	currentDirectory string
	client           *Client
}

func MakeWorkingState(client *Client) *WorkingState {
	return &WorkingState{
		currentNamespace: "",
		currentDirectory: "/",
		client:           client,
	}
}
