package client

import (
	"gfs"
	"time"
)

type ReplicaInfo struct {
	ChunkHandle       gfs.ChunkHandle
	Locations         []gfs.ServerInfo
	Primary           *gfs.ServerInfo
	PrimaryExpireTime time.Time
}
