package client

import (
	"gfs"
	"sync"
)

// FileDescriptor is a struct that contains the metadata of a file.
// No data will be stored in this structure.
type FileDescriptor struct {
	Namespace   gfs.Namespace
	Pathname    string
	Handles     []gfs.ChunkHandle
	HandlesLock sync.RWMutex
}
