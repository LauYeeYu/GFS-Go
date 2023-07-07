package master

import (
	"gfs"
	"sync"
	"time"
)

type NamespaceMetadata struct {
	Files       map[gfs.PathInfo]FileMetadata
	FilesLock   sync.RWMutex
	Directories map[string]gfs.PathInfo
	FirsLock    sync.RWMutex
	Locks       map[string]sync.RWMutex
	LocksLock   sync.RWMutex

	// access control
	// TODO: add access control
}

type FileMetadata struct {
	Chunks []gfs.ChunkHandle
	Lock   sync.RWMutex
}

type ChunkMetadata struct {
	Lock sync.RWMutex
	// Persistent data
	Version     gfs.ChunkVersion
	RefCount    int
	LeaseHolder *gfs.ServerInfo
	LeaseExpire time.Time

	// In-memory data
	Servers map[gfs.ServerInfo]bool // initialized with HeartBeat
}

func (namespace *NamespaceMetadata) lockFileOrDirectory(pathname string, readOnly bool) {
	namespace.LocksLock.RLock()
	defer namespace.LocksLock.RUnlock()
	if lock, ok := namespace.Locks[pathname]; ok {
		if readOnly {
			lock.RLock()
		} else {
			lock.Lock()
		}
	}
}

// LockAncestors Lock all ancestors of a path
// Return true if all ancestors are locked successfully
// Return false if any ancestor is locked by others
// The operation is done hierarchically from the root to the leaf
func (namespace *NamespaceMetadata) LockAncestors(pathInfo *gfs.PathInfo) bool {
	namespace.LocksLock.RLock()
	defer namespace.LocksLock.RUnlock()
	parent, err := pathInfo.Parent()
	if err != nil {
		return false
	}
	for index := range parent.Pathname {
		if index != 0 && parent.Pathname[index] == '/' {
			namespace.lockFileOrDirectory(parent.Pathname[:index], true)
		}
	}
	return true
}
