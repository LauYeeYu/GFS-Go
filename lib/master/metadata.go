package master

import (
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"log"
	"sync"
	"time"
)

type NamespaceMetadata struct {
	Files       map[string]FileMetadata
	Directories map[string]DirectoryInfo
	Locks       map[string]sync.RWMutex
	sync.RWMutex

	// access control
	// TODO: add access control
}

type DirectoryInfo struct {
	Files       map[string]struct{} // full pathname
	Directories map[string]struct{} // full pathname
	sync.RWMutex
}

type FileMetadata struct {
	Chunks []gfs.ChunkHandle
	sync.RWMutex
}

type ChunkMetadata struct {
	sync.RWMutex
	// Persistent data
	Version     gfs.ChunkVersion
	RefCount    int
	LeaseHolder *gfs.ServerInfo
	LeaseExpire time.Time

	// In-memory data
	Servers map[gfs.ServerInfo]bool // initialized with HeartBeat
}

// alreadyExists returns true if the file or directory already exists
// Note: this method is not thread-safe, caller should hold the lock
func (namespace *NamespaceMetadata) alreadyExists(pathname string) bool {
	_, ok1 := namespace.Files[pathname]
	_, ok2 := namespace.Directories[pathname]
	return ok1 || ok2
}

// lockFileOrDirectory locks the file or directory
// This function will not lock the ancestors of the file or directory
// Note: this method is not thread-safe, caller should hold the lock of the namespace
func (namespace *NamespaceMetadata) lockFileOrDirectory(pathname string, readOnly bool) error {
	if lock, ok := namespace.Locks[pathname]; ok {
		if readOnly {
			lock.RLock()
		} else {
			lock.Lock()
		}
		return nil
	}
	return errors.New(fmt.Sprintf("file or directory %s does not exist", pathname))
}

// unlockFileOrDirectory unlocks the file or directory
// Note: this method is not thread-safe, caller should hold the lock of the namespace
func (namespace *NamespaceMetadata) unlockFileOrDirectory(pathname string, readOnly bool) error {
	if lock, ok := namespace.Locks[pathname]; ok {
		if readOnly {
			lock.RUnlock()
		} else {
			lock.Unlock()
		}
		return nil
	}
	return errors.New(fmt.Sprintf("file or directory %s does not exist", pathname))
}

// LockFile locks the file
// Note: this method is not thread-safe, caller should hold the lock of the namespace
func (namespace *NamespaceMetadata) LockFile(pathname string, readOnly bool) error {
	_, ok := namespace.Files[pathname]
	if !ok {
		return errors.New(fmt.Sprintf("file %s does not exist", pathname))
	}
	err := namespace.lockAncestors(&gfs.PathInfo{Pathname: pathname, IsDir: false})
	if err != nil {
		return err
	}
	err = namespace.lockFileOrDirectory(pathname, readOnly)
	if err != nil {
		namespace.unlockAncestors(&gfs.PathInfo{Pathname: pathname, IsDir: false})
		return err
	}
	return nil
}

// LockDirectory locks the directory
// Note: this method is not thread-safe, caller should hold the lock of the namespace
func (namespace *NamespaceMetadata) LockDirectory(pathname string, readOnly bool) error {
	_, ok := namespace.Directories[pathname]
	if !ok {
		return errors.New(fmt.Sprintf("directory %s does not exist", pathname))
	}
	err := namespace.lockAncestors(&gfs.PathInfo{Pathname: pathname, IsDir: true})
	if err != nil {
		return err
	}
	err = namespace.lockFileOrDirectory(pathname, readOnly)
	if err != nil {
		namespace.unlockAncestors(&gfs.PathInfo{Pathname: pathname, IsDir: true})
		return err
	}

	// lock all files and directory in the directory
	queue := utils.Queue[string]{}
	queue.Push(pathname)
	for !queue.Empty() {
		// lock files
		pathname := queue.Pop()
		directory, _ := namespace.Directories[pathname]
		filesInThisDirectory := utils.Keys(&directory.Files)
		utils.SortByDefault(filesInThisDirectory)
		for _, file := range filesInThisDirectory {
			_ = namespace.lockFileOrDirectory(file, readOnly)
		}

		// lock directories
		directoriesInThisDirectory := utils.Keys(&directory.Directories)
		utils.SortByDefault(directoriesInThisDirectory)
		for _, directory := range directoriesInThisDirectory {
			_ = namespace.lockFileOrDirectory(directory, readOnly)
			queue.Push(directory)
		}
	}
	return nil
}

// lockAncestors locks all ancestors of a path
// Return true if all ancestors are locked successfully
// Return false if any ancestor is locked by others
// The operation is done hierarchically from the root to the leaf
func (namespace *NamespaceMetadata) lockAncestors(pathInfo *gfs.PathInfo) error {
	parent, err := pathInfo.Parent()
	if err != nil {
		return err
	}
	for index := range parent.Pathname {
		if index != 0 && parent.Pathname[index] == '/' {
			err = namespace.lockFileOrDirectory(parent.Pathname[:index], true)
			if err != nil {
				namespace.unlockAncestors(&gfs.PathInfo{
					Pathname: parent.Pathname[:index], IsDir: true,
				})
				return err
			}
		}
	}
	return nil
}

func (namespace *NamespaceMetadata) unlockAncestors(pathInfo *gfs.PathInfo) {
	parent, err := pathInfo.Parent()
	if err != nil {
		return
	}
	for index := range parent.Pathname {
		if index != 0 && parent.Pathname[index] == '/' {
			err = namespace.unlockFileOrDirectory(parent.Pathname[:index], true)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (chunkMeta *ChunkMetadata) removeChunkserver(server gfs.ServerInfo) {
	chunkMeta.Lock()
	defer chunkMeta.Unlock()
	delete(chunkMeta.Servers, server)
}

func (chunkMeta *ChunkMetadata) addChunkserver(server gfs.ServerInfo) {
	chunkMeta.Lock()
	defer chunkMeta.Unlock()
	chunkMeta.Servers[server] = true
}
