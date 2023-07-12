package master

import (
	"errors"
	"fmt"
	"gfs"
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
// Note: this method is not concurrency-safe.
func (namespace *NamespaceMetadata) alreadyExists(pathname string) bool {
	_, ok1 := namespace.Files[pathname]
	_, ok2 := namespace.Directories[pathname]
	return ok1 || ok2
}

// lockFileOrDirectory locks the file or directory
// This function will not lock the ancestors of the file or directory
// Note: this method is not concurrency-safe.
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
// Note: this method is not concurrency-safe.
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
// Note: this method is not concurrency-safe.
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
// Note: this method is not concurrency-safe.
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
	return nil
}

// lockAncestors locks all ancestors of a path
// Return true if all ancestors are locked successfully.
// Return false if any ancestor is locked by others.
// The operation is done hierarchically from the root to the leaf.
// Note: this method is not concurrency-safe.
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

// unlockAncestors unlocks all ancestors of a path
// The operation is done hierarchically from the root to the leaf.
// Note: this method is not concurrency-safe.
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

func (namespace *NamespaceMetadata) addDirectoryIfNotExists(pathname string) {
	if _, ok := namespace.Directories[pathname]; !ok {
		namespace.Directories[pathname] = DirectoryInfo{
			Files:       map[string]struct{}{},
			Directories: map[string]struct{}{},
		}
		namespace.Locks[pathname] = sync.RWMutex{}
	}
}

func (namespace *NamespaceMetadata) addAllParentDirectoriesIfNotExists(pathname string) {
	parent, err := (&gfs.PathInfo{Pathname: pathname, IsDir: false}).Parent()
	if err != nil {
		return // the case that the file is in the root directory
	}
	for index := range parent.Pathname {
		if index != 0 && parent.Pathname[index] == '/' {
			namespace.addDirectoryIfNotExists(parent.Pathname[:index])
		}
	}
}

// addFile adds a file to the namespace
// The parent directories must exist. A suggested way is to lock the namespace
// first, add all parent directories, and lock these directories before adding
// the file.
// Note: this method is not concurrency-safe, the caller should hold the write
// lock of the namespace.
func (namespace *NamespaceMetadata) addFile(pathname string) error {
	if _, ok := namespace.Files[pathname]; ok {
		return errors.New(fmt.Sprintf("file %s already exists", pathname))
	}
	namespace.Files[pathname] = FileMetadata{Chunks: []gfs.ChunkHandle{}}
	namespace.Locks[pathname] = sync.RWMutex{}
	return nil
}

// tryRemoveParentsWhenRemoved tries to remove all empty parent directories
// when a file or directory is removed. The operation is done recursively.
// Note: this method is not concurrency-safe, the caller should hold the write
// lock of the namespace.
func (namespace *NamespaceMetadata) tryRemoveParentsWhenRemoved(pathname string) {
	parent, err := (&gfs.PathInfo{Pathname: pathname, IsDir: false}).Parent()
	if err != nil {
		return // the case in the root directory
	}
	if len(namespace.Directories[parent.Pathname].Files) == 0 &&
		len(namespace.Directories[parent.Pathname].Directories) == 0 {
		delete(namespace.Directories, parent.Pathname)
		delete(namespace.Locks, parent.Pathname)
	}
	namespace.tryRemoveParentsWhenRemoved(parent.Pathname)
}

// deleteFile deletes a file from the namespace
// The file should have no chunks. A suggested way is to remove all chunks
// first, and then delete the file.
// Note: this method is not concurrency-safe, the caller should hold the write
// lock of the namespace.
func (namespace *NamespaceMetadata) deleteFile(pathname string) error {
	if _, ok := namespace.Files[pathname]; !ok {
		return errors.New(fmt.Sprintf("file %s does not exist", pathname))
	}
	if len(namespace.Files[pathname].Chunks) != 0 {
		return errors.New(fmt.Sprintf("file %s still has chunks", pathname))
	}
	delete(namespace.Files, pathname)
	delete(namespace.Locks, pathname)
	namespace.tryRemoveParentsWhenRemoved(pathname)
	return nil
}

// moveFile moves a file from oldPathname to newPathname
// Note: this method is not concurrency-safe, the caller should hold the write
// lock of the namespace.
func (namespace *NamespaceMetadata) moveFile(oldPathname, newPathname string) error {
	if _, ok := namespace.Files[oldPathname]; !ok {
		return errors.New(fmt.Sprintf("source file %s does not exist", oldPathname))
	}
	if _, ok := namespace.Files[newPathname]; ok {
		return errors.New(fmt.Sprintf("target file %s already exists", newPathname))
	}
	chunks := namespace.Files[oldPathname].Chunks
	delete(namespace.Files, oldPathname)
	delete(namespace.Locks, oldPathname)
	namespace.Files[newPathname] = FileMetadata{Chunks: chunks}
	namespace.Locks[newPathname] = sync.RWMutex{}
	namespace.tryRemoveParentsWhenRemoved(oldPathname)
	return nil
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
