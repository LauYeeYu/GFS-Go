package master

import (
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"sync"
	"time"
)

type NamespaceMetadata struct {
	Root *DirectoryInfo
	sync.RWMutex
}

func MakeNamespace() *NamespaceMetadata {
	return &NamespaceMetadata{
		Root: &DirectoryInfo{
			Parent:      nil,
			Files:       map[string]*FileMetadata{},
			Directories: map[string]*DirectoryInfo{},
		},
	}
}

type DirectoryInfo struct {
	Parent      *DirectoryInfo
	Files       map[string]*FileMetadata  // relative path -> file metadata
	Directories map[string]*DirectoryInfo // relative path -> directory metadata
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
	RefCount    int64
	LeaseHolder *gfs.ServerInfo
	LeaseExpire time.Time

	// In-memory data
	Servers utils.Set[gfs.ServerInfo] // initialized with HeartBeat
}

// exists returns true if the file or directory already exists
func (namespace *NamespaceMetadata) exists(pathname string) bool {
	err := namespace.LockFileOrDirectory(pathname, true)
	if err != nil {
		return false
	}
	_ = namespace.UnlockFileOrDirectory(pathname, true)
	return true
}

// getDirectory returns the directory metadata of the directory
// Note: this method is not concurrency-safe, the caller should hold the read
// lock of all its parent directories.
func (namespace *NamespaceMetadata) getDirectory(pathname string) (*DirectoryInfo, error) {
	segments := utils.ParsePath(pathname)
	current := namespace.Root
	for _, segment := range segments {
		if subdirectory, ok := current.Directories[segment]; ok {
			current = subdirectory
		} else {
			return nil, errors.New(fmt.Sprintf("directory %s does not exist", pathname))
		}
	}
	return current, nil
}

// lockAndGetDirectory locks and returns the directory metadata of the directory
func (namespace *NamespaceMetadata) lockAndGetDirectory(
	pathname string,
	readOnly bool,
) (*DirectoryInfo, error) {
	err := namespace.LockFileOrDirectory(pathname, readOnly)
	if err != nil {
		return nil, err
	}
	parentDir, err := namespace.getDirectory(pathname)
	if err != nil {
		_ = namespace.UnlockFileOrDirectory(pathname, readOnly)
		return nil, err
	}
	return parentDir, nil
}

// getFile returns the file metadata of the file
// Note: this method is not concurrency-safe, the caller should hold the read
// lock of all its parent directories.
func (namespace *NamespaceMetadata) getFile(pathname string) (*FileMetadata, error) {
	parent, err := utils.Parent(pathname)
	if err != nil {
		return nil, err
	}
	dir, err := namespace.getDirectory(parent)
	if err != nil {
		return nil, err
	}
	if file, ok := dir.Files[utils.LastSegment(pathname)]; ok {
		return file, nil
	} else {
		return nil, errors.New(fmt.Sprintf("file %s does not exist", pathname))
	}
}

// lockAndGetFile locks and returns the file metadata of the file
func (namespace *NamespaceMetadata) lockAndGetFile(
	pathname string,
	readOnly bool,
) (*FileMetadata, error) {
	err := namespace.LockFileOrDirectory(pathname, readOnly)
	if err != nil {
		return nil, err
	}
	file, err := namespace.getFile(pathname)
	if err != nil {
		_ = namespace.UnlockFileOrDirectory(pathname, readOnly)
		return nil, err
	}
	return file, nil
}

func (dir *DirectoryInfo) lockFileOrDirectory(segments []string, readOnly bool) error {
	if len(segments) == 0 {
		if readOnly {
			dir.RLock()
		} else {
			dir.Lock()
		}
		return nil
	}
	dir.RLock()
	if subdirectory, ok := dir.Directories[segments[0]]; ok {
		err := subdirectory.lockFileOrDirectory(segments[1:], readOnly)
		if err != nil {
			dir.RUnlock()
		}
		return err
	} else if file, ok := dir.Files[segments[0]]; ok {
		if len(segments) == 1 { // the last segment
			if readOnly {
				file.RLock()
			} else {
				file.Lock()
			}
			return nil
		} else { // not the last segment
			return errors.New(fmt.Sprintf("file %s is not a directory", segments[0]))
		}
	} else { // the file or directory does not exist
		return errors.New(fmt.Sprintf("file or directory %s does not exist", segments[0]))
	}
}

func (dir *DirectoryInfo) lockWithLockTask(task *LockTask) error {
	if task == nil {
		return nil
	}

	// lock itself
	if task.ReadOnly {
		dir.RLock()
	} else {
		dir.Lock()
	}

	// traverse through the subtasks
	for i, subtask := range task.Subtasks {
		cleanup := func() {
			unlockTask := &LockTask{
				Name:     task.Name,
				ReadOnly: task.ReadOnly,
				Subtasks: task.Subtasks[:i],
			}
			_ = dir.unlockWithLockTask(unlockTask)
		}
		if CanBeDirectory(subtask.Requirement) {
			if subdirectory, ok := dir.Directories[subtask.Name]; ok {
				err := subdirectory.lockWithLockTask(subtask)
				if err != nil {
					cleanup()
					return err
				}
			}
		}
		if CanBeFile(subtask.Requirement) {
			if file, ok := dir.Files[subtask.Name]; ok {
				if len(subtask.Subtasks) == 0 { // the last segment
					if subtask.ReadOnly {
						file.RLock()
					} else {
						file.Lock()
					}
				} else { // not the last segment
					cleanup()
					return errors.New(fmt.Sprintf("file %s is not a directory", subtask.Name))
				}
			}
		}
		// the file or directory does not exist
		cleanup()
		return errors.New(fmt.Sprintf("file or directory %s does not exist", subtask.Name))
	}
	return nil
}

// unlockWithLockTask unlocks the file or directory
// The function returns the last error that occurs during the unlocking process
// if exists.
func (dir *DirectoryInfo) unlockWithLockTask(task *LockTask) error {
	if task == nil {
		return nil
	}

	// unlock itself
	if task.ReadOnly {
		dir.RUnlock()
	} else {
		dir.Unlock()
	}

	// traverse through the subtasks
	var err error = nil
	for _, subtask := range task.Subtasks {
		if subdirectory, ok := dir.Directories[subtask.Name]; ok {
			newError := subdirectory.unlockWithLockTask(subtask)
			if newError != nil {
				err = newError
			}
		} else if file, ok := dir.Files[subtask.Name]; ok {
			if len(subtask.Subtasks) == 0 { // the last segment
				if subtask.ReadOnly {
					file.RUnlock()
				} else {
					file.Unlock()
				}
			} else { // not the last segment
				err = errors.New(fmt.Sprintf("file %s is not a directory", subtask.Name))
			}
		} else { // the file or directory does not exist
			err = errors.New(fmt.Sprintf("file or directory %s does not exist", subtask.Name))
		}

	}
	return err
}

func (dir *DirectoryInfo) unlockFileOrDirectory(segments []string, readOnly bool) error {
	if len(segments) == 0 {
		if readOnly {
			dir.RUnlock()
		} else {
			dir.Unlock()
		}
		return nil
	}
	dir.RUnlock()
	if subdirectory, ok := dir.Directories[segments[0]]; ok {
		return subdirectory.unlockFileOrDirectory(segments[1:], readOnly)
	} else if file, ok := dir.Files[segments[0]]; ok {
		if len(segments) == 1 { // the last segment
			if readOnly {
				file.RUnlock()
			} else {
				file.Unlock()
			}
			return nil
		} else { // not the last segment
			return errors.New(fmt.Sprintf("file %s is not a directory", segments[0]))
		}
	} else { // the file or directory does not exist
		return errors.New(fmt.Sprintf("file or directory %s does not exist", segments[0]))
	}
}

// LockFileOrDirectory locks the file or directory and all its parent directories
func (namespace *NamespaceMetadata) LockFileOrDirectory(pathname string, readOnly bool) error {
	return namespace.Root.lockFileOrDirectory(utils.ParsePath(pathname), readOnly)
}

// UnlockFileOrDirectory unlocks the file or directory and all its parent directories
func (namespace *NamespaceMetadata) UnlockFileOrDirectory(pathname string, readOnly bool) error {
	return namespace.Root.unlockFileOrDirectory(utils.ParsePath(pathname), readOnly)
}

func (dir *DirectoryInfo) addDirectoryIfNotExists(segment []string) {
	dir.Lock()
	subDir, ok := dir.Directories[segment[0]]
	if ok {
		subDir.addDirectoryIfNotExists(segment[1:])
	} else {
		dir.Directories[segment[0]] = &DirectoryInfo{
			Parent:      dir,
			Files:       map[string]*FileMetadata{},
			Directories: map[string]*DirectoryInfo{},
		}
		dir.Directories[segment[0]].addDirectoryIfNotExists(segment[1:])
	}
	dir.Unlock()
}

// addDirectoriesIfNotExists adds all parent directories of the file or directory
func (namespace *NamespaceMetadata) addDirectoriesIfNotExists(pathname string) {
	namespace.Root.addDirectoryIfNotExists(utils.ParsePath(pathname))
}

func (dir *DirectoryInfo) makeDirectoryIfNotExists(directories []string) error {
	if len(directories) == 0 {
		return nil
	}
	dir.Lock()
	if subDir, ok := dir.Directories[directories[0]]; ok {
		err := subDir.makeDirectoryIfNotExists(directories[1:])
		dir.Unlock()
		return err
	} else if _, ok := dir.Files[directories[0]]; ok {
		dir.Unlock()
		return errors.New(fmt.Sprintf("%s is a file", directories[0]))
	} else {
		dir.Directories[directories[0]] = &DirectoryInfo{
			Parent:      dir,
			Files:       map[string]*FileMetadata{},
			Directories: map[string]*DirectoryInfo{},
		}
		err := dir.Directories[directories[0]].makeDirectoryIfNotExists(directories[1:])
		if err != nil {
			delete(dir.Directories, directories[0])
			dir.Unlock()
			return nil
		}
		dir.Unlock()
		return nil
	}
}

func (dir *DirectoryInfo) addFile(directories []string, filename string) error {
	dir.Lock()
	defer dir.Unlock()
	if len(directories) == 0 { // the final directory
		if _, ok := dir.Files[filename]; ok {
			return errors.New(fmt.Sprintf("file %s already exists", filename))
		}
		if _, ok := dir.Directories[filename]; ok {
			return errors.New(fmt.Sprintf("%s is a directory", filename))
		}
		dir.Files[filename] = &FileMetadata{Chunks: []gfs.ChunkHandle{}}
		return nil
	}
	if _, ok := dir.Files[directories[0]]; ok {
		return errors.New(fmt.Sprintf("%s is a file", directories[0]))
	}
	if _, ok := dir.Directories[directories[0]]; !ok {
		dir.Directories[directories[0]] = &DirectoryInfo{
			Parent:      dir,
			Files:       map[string]*FileMetadata{},
			Directories: map[string]*DirectoryInfo{},
		}
	}
	err := dir.Directories[directories[0]].addFile(directories[1:], filename)
	return err
}

// addFile adds a file to the namespace
// The function will create all parent directories if they do not exist.
func (namespace *NamespaceMetadata) addFile(pathname string) error {
	segment := utils.ParsePath(pathname)
	return namespace.Root.addFile(segment[:len(segment)-1], segment[len(segment)-1])
}

// deleteFile deletes a file from the namespace
// The file should have no chunks. A suggested way is to remove all chunks
// first, and then delete the file.
func (namespace *NamespaceMetadata) deleteFile(pathname string) error {
	parent, err := utils.Parent(pathname)
	if err != nil {
		return err
	}
	parentDir, err := namespace.lockAndGetDirectory(parent, false)
	if err != nil {
		return err
	}
	if file, ok := parentDir.Files[utils.LastSegment(pathname)]; ok {
		file.Lock()
		if len(file.Chunks) != 0 {
			_ = namespace.UnlockFileOrDirectory(parent, false)
			file.Unlock()
			return errors.New(fmt.Sprintf("file %s still has chunks", pathname))
		}
		file.Unlock()
		delete(parentDir.Files, utils.LastSegment(pathname))
		_ = namespace.UnlockFileOrDirectory(parent, false)
		return nil
	} else {
		_ = namespace.UnlockFileOrDirectory(parent, false)
		return errors.New(fmt.Sprintf("file %s does not exist", pathname))
	}
}

// moveFile moves a file from oldPathname to newPathname
func (namespace *NamespaceMetadata) moveFile(oldPathname, newPathname string) error {
	newParent, err := utils.Parent(newPathname)
	if err != nil {
		return err
	}
	oldParent, err := utils.Parent(oldPathname)
	if err != nil {
		return err
	}
	newSegment := utils.ParsePath(newParent)
	oldSegment := utils.ParsePath(oldParent)
	newLockTask := MakeLockTaskFromStringSlice(newSegment, false, LockDirectory)
	oldLockTask := MakeLockTaskFromStringSlice(oldSegment, false, LockDirectory)
	task, err := MergeLockTasks(newLockTask, oldLockTask)
	if err != nil {
		return err
	}
	err = namespace.Root.lockWithLockTask(task)
	if err != nil {
		return err
	}
	newParentDir, err := namespace.getDirectory(newParent)
	if err != nil {
		return err
	}
	oldParentDir, err := namespace.getDirectory(oldParent)
	if err != nil {
		return err
	}
	newFileName := utils.LastSegment(newPathname)
	oldFileName := utils.LastSegment(oldPathname)
	if _, ok := newParentDir.Files[newFileName]; ok {
		_ = namespace.Root.unlockWithLockTask(task)
		return errors.New(fmt.Sprintf("file %s already exists", newPathname))
	}
	if _, ok := newParentDir.Directories[newFileName]; ok {
		_ = namespace.Root.unlockWithLockTask(task)
		return errors.New(fmt.Sprintf("%s is a directory", newPathname))
	}
	if _, ok := oldParentDir.Files[oldFileName]; !ok {
		_ = namespace.Root.unlockWithLockTask(task)
		return errors.New(fmt.Sprintf("file %s does not exist", oldPathname))
	}
	newParentDir.Files[newFileName] = oldParentDir.Files[oldFileName]
	delete(oldParentDir.Files, oldFileName)
	_ = namespace.Root.unlockWithLockTask(task)
	return nil
}

// snapshot copies the content of sourceDir to dir
// The destination directory should be empty.
// Note: this function does not lock the destination directory.
func (dir *DirectoryInfo) snapshot(master *Master, sourceDir *DirectoryInfo) error {
	if len(dir.Files) != 0 || len(dir.Directories) != 0 {
		return errors.New("destination directory is not empty")
	}
	for filename, file := range sourceDir.Files {
		dir.Files[filename] = master.makeFileCopy(file)
	}
	for dirname, subDir := range sourceDir.Directories {
		dir.Directories[dirname] = &DirectoryInfo{
			Parent:      dir,
			Files:       map[string]*FileMetadata{},
			Directories: map[string]*DirectoryInfo{},
		}
		_ = dir.Directories[dirname].snapshot(master, subDir)
	}
	return nil
}

func (master *Master) makeFileCopy(file *FileMetadata) *FileMetadata {
	master.chunksLock.Lock()
	for _, chunk := range file.Chunks {
		master.chunks[chunk].RefCount++
	}
	master.chunksLock.Unlock()
	return &FileMetadata{
		Chunks: file.Chunks,
	}
}

func (chunkMeta *ChunkMetadata) removeChunkserver(server gfs.ServerInfo) {
	chunkMeta.Lock()
	defer chunkMeta.Unlock()
	chunkMeta.Servers.Remove(server)
}

func (chunkMeta *ChunkMetadata) addChunkserver(server gfs.ServerInfo) {
	chunkMeta.Lock()
	defer chunkMeta.Unlock()
	chunkMeta.Servers.Add(server)
}

func (master *Master) reduceChunkRef(chunk gfs.ChunkHandle) error {
	master.chunksLock.Lock()
	defer master.chunksLock.Unlock()
	chunkMeta, ok := master.chunks[chunk]
	if !ok {
		return errors.New(fmt.Sprintf("chunk %d does not exist", chunk))
	}
	chunkMeta.Lock()
	defer chunkMeta.Unlock()
	chunkMeta.RefCount--
	if chunkMeta.RefCount == 0 {
		delete(master.chunks, chunk)
	}
	return nil
}
