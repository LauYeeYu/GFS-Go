package master

import (
	"encoding/gob"
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"os"
	"time"
)

const (
	Invalid = iota
	// For namespaces
	CreateNamespaceOperation

	// For files
	AddFileOperation
	DeleteFileOperation
	MoveFileOperation
	SnapshotOperation
	AddChunkToFile

	// For leases
	GrantLeaseOperation
	RevokeLeaseOperation
)

type OperationLogEntryHeader struct {
	Time      time.Time
	Operation int
}

func NewOperationLogEntryHeader(operation int) *OperationLogEntryHeader {
	return &OperationLogEntryHeader{
		Time:      time.Now(),
		Operation: operation,
	}
}

type LogEntry interface {
	// Execute makes the changes to the master the metadata.
	// Note: Execute should not send any RPCs because it can also be
	// call when recovering.
	Execute(master *Master) error
}

func (master *Master) appendLog(log OperationLogEntryHeader, entry LogEntry) error {
	// Write the log to the log file
	master.operationLogLock.Lock()
	logIndex := master.nextLogIndex
	master.nextLogIndex++
	err := master.writeLog(logIndex, log, entry)
	if err != nil {
		master.nextLogIndex--
		master.operationLogLock.Unlock()
		return err
	}
	master.operationLogLock.Unlock()

	// Add a new checkpoint if necessary
	go func() {
		master.checkpointLock.Lock()
		lastCheckpoint := master.lastCheckpoint
		master.checkpointLock.Unlock()
		if logIndex-lastCheckpoint >= gfs.CheckpointInterval {
			_ = master.addNewCheckpoint(logIndex)
		}
	}()

	// execute the log
	return entry.Execute(master)
}

func (master *Master) writeLog(logIndex int64, log OperationLogEntryHeader, entry LogEntry) error {
	fileName := utils.MergePath(master.logDir, fmt.Sprintf("%d.log", logIndex))
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if file.Truncate(0) != nil {
		return err
	}
	if _, err = file.Seek(0, 0); err != nil {
		_ = file.Close()
		return err
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(log)
	if err != nil {
		_ = file.Close()
		return err
	}
	err = encoder.Encode(entry)
	if err != nil {
		_ = file.Close()
		return err
	}
	_ = file.Close()
	// Update the index file
	return utils.WriteTextInt64ToFile(utils.MergePath(master.logDir, "index"), logIndex)
}

// replayLog replays the log at logIndex
// Return err if the log file cannot be retrieved. Execution errors will not
// be regarded as errors.
// Note: When calling this function, You can do nothing else on master
func (master *Master) replayLog(logIndex int64) error {
	fileName := utils.MergePath(master.logDir, fmt.Sprintf("%d.log", logIndex))
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	if _, err = file.Seek(0, 0); err != nil {
		_ = file.Close()
		return err
	}
	decoder := gob.NewDecoder(file)
	var header OperationLogEntryHeader
	err = decoder.Decode(&header)
	if err != nil {
		return err
	}
	switch header.Operation {
	case Invalid:
		return errors.New("invalid operation")
	case CreateNamespaceOperation:
		var entry CreateNamespaceOperationLogEntry
		if err = decoder.Decode(&entry); err != nil {
			return err
		}
		_ = entry.Execute(master)
	case AddFileOperation:
		var entry AddFileOperationLogEntry
		if err = decoder.Decode(&entry); err != nil {
			return err
		}
		_ = entry.Execute(master)
	case DeleteFileOperation:
		var entry DeleteFileOperationLogEntry
		if err = decoder.Decode(&entry); err != nil {
			return err
		}
		_ = entry.Execute(master)
	case MoveFileOperation:
		var entry MoveFileOperationLogEntry
		if err = decoder.Decode(&entry); err != nil {
			return err
		}
		_ = entry.Execute(master)
	case SnapshotOperation:
		var entry SnapshotOperationLogEntry
		if err = decoder.Decode(&entry); err != nil {
			return err
		}
		_ = entry.Execute(master)
	case AddChunkToFile:
		var entry AddChunkToFileOperationLogEntry
		if err = decoder.Decode(&entry); err != nil {
			return err
		}
		_ = entry.Execute(master)
	case GrantLeaseOperation:
		var entry GrantLeaseOperationLogEntry
		if err = decoder.Decode(&entry); err != nil {
			return err
		}
		_ = entry.Execute(master)
	case RevokeLeaseOperation:
		var entry RevokeLeaseOperationLogEntry
		if err = decoder.Decode(&entry); err != nil {
			return err
		}
		_ = entry.Execute(master)
	default:
		return errors.New("log type not found")
	}
	return nil
}

type CreateNamespaceOperationLogEntry struct {
	Namespace gfs.Namespace
}

func (entry *CreateNamespaceOperationLogEntry) Execute(master *Master) error {
	master.namespacesLock.Lock()
	defer master.namespacesLock.Unlock()
	if _, exists := master.namespaces[entry.Namespace]; exists {
		return errors.New("namespace already exists")
	}
	master.namespaces[entry.Namespace] = MakeNamespace()
	return nil
}

type AddFileOperationLogEntry struct {
	Namespace gfs.Namespace
	Pathname  string
}

func (entry *AddFileOperationLogEntry) Execute(master *Master) error {
	master.namespacesLock.RLock()
	namespaceMeta, exists := master.namespaces[entry.Namespace]
	master.namespacesLock.RUnlock()
	if !exists {
		return errors.New("namespace not found")
	}
	return namespaceMeta.addFile(entry.Pathname)
}

type DeleteFileOperationLogEntry struct {
	Namespace gfs.Namespace
	Pathname  string
}

func (entry *DeleteFileOperationLogEntry) Execute(master *Master) error {
	master.namespacesLock.RLock()
	namespaceMeta, exists := master.namespaces[entry.Namespace]
	master.namespacesLock.RUnlock()
	if !exists {
		return errors.New("namespace not found")
	}
	parent, err := utils.Parent(entry.Pathname)
	if err != nil {
		return err
	}
	dir, err := namespaceMeta.lockAndGetDirectory(parent, false)
	if err != nil {
		return err
	}
	fileMeta, exists := dir.Files[entry.Pathname]
	if !exists {
		_ = namespaceMeta.UnlockFileOrDirectory(parent, false)
		return errors.New("file not found")
	}
	fileMeta.Lock()
	for _, chunk := range fileMeta.Chunks {
		_ = master.reduceChunkRef(chunk)
	}
	fileMeta.Chunks = []gfs.ChunkHandle{}
	fileMeta.Unlock()
	delete(dir.Files, entry.Pathname)
	return nil
}

type MoveFileOperationLogEntry struct {
	Namespace   gfs.Namespace
	Source      string
	Destination string
}

func (entry *MoveFileOperationLogEntry) Execute(master *Master) error {
	master.namespacesLock.RLock()
	namespaceMeta, exists := master.namespaces[entry.Namespace]
	master.namespacesLock.RUnlock()
	if !exists {
		return errors.New("namespace not found")
	}
	return namespaceMeta.moveFile(entry.Source, entry.Destination)
}

type SnapshotOperationLogEntry struct {
	Namespace   gfs.Namespace
	Source      string
	Destination string
}

func (entry *SnapshotOperationLogEntry) Execute(master *Master) error {
	master.namespacesLock.RLock()
	namespaceMeta, exists := master.namespaces[entry.Namespace]
	master.namespacesLock.RUnlock()
	if !exists {
		return errors.New("namespace not found")
	}
	err := namespaceMeta.Root.makeDirectoryIfNotExists(utils.ParsePath(entry.Destination))
	if err != nil {
		return err
	}
	newLockPlan := MakeLockTaskFromStringSlice(utils.ParsePath(entry.Destination), false)
	oldLockPlan := MakeLockTaskFromStringSlice(utils.ParsePath(entry.Source), true)
	if newLockPlan == nil || oldLockPlan == nil {
		return errors.New("invalid snapshot path")
	}
	lockPlan, err := MergeLockTasks(newLockPlan, oldLockPlan)
	if err != nil {
		return err
	}
	err = namespaceMeta.Root.lockWithLockTask(lockPlan)
	if err != nil {
		return err
	}

	// snapshot
	destDir, err := namespaceMeta.getDirectory(entry.Destination)
	if err != nil {
		return err
	}
	srcDir, err := namespaceMeta.getDirectory(entry.Source)
	if err != nil {
		return err
	}
	return destDir.snapshot(master, srcDir)
}

type AddChunkToFileOperationLogEntry struct {
	Namespace gfs.Namespace
	Pathname  string
	Chunk     gfs.ChunkHandle
}

func (entry *AddChunkToFileOperationLogEntry) Execute(master *Master) error {
	master.namespacesLock.RLock()
	namespaceMeta, exists := master.namespaces[entry.Namespace]
	master.namespacesLock.RUnlock()
	if !exists {
		return errors.New("namespace not found")
	}
	fileMeta, err := namespaceMeta.lockAndGetFile(entry.Pathname, false)
	if err != nil {
		return err
	}
	fileMeta.Chunks = append(fileMeta.Chunks, entry.Chunk)
	master.chunksLock.Lock()
	chunk, ok := master.chunks[entry.Chunk]
	if !ok {
		// FIXME: servers should be the servers that have the chunk
		var servers []gfs.ServerInfo
		serverMap := make(map[gfs.ServerInfo]bool)
		for _, server := range servers {
			serverMap[server] = true
		}
		master.chunksLock.Lock()
		master.chunks[entry.Chunk] = &ChunkMetadata{
			Version:     0,
			RefCount:    1,
			LeaseHolder: nil,
			LeaseExpire: time.Now(),
			Servers:     serverMap,
		}
	} else {
		chunk.RefCount++
	}
	return nil
}

type GrantLeaseOperationLogEntry struct {
	Chunk       gfs.ChunkHandle
	LeaseHolder gfs.ServerInfo
	LeaseExpire time.Time
}

func (entry *GrantLeaseOperationLogEntry) Execute(master *Master) error {
	//TODO
	return nil
}

type RevokeLeaseOperationLogEntry struct {
	Chunk gfs.ChunkHandle
}

func (entry *RevokeLeaseOperationLogEntry) Execute(master *Master) error {
	//TODO
	return nil
}
