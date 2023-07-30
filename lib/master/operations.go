package master

import (
	"gfs"
	"time"
)

const (
	// For files
	AddFileOperation = iota
	DeleteFileOperation
	MoveFileOperation
	SnapshotOperation
	AddChunkToFile

	// For chunks
	AddChunkOperation

	// For leases
	GrantLeaseOperation
	RevokeLeaseOperation
)

type OperationLogEntryHeader struct {
	Time      time.Time
	Operation int
}

type LogEntry interface {
	Execute() error
}

type AddFileOperationLogEntry struct {
	Namespace gfs.Namespace
	Pathname  string
}

func (entry *AddFileOperationLogEntry) Execute() error {
	//TODO
	return nil
}

type DeleteFileOperationLogEntry struct {
	Namespace gfs.Namespace
	Pathname  string
}

func (entry *DeleteFileOperationLogEntry) Execute() error {
	//TODO
	return nil
}

type MoveFileOperationLogEntry struct {
	Namespace   gfs.Namespace
	Source      string
	Destination string
}

func (entry *MoveFileOperationLogEntry) Execute() error {
	//TODO
	return nil
}

type SnapshotOperationLogEntry struct {
	Namespace   gfs.Namespace
	Source      string
	Destination string
}

func (entry *SnapshotOperationLogEntry) Execute() error {
	//TODO
	return nil
}

type AddChunkToFileOperationLogEntry struct {
	Namespace gfs.Namespace
	Pathname  string
	Chunk     gfs.ChunkHandle
}

func (entry *AddChunkToFileOperationLogEntry) Execute() error {
	//TODO
	return nil
}

type AddChunkOperationLogEntry struct {
	Chunk gfs.ChunkHandle
}

func (entry *AddChunkOperationLogEntry) Execute() error {
	//TODO
	return nil
}

type GrantLeaseOperationLogEntry struct {
	Chunk       gfs.ChunkHandle
	LeaseHolder gfs.ServerInfo
	LeaseExpire time.Time
}

func (entry *GrantLeaseOperationLogEntry) Execute() error {
	//TODO
	return nil
}

type RevokeLeaseOperationLogEntry struct {
	Chunk gfs.ChunkHandle
}

func (entry *RevokeLeaseOperationLogEntry) Execute() error {
	//TODO
	return nil
}
