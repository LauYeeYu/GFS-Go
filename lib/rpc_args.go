package gfs

import "time"

// HeartBeats

type ChunkInfo struct {
	Version ChunkVersion
	Handle  ChunkHandle
	Length  Length
}

type HeartBeatArgs struct {
	ServerInfo      ServerInfo
	Chunks          []ChunkInfo
	CorruptedChunks []ChunkHandle
}

type HeartBeatReply struct {
	ExpiredChunks    []ChunkHandle
	RequireAllChunks bool
}

type GrantLeaseArgs struct {
	ServerInfo  ServerInfo
	ChunkHandle ChunkHandle
	LeaseExpire time.Time
}

type GrantLeaseReply struct {
	Accepted bool
}

type UpdateChunkArgs struct {
	ServerInfo      ServerInfo
	ChunkHandle     ChunkHandle
	OriginalVersion ChunkVersion
	NewVersion      ChunkVersion
}

type UpdateChunkReply struct {
	Accepted       bool
	CurrentVersion ChunkVersion // -1 if not exist
}

type ExtendLeaseArgs struct {
	ServerInfo  ServerInfo
	ChunkHandle ChunkHandle
}

type ExtendLeaseReply struct {
	Accepted  bool
	NewExpire time.Time
}

type RevokeLeaseArgs struct {
	ServerInfo  ServerInfo
	ChunkHandle ChunkHandle
}

type RevokeLeaseReply struct {
	Accepted bool
}

type WriteDataAndForwardArgs struct {
	ServersToWrite []ServerInfo
	ChunkHandle    ChunkHandle
	Offset         Length
	Data           []byte
}

type WriteDataAndForwardReply struct {
	Successful bool // true if succeed in writing on all servers in the list
}

type RemoveChunkMetaArgs struct {
	ServerInfo  ServerInfo
	ChunkHandle ChunkHandle
}

type RemoveChunkMetaReply struct{}

type NamespaceExistsArgs struct {
	Namespace Namespace
}

type NamespaceExistsReply struct {
	Exists bool
}

type CreateNamespaceArgs struct {
	Namespace Namespace
}

type CreateNamespaceReply struct {
	Success bool
	Message string
}

type GetChunkReplicasArgs struct {
	ChunkHandle ChunkHandle
	ReadOnly    bool // If true, the master will not grant lease to the chunk
}

type GetChunkReplicasReply struct {
	Valid             bool
	Orphan            bool
	Version           ChunkVersion
	Locations         []ServerInfo
	HasPrimary        bool
	Primary           ServerInfo
	PrimaryExpireTime time.Time
}

type GetFileChunksArgs struct {
	Namespace Namespace
	Filename  string
}

type GetFileChunksReply struct {
	Valid  bool
	Chunks []ChunkHandle
}

type GetChunkSizeArgs struct {
	ChunkHandle  ChunkHandle
	ChunkVersion ChunkVersion
}

type GetChunkSizeReply struct {
	Size         Length // -1 if not exist
	WrongVersion bool
}

type AddNewChunkArgs struct {
	ServerInfo  ServerInfo
	ChunkHandle ChunkHandle
}

type AddNewChunkReply struct {
	Successful bool
}

type WriteChunkArgs struct {
	ServerInfo     ServerInfo
	ChunkHandle    ChunkHandle
	Offset         Length
	Data           []byte
	ChunkVersion   ChunkVersion
	ServersToWrite []ServerInfo
}

type WriteChunkReply struct {
	Status int
}

type RecordAppendChunkArgs struct {
	ServerInfo     ServerInfo
	ChunkHandle    ChunkHandle
	Data           []byte
	ChunkVersion   ChunkVersion
	ServersToWrite []ServerInfo
}

type RecordAppendChunkReply struct {
	Status int
	Offset Length
}

type ReadChunkArgs struct {
	ServerInfo   ServerInfo
	ChunkHandle  ChunkHandle
	Offset       Length
	Length       Length
	ChunkVersion ChunkVersion
}

type ReadChunkReply struct {
	Status int
	Data   []byte
}

type CreateFileArgs struct {
	Namespace Namespace
	Filename  string
}

type CreateFileReply struct {
	Successful bool
	ErrorMsg   string
}

type AddNewChunkToFileArgs struct {
	Namespace Namespace
	Filename  string
}

type AddNewChunkToFileReply struct {
	Successful bool
	Chunk      ChunkHandle
	ErrorMsg   string
}
