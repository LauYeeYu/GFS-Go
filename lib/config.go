package gfs

import "time"

const CheckpointInterval = 100

// Constants for chunks
const (
	ChunkSize        = 64 * 1024 * 1024 // 64MiB
	NumberOfReplicas = 3
)

// Constants for time
const (
	PeriodicWorkInterval = 60 * time.Second        // 60s
	HeartbeatInterval    = 200 * time.Millisecond  // 200ms
	HeartbeatTimeout     = 1000 * time.Millisecond // 1s
	LeaseTimeout         = 60 * time.Second        // 5s
	LeaseExtendBefore    = 1 * time.Second         // 1s
)

// Constant strings for master
const (
	LogDirName           = "log"
	LogIndexName         = "last_log_index"
	CheckpointDirName    = "checkpoints"
	CheckpointIndexName  = "last_checkpoint_index"
	CheckpointSuffix     = ".checkpoint"
	CompressedLogDirName = "compressed_log"
)

// Constants for chunkserver
const (
	HeartbeatChunkNumber = 10
	ChunkDirName         = "chunks"
	LeaseDirName         = "leases"
	ChecksumSuffix       = ".checksum"
	VersionSuffix        = ".version"
)
