package gfs

import "time"

// Constants for chunks
const (
	ChunkSize        = 64 * 1024 * 1024 // 64MiB
	NumberOfReplicas = 3
)

// Constant for time
const (
	PeriodicWorkInterval = 60 * time.Second        // 60s
	HeartbeatInterval    = 200 * time.Millisecond  // 200ms
	HeartbeatTimeout     = 1000 * time.Millisecond // 1s
	LeaseTimeout         = 5 * time.Second         // 5s
)
