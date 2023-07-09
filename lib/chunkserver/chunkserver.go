package chunkserver

import (
	"gfs"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Chunkserver struct {
	server     gfs.ServerInfo
	master     gfs.ServerInfo
	storageDir string
	chunksDir  string
	leasesDir  string

	chunks     map[gfs.ChunkHandle]*Chunk
	chunksLock sync.RWMutex
}

// MakeChunkserver creates a new Chunkserver instance
func MakeChunkserver(
	chunkserverInfo gfs.ServerInfo,
	masterInfo gfs.ServerInfo,
	storageDir string,
) *Chunkserver {
	chunkserver := Chunkserver{
		server:     chunkserverInfo,
		master:     masterInfo,
		storageDir: storageDir,
		chunksDir:  storageDir + "/chunks",
		leasesDir:  storageDir + "/leases",
		chunks:     make(map[gfs.ChunkHandle]*Chunk),
	}
	if err := chunkserver.LoadChunks(); err != nil {
		log.Println(err.Error())
		return nil
	}
	return &chunkserver
}

func (chunkserver *Chunkserver) LoadChunks() error {
	files, err := os.ReadDir(chunkserver.chunksDir)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasSuffix(file.Name(), ".checksum") &&
			!strings.HasSuffix(file.Name(), ".version") {
			// The file is a chunk
			handleInt, err := strconv.ParseInt(file.Name(), 10, 64)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			handle := gfs.ChunkHandle(handleInt)
			chunkMeta := LoadChunkMetadata(handle, chunkserver)
			if chunkMeta == nil {
				continue // Chunk is corrupted
			}
			chunkserver.chunks[handle] = chunkMeta
		}
	}
	return nil
}
