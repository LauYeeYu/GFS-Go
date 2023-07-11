package chunkserver

import (
	"gfs"
	"log"
	"os"
	"strconv"
	"sync"
)

type Chunk struct {
	version   gfs.ChunkVersion // stored in $storageDir/chunks/$handle.version
	handle    gfs.ChunkHandle
	length    gfs.Length
	chunkFile *os.File // stored in $storageDir/chunks/$handle
	checksum  Checksum // stored in $storageDir/chunks/$handle.checksum
	sync.RWMutex
}

func (chunk *Chunk) Read() ([]byte, error) {
	chunk.RLock()
	defer chunk.RUnlock()
	data := make([]byte, chunk.length)
	_, err := chunk.chunkFile.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func LoadChunkMetadata(
	handle gfs.ChunkHandle,
	chunkserver *Chunkserver,
) *Chunk {
	// Get the version
	versionFile, err := os.Open(chunkserver.chunksDir + "/" + handle.String() + ".version")
	if err != nil {
		log.Println("Fail to open version file:", err.Error())
		return nil
	}
	version, err := strconv.ParseInt(versionFile.Name(), 10, 32)
	if err != nil {
		log.Println("Fail to parse version:", err.Error())
		return nil
	}

	// Get the checksum
	checksumFile, err := os.Open(chunkserver.chunksDir + "/" + handle.String() + ".checksum")
	if err != nil {
		log.Println("Fail to open version file:", err.Error())
		return nil
	}
	checksumInt, err := strconv.ParseInt(checksumFile.Name(), 10, 32)
	checksum := Checksum(checksumInt)
	if err != nil {
		log.Println("Fail to parse checksum:", err.Error())
		return nil
	}

	// Get the chunk file
	chunkFile, err := os.Open(chunkserver.chunksDir + "/" + handle.String())
	if err != nil {
		log.Println("Fail to open chunk file:", err.Error())
		return nil
	}
	chunkFileStatus, err := chunkFile.Stat()
	if err != nil {
		log.Println("Fail to stat chunk file:", err.Error())
		return nil
	}
	chunk := Chunk{
		version:   gfs.ChunkVersion(version),
		handle:    handle,
		length:    gfs.Length(chunkFileStatus.Size()),
		chunkFile: chunkFile,
		checksum:  checksum,
	}
	chunkData, err := chunk.Read()
	if err != nil {
		log.Println("Fail to read chunk:", err.Error())
		return nil
	}
	if !chunk.checksum.Check(chunkData) {
		log.Println("Checksum mismatch")
		chunk.removeChunk(chunkserver)
		return nil
	}
	return &chunk
}

// removeChunk removes the chunk from the chunkserver.
// Note: this function only removes the chunk and its metadata from the disk.
// It does not remove the chunk from the chunkserver's chunk list.
func (chunk *Chunk) removeChunk(chunkserver *Chunkserver) {
	chunkserver.chunksLock.Lock()
	defer chunkserver.chunksLock.Unlock()
	_ = os.Remove(chunkserver.chunksDir + "/" + chunk.handle.String())
	_ = os.Remove(chunkserver.chunksDir + "/" + chunk.handle.String() + ".version")
	_ = os.Remove(chunkserver.chunksDir + "/" + chunk.handle.String() + ".checksum")
}
