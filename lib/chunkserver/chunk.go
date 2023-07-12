package chunkserver

import (
	"errors"
	"fmt"
	"gfs"
	"log"
	"os"
	"strconv"
	"sync"
)

type Chunk struct {
	version   gfs.ChunkVersion // stored in $storageDir/chunks/$handle.version
	handle    gfs.ChunkHandle
	chunkFile *os.File // stored in $storageDir/chunks/$handle
	checksum  Checksum // stored in $storageDir/chunks/$handle.checksum
	sync.RWMutex
}

// length returns the length of the chunk file
// Note: this function is not concurrency-safe.
func (chunk *Chunk) length() gfs.Length {
	fileInfo, err := chunk.chunkFile.Stat()
	if err != nil {
		return 0
	}
	return gfs.Length(fileInfo.Size())
}

func (chunk *Chunk) read() ([]byte, error) {
	data := make([]byte, chunk.length())
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
	chunk := Chunk{
		version:   gfs.ChunkVersion(version),
		handle:    handle,
		chunkFile: chunkFile,
		checksum:  checksum,
	}
	chunkData, err := chunk.read()
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

// rangedRead reads data from the chunk at the given offset.
// Note: this function is not concurrency-safe, the caller should hold the lock.
func (chunk *Chunk) rangedRead(offset gfs.Length, data []byte) error {
	end := offset + gfs.Length(len(data))
	chunkLength := chunk.length()
	if offset < 0 || end >= chunkLength {
		return errors.New(fmt.Sprintf("read data (%v-%v) out of range (0-%v)",
			offset, end, chunkLength))
	}

	fileOffset, err := chunk.chunkFile.Seek(int64(offset), 0)
	if err != nil {
		return err
	}
	if fileOffset != int64(offset) {
		return errors.New(fmt.Sprintf("seek to %v failed", offset))
	}
	_, err = chunk.chunkFile.Read(data)
	if err != nil {
		return err
	}
	return nil
}

func (chunk *Chunk) internalRangedWrite(offset gfs.Length, data []byte) error {
	fileOffset, err := chunk.chunkFile.Seek(int64(offset), 0)
	if err != nil {
		return gfs.NewFatalError(err)
	} else if fileOffset != int64(offset) {
		return gfs.NewFatalError(errors.New(fmt.Sprintf("seek to %v failed", offset)))
	}
	_, err = chunk.chunkFile.Write(data)
	return gfs.NewFatalError(err)
}

// rangedWrite writes data to the chunk at the given offset.
// Any error should be regarded as a data corruption.
// Note: this function is not concurrency-safe, the caller should hold the lock.
func (chunk *Chunk) rangedWrite(offset gfs.Length, data []byte) error {
	end := offset + gfs.Length(len(data))
	chunkLength := chunk.length()
	if offset < 0 || end > gfs.ChunkSize {
		return errors.New(fmt.Sprintf("write data (%v-%v) out of range (0-%v)",
			offset, end, chunkLength))
	}
	err := chunk.internalRangedWrite(offset, data)
	if err != nil {
		return err
	}
	return chunk.checksum.Update(chunk.chunkFile)
}

// padChunkTo pads the chunk to the given offset. Any error should be regarded
// as a data corruption.
// Note: this function is not concurrency-safe, the caller should hold the lock.
func (chunk *Chunk) padChunkTo(offset gfs.Length) error {
	if offset > gfs.ChunkSize {
		return errors.New(fmt.Sprintf("cannot pad chunk to offset %v", offset))
	}
	err := chunk.chunkFile.Truncate(int64(offset))
	if err != nil {
		return gfs.NewFatalError(err)
	}
	return chunk.checksum.Update(chunk.chunkFile)
}

func (chunk *Chunk) padChunk(length gfs.Length) error {
	return chunk.padChunkTo(chunk.length() + length)
}

// append appends data to the chunk.
// Any error should be regarded as a data corruption.
// Note: this function is not concurrency-safe, the caller should hold the lock.
func (chunk *Chunk) append(data []byte) error {
	originalLength := chunk.length()
	if originalLength+gfs.Length(len(data)) > gfs.ChunkSize {
		err := chunk.padChunk(gfs.Length(len(data)))
		if err != nil {
			return err
		}
	}
	err := chunk.internalRangedWrite(originalLength, data)
	if err != nil {
		return err
	}
	return chunk.checksum.Update(chunk.chunkFile)
}

// appendFrom appends data to the chunk from the given offset.
// Any error should be regarded as a data corruption.
// Note: this function is not concurrency-safe, the caller should hold the lock.
func (chunk *Chunk) appendFrom(offset gfs.Length, data []byte) error {
	if offset+gfs.Length(len(data)) > gfs.ChunkSize {
		err := chunk.padChunk(offset + gfs.Length(len(data)))
		if err != nil {
			return err
		}
	}
	err := chunk.internalRangedWrite(offset, data)
	if err != nil {
		return err
	}
	return chunk.checksum.Update(chunk.chunkFile)
}
