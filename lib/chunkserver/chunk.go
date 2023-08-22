package chunkserver

import (
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"os"
	"strconv"
	"sync"
	"time"
)

type Chunk struct {
	version   gfs.ChunkVersion // stored in $storageDir/chunks/$handle.version
	handle    gfs.ChunkHandle
	chunkFile *os.File // stored in $storageDir/chunks/$handle
	checksum  Checksum // stored in $storageDir/chunks/$handle.checksum

	// Lease control
	isPrimary       bool
	leaseExpireTime time.Time

	sync.RWMutex
}

func chunkFilePath(handle gfs.ChunkHandle, chunkserver *Chunkserver) string {
	return utils.MergePath(chunkserver.chunksDir, fmt.Sprintf("%d", handle))
}
func chunkVersionFilePath(handle gfs.ChunkHandle, chunkserver *Chunkserver) string {
	return utils.MergePath(chunkserver.chunksDir, fmt.Sprintf("%d.version", handle))
}
func chunkChecksumFilePath(handle gfs.ChunkHandle, chunkserver *Chunkserver) string {
	return utils.MergePath(chunkserver.chunksDir, fmt.Sprintf("%d.checksum", handle))
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

// flushLease flushes the lease status of the chunk
// Note: this function is not concurrency-safe.
func (chunk *Chunk) flushLease() {
	if chunk.isPrimary {
		if chunk.leaseExpireTime.Before(time.Now()) {
			chunk.isPrimary = false
		}
	}
}

func LoadChunkMetadata(
	handle gfs.ChunkHandle,
	chunkserver *Chunkserver,
) *Chunk {
	// Get the version
	versionFile, err := os.Open(chunkVersionFilePath(handle, chunkserver))
	if err != nil {
		gfs.Log(gfs.Error, "Fail to open version file: ", err.Error())
		return nil
	}
	version, err := strconv.ParseInt(versionFile.Name(), 10, 32)
	if err != nil {
		gfs.Log(gfs.Error, "Fail to parse version: ", err.Error())
		return nil
	}

	// Get the checksum
	checksumFile, err := os.Open(chunkChecksumFilePath(handle, chunkserver))
	if err != nil {
		gfs.Log(gfs.Error, "Fail to open version file: ", err.Error())
		return nil
	}
	checksumInt, err := strconv.ParseInt(checksumFile.Name(), 10, 32)
	checksum := Checksum(checksumInt)
	if err != nil {
		gfs.Log(gfs.Error, "Fail to parse checksum: ", err.Error())
		return nil
	}

	// Get the chunk file
	chunkFile, err := os.Open(chunkFilePath(handle, chunkserver))
	if err != nil {
		gfs.Log(gfs.Error, "Fail to open chunk file: ", err.Error())
		return nil
	}
	chunk := &Chunk{
		version:   gfs.ChunkVersion(version),
		handle:    handle,
		chunkFile: chunkFile,
		checksum:  checksum,
		isPrimary: false,
	}
	chunkData, err := chunk.read()
	if err != nil {
		gfs.Log(gfs.Error, "Fail to read chunk:", err.Error())
		return nil
	}
	if !chunk.checksum.Check(chunkData) {
		gfs.Log(gfs.Error, "Checksum mismatch")
		chunk.removeChunk(chunkserver)
		return nil
	}
	return chunk
}

// removeChunk removes the chunk from the chunkserver.
// Note: this function only removes the chunk and its metadata from the disk.
// It does not remove the chunk from the chunkserver's chunk list.
func (chunk *Chunk) removeChunk(chunkserver *Chunkserver) {
	chunkserver.chunksLock.Lock()
	defer chunkserver.chunksLock.Unlock()
	_ = os.Remove(chunkFilePath(chunk.handle, chunkserver))
	_ = os.Remove(chunkVersionFilePath(chunk.handle, chunkserver))
	_ = os.Remove(chunkChecksumFilePath(chunk.handle, chunkserver))
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
	if _, err = chunk.chunkFile.Write(data); err != nil {
		return gfs.NewFatalError(err)
	}
	return nil
}

// rangedWrite writes data to the chunk at the given offset.
// Any error should be regarded as a data corruption.
// Note: this function is not concurrency-safe, the caller should hold the lock.
func (chunk *Chunk) rangedWrite(offset gfs.Length, data []byte, storagePath string) error {
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
	return chunk.checksum.Update(chunk.chunkFile, checksumPath(storagePath, chunk.handle))
}

// padChunkTo pads the chunk to the given offset. Any error should be regarded
// as a data corruption.
// Note: this function is not concurrency-safe, the caller should hold the lock.
func (chunk *Chunk) padChunkTo(offset gfs.Length, storagePath string) error {
	if offset > gfs.ChunkSize {
		return errors.New(fmt.Sprintf("cannot pad chunk to offset %v", offset))
	}
	if err := chunk.chunkFile.Truncate(int64(offset)); err != nil {
		return gfs.NewFatalError(err)
	}
	return chunk.checksum.Update(chunk.chunkFile, checksumPath(storagePath, chunk.handle))
}

func (chunk *Chunk) padChunk(length gfs.Length, storagePath string) error {
	return chunk.padChunkTo(chunk.length()+length, storagePath)
}

// append appends data to the chunk.
// Any error should be regarded as a data corruption.
// Note: this function is not concurrency-safe, the caller should hold the lock.
func (chunk *Chunk) append(data []byte, storagePath string) error {
	originalLength := chunk.length()
	if originalLength+gfs.Length(len(data)) > gfs.ChunkSize {
		err := chunk.padChunk(gfs.Length(len(data)), storagePath)
		if err != nil {
			return err
		}
	}
	err := chunk.internalRangedWrite(originalLength, data)
	if err != nil {
		return err
	}
	return chunk.checksum.Update(chunk.chunkFile, checksumPath(storagePath, chunk.handle))
}

// appendFrom appends data to the chunk from the given offset.
// Any error should be regarded as a data corruption.
// Note: this function is not concurrency-safe, the caller should hold the lock.
func (chunk *Chunk) appendFrom(offset gfs.Length, data []byte, storagePath string) error {
	if offset+gfs.Length(len(data)) > gfs.ChunkSize {
		err := chunk.padChunk(offset+gfs.Length(len(data)), storagePath)
		if err != nil {
			return err
		}
	}
	err := chunk.internalRangedWrite(offset, data)
	if err != nil {
		return err
	}
	return chunk.checksum.Update(chunk.chunkFile, checksumPath(storagePath, chunk.handle))
}
