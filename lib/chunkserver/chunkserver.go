package chunkserver

import (
	"container/heap"
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Chunkserver struct {
	server   gfs.ServerInfo
	master   gfs.ServerInfo
	listener net.Listener

	// storage
	storageDir string
	chunksDir  string

	// chunks
	chunks              map[gfs.ChunkHandle]*Chunk
	chunksLock          sync.RWMutex
	chunkQueue          chunkQueue
	corruptedChunks     utils.Set[gfs.ChunkHandle]
	corruptedChunksLock sync.Mutex

	// shutdown
	shutdown chan struct{}
}

// MakeChunkserver creates a new Chunkserver instance
func MakeChunkserver(
	chunkserverInfo gfs.ServerInfo,
	masterInfo gfs.ServerInfo,
	storageDir string,
) *Chunkserver {
	chunkserver := Chunkserver{
		server:          chunkserverInfo,
		master:          masterInfo,
		storageDir:      storageDir,
		chunksDir:       utils.MergePath(storageDir, gfs.ChunkDirName),
		chunks:          make(map[gfs.ChunkHandle]*Chunk),
		corruptedChunks: utils.MakeSet[gfs.ChunkHandle](),
		shutdown:        make(chan struct{}),
	}
	if err := chunkserver.loadChunks(); err != nil {
		gfs.Log(gfs.Error, err.Error())
		return nil
	}
	chunks := make([]*chunkPriority, 0, len(chunkserver.chunks))
	for _, chunk := range chunkserver.chunks {
		chunks = append(chunks, makeChunkPriority(chunk))
	}
	chunkserver.chunkQueue = makeChunkQueue(chunks...)
	return &chunkserver
}

func (chunkserver *Chunkserver) loadChunks() error {
	err := os.MkdirAll(chunkserver.chunksDir, 0755)
	if err != nil {
		return err
	}
	files, err := os.ReadDir(chunkserver.chunksDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasSuffix(file.Name(), gfs.ChecksumSuffix) &&
			!strings.HasSuffix(file.Name(), gfs.VersionSuffix) {
			// The file is a chunk
			handleInt, err := strconv.ParseInt(file.Name(), 10, 64)
			if err != nil {
				gfs.Log(gfs.Error, err.Error())
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

func (chunkserver *Chunkserver) Start() error {
	service := rpc.NewServer()
	if err := service.Register(chunkserver); err != nil {
		gfs.Log(gfs.Error, err.Error())
		return errors.New("Master.Start: Register failed")
	}
	listener, err := net.Listen("tcp", chunkserver.server.ServerAddr)
	if err != nil {
		gfs.Log(gfs.Error, err.Error())
		return errors.New("Master.Start: Listen failed")
	}
	chunkserver.listener = listener

	// send HeartBeat
	go func() {
		for {
			err := chunkserver.sendHeartBeat(false)
			if err != nil {
				gfs.Log(gfs.Error, err.Error())
			}
			time.Sleep(gfs.HeartbeatInterval)
		}
	}()

	// handle all RPCs
	go chunkserver.handleAllRPCs()
	return nil
}

func (chunkserver *Chunkserver) handleAllRPCs() {
	for {
		conn, err := chunkserver.listener.Accept()
		if err != nil {
			gfs.Log(gfs.Error, err.Error())
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func (chunkserver *Chunkserver) sendHeartBeat(withAllChunks bool) error {
	// Prepare the number of chunks to send
	var number int
	if withAllChunks {
		number = len(chunkserver.chunks)
	} else {
		number = gfs.HeartbeatChunkNumber
	}

	// Get the chunks to send
	chunks := make([]gfs.ChunkInfo, 0, number)
	putChunk := func(chunk *Chunk) {
		chunk.RLock()
		chunks = append(
			chunks,
			gfs.ChunkInfo{
				Version: chunk.version,
				Handle:  chunk.handle,
				Length:  chunk.length(),
			},
		)
		chunk.RUnlock()
	}
	if withAllChunks {
		chunkserver.chunksLock.RLock()
		for _, chunk := range chunkserver.chunks {
			putChunk(chunk)
		}
		chunkserver.chunksLock.RUnlock()
	} else {
		chunkserver.chunksLock.RLock()
		popped := make([]*chunkPriority, 0, number)
		for i := 0; i < number || len(chunkserver.chunkQueue) > 0; i++ {
			popped = append(popped, heap.Pop(&chunkserver.chunkQueue).(*chunkPriority))
		}
		for _, chunk := range popped {
			putChunk(chunk.chunk)
			chunk.count++
			heap.Push(&chunkserver.chunkQueue, chunk)
		}
		chunkserver.chunksLock.RUnlock()
	}

	// Get the corrupted chunks
	chunkserver.corruptedChunksLock.Lock()
	corruptedChunks := chunkserver.corruptedChunks.ToSlice()
	// Clear the corrupted chunk set
	chunkserver.corruptedChunks.Clear()
	chunkserver.corruptedChunksLock.Unlock()

	// Send heartbeat
	reply := gfs.HeartBeatReply{}
	err := utils.RemoteCall(chunkserver.master, "Master.ReceiveHeartBeatRPC",
		gfs.HeartBeatArgs{
			ServerInfo:      chunkserver.server,
			Chunks:          chunks,
			CorruptedChunks: corruptedChunks,
		},
		&reply)
	if err != nil {
		return err
	}
	for _, chunkHandle := range reply.ExpiredChunks {
		_ = chunkserver.removeChunkAndMeta(chunkHandle)
	}
	if reply.RequireAllChunks {
		go func() {
			err := chunkserver.sendHeartBeat(true)
			if err != nil {
				gfs.Log(gfs.Error, err.Error())
			}
		}()
	}
	return nil
}

func (chunkserver *Chunkserver) removeChunkAndMeta(chunkHandle gfs.ChunkHandle) error {
	chunkserver.chunksLock.Lock()
	chunk, exists := chunkserver.chunks[chunkHandle]
	if !exists {
		chunkserver.chunksLock.Unlock()
		gfs.Log(gfs.Error, "Chunkserver.removeChunk: chunk does not exist")
		return errors.New("Chunkserver.removeChunk: chunk does not exist")
	}
	delete(chunkserver.chunks, chunkHandle)
	chunkserver.chunksLock.Unlock()
	gfs.Log(gfs.Info, fmt.Sprintf("Chunkserver.removeChunk: chunk %d removed", chunkHandle))
	chunk.Lock()
	chunk.removed = true
	chunk.Unlock()
	chunk.removeChunk(chunkserver)
	return nil
}

func (chunkserver *Chunkserver) OnChunkCorrupted(chunkHandle gfs.ChunkHandle) {
	chunkserver.chunksLock.Lock()
	chunk, exists := chunkserver.chunks[chunkHandle]
	if !exists {
		gfs.Log(gfs.Error, "Chunkserver.OnChunkCorrupted: chunk does not exist in chunkserver map")
	} else {
		delete(chunkserver.chunks, chunkHandle)
	}
	chunkserver.chunksLock.Unlock()
	chunk.Lock()
	chunk.removed = true
	chunk.Unlock()
	chunk.removeChunk(chunkserver)

	// Put the chunk into the corrupted chunk set to allow master to
	// remove it through heartbeat
	chunkserver.corruptedChunksLock.Lock()
	chunkserver.corruptedChunks.Add(chunkHandle)
	chunkserver.corruptedChunksLock.Unlock()
}
