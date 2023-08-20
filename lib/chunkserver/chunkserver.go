package chunkserver

import (
	"container/heap"
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"log"
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
	leasesDir  string

	// chunks
	chunks     map[gfs.ChunkHandle]*Chunk
	chunksLock sync.RWMutex
	chunkQueue chunkQueue

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
		server:     chunkserverInfo,
		master:     masterInfo,
		storageDir: storageDir,
		chunksDir:  utils.MergePath(storageDir, gfs.ChunkDirName),
		leasesDir:  utils.MergePath(storageDir, gfs.LeaseDirName),
		chunks:     make(map[gfs.ChunkHandle]*Chunk),
	}
	if err := chunkserver.loadChunks(); err != nil {
		log.Println(err.Error())
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
	files, err := os.ReadDir(chunkserver.chunksDir)
	if err != nil {
		log.Println(err.Error())
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

func (chunkserver *Chunkserver) Start() error {
	service := rpc.NewServer()
	if err := service.Register(chunkserver); err != nil {
		log.Println(err.Error())
		return errors.New("Master.Start: Register failed")
	}
	listener, err := net.Listen("tcp", chunkserver.server.ServerAddr)
	if err != nil {
		log.Println(err.Error())
		return errors.New("Master.Start: Listen failed")
	}
	chunkserver.listener = listener

	// send HeartBeat
	go func() {
		for {
			err := chunkserver.sendHeartBeat(false)
			if err != nil {
				log.Println(err.Error())
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
			log.Println(err.Error())
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func (chunkserver *Chunkserver) sendHeartBeat(withAllChunks bool) error {
	var number int
	if withAllChunks {
		number = len(chunkserver.chunks)
	} else {
		number = gfs.HeartbeatChunkNumber
	}
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
	reply := gfs.HeartBeatReply{}
	err := utils.RemoteCall(chunkserver.master, "Master.ReceiveHeartBeatRPC",
		gfs.HeartBeatArgs{ServerInfo: chunkserver.server, Chunks: chunks},
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
				log.Println(err.Error())
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
	chunk.removeChunk(chunkserver)
	return nil
}

func (chunkserver *Chunkserver) UpdateChunkRPC(
	args gfs.UpdateChunkArgs,
	reply *gfs.UpdateChunkReply,
) error {
	chunkserver.chunksLock.Lock()
	chunk, exists := chunkserver.chunks[args.ChunkHandle]
	chunkserver.chunksLock.Unlock()
	if !exists {
		reply.Accepted = false
		reply.CurrentVersion = -1
		gfs.Log(gfs.Error, "Chunkserver.UpdateChunkRPC: chunk does not exist")
		return errors.New("Chunkserver.UpdateChunkRPC: chunk does not exist")
	}
	chunk.Lock()
	defer chunk.Unlock()
	if args.OriginalVersion != chunk.version {
		reply.Accepted = false
		reply.CurrentVersion = chunk.version
		if args.OriginalVersion < chunk.version {
			gfs.Log(
				gfs.Info,
				fmt.Sprintf(
					"Chunkserver.UpdateChunkRPC: version too old, current version: %d, original version: %d",
					chunk.version,
					args.OriginalVersion,
				),
			)
		} else {
			gfs.Log(gfs.Error,
				fmt.Sprintf(
					"Chunkserver.UpdateChunkRPC: version too new, current version: %d, original version: %d",
					chunk.version,
					args.OriginalVersion,
				),
			)
			return errors.New("Chunkserver.UpdateChunkRPC: version too new")
		}
	} else {
		chunk.version = args.NewVersion
		reply.Accepted = true
		reply.CurrentVersion = chunk.version
		gfs.Log(gfs.Info,
			fmt.Sprintf(
				"Chunkserver.UpdateChunkRPC: version updated, current version: %d, original version: %d",
				chunk.version,
				args.OriginalVersion,
			),
		)
	}
	return nil
}
