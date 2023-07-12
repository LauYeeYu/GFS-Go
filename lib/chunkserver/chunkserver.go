package chunkserver

import (
	"errors"
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
		chunksDir:  storageDir + "/chunks",
		leasesDir:  storageDir + "/leases",
		chunks:     make(map[gfs.ChunkHandle]*Chunk),
	}
	if err := chunkserver.loadChunks(); err != nil {
		log.Println(err.Error())
		return nil
	}
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
			err := chunkserver.sendHeartBeat()
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

func (chunkserver *Chunkserver) sendHeartBeat() error {
	chunks := make([]gfs.ChunkInfo, 0)
	chunkserver.chunksLock.RLock()
	for _, chunk := range chunkserver.chunks {
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
	chunkserver.chunksLock.RUnlock()
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
	return nil
}

func (chunkserver *Chunkserver) removeChunkAndMeta(chunkHandle gfs.ChunkHandle) error {
	chunkserver.chunksLock.Lock()
	chunk, exists := chunkserver.chunks[chunkHandle]
	if !exists {
		chunkserver.chunksLock.Unlock()
		return errors.New("Chunkserver.removeChunk: chunk does not exist")
	}
	delete(chunkserver.chunks, chunkHandle)
	chunkserver.chunksLock.Unlock()
	chunk.removeChunk(chunkserver)
	return nil
}
