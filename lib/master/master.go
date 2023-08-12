package master

import (
	"errors"
	"gfs"
	"gfs/utils"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	server gfs.ServerInfo

	storageDir       string // Path for storing metadata
	logDir           string // Path for storing operation logs
	compressedLogDir string // Path for storing compressed operation logs
	checkpointDir    string // Path for storing checkpoints

	namespaces       map[gfs.Namespace]*NamespaceMetadata
	namespacesLock   sync.RWMutex
	chunks           map[gfs.ChunkHandle]*ChunkMetadata
	chunksLock       sync.RWMutex
	nextChunkHandle  gfs.ChunkHandle
	nextChunkLock    sync.Mutex
	chunkservers     map[gfs.ServerInfo]struct{}
	chunkserversLock sync.RWMutex

	// Operation logs
	nextLogIndex     int64      // start from 1
	operationLogLock sync.Mutex // make sure that only one operation log is written at a time
	checkpointLock   sync.Mutex // make sure that only one checkpoint is written at a time

	// RPC
	listener net.Listener

	// shutdown
	shutdown chan struct{}
}

// MakeMaster creates a new Master instance
func MakeMaster(server gfs.ServerInfo, storageDir string) *Master {
	return &Master{
		server:           server,
		storageDir:       storageDir,
		logDir:           utils.MergePath(storageDir, "log"),
		compressedLogDir: utils.MergePath(storageDir, "compressed_log"),
		checkpointDir:    utils.MergePath(storageDir, "checkpoints"),

		namespaces:   make(map[gfs.Namespace]*NamespaceMetadata),
		chunks:       make(map[gfs.ChunkHandle]*ChunkMetadata),
		chunkservers: make(map[gfs.ServerInfo]struct{}),

		nextChunkHandle: 0,
		nextLogIndex:    1,

		shutdown: make(chan struct{}),
	}
}

// Start starts the master server
// Note: this method is not thread-safe
func (master *Master) Start() error {
	service := rpc.NewServer()
	if err := service.Register(master); err != nil {
		log.Println(err.Error())
		return errors.New("Master.Start: Register failed")
	}
	listener, err := net.Listen("tcp", master.server.ServerAddr)
	if err != nil {
		log.Println(err.Error())
		return errors.New("Master.Start: Listen failed")
	}
	master.listener = listener
	go master.handleAllRPCs()
	go master.periodicCheck()
	log.Printf("Master started. Listening on %v, root path: %v\n",
		master.server.ServerAddr, master.storageDir)
	return nil
}

// Shutdown shuts down the master server
func (master *Master) Shutdown() error {
	err := master.listener.Close()
	if err != nil {
		return err
	}
	close(master.shutdown)
	return nil
}

func (master *Master) handleAllRPCs() {
	for {
		select {
		case <-master.shutdown:
			return
		default:
		}
		conn, err := master.listener.Accept()
		if err != nil {
			log.Println(err.Error())
			continue
		}
		go func() {
			rpc.ServeConn(conn)
			err := conn.Close()
			if err != nil {
				return
			}
		}()
	}
}

// periodicCheck periodically checks the status of the master
// Things to check:
// 1. Garbage collection
// 2. Re-replication
// 3. Rebalancing
func (master *Master) periodicCheck() {
	ticker := time.Tick(gfs.PeriodicWorkInterval)
	for {
		select {
		case <-master.shutdown:
			return
		default:
		}
		<-ticker
		// TODO: implement periodic check
	}
}

func (master *Master) ReceiveHeartBeatRPC(
	args gfs.HeartBeatArgs,
	reply *gfs.HeartBeatReply,
) error {
	// register new chunkserver if needed
	master.chunkserversLock.Lock()
	_, exist := master.chunkservers[args.ServerInfo]
	if !exist {
		log.Printf("New chunkserver %v joined\n", args.ServerInfo)
		master.chunkservers[args.ServerInfo] = struct{}{}
	}
	master.chunkserversLock.Unlock()

	// update chunk status
	expiredChunks := make([]gfs.ChunkHandle, 0)
	for _, chunk := range args.Chunks {
		master.chunksLock.Lock()
		chunkMeta, existChunk := master.chunks[chunk.Handle]
		if existChunk {
			if chunk.Version < master.chunks[chunk.Handle].Version {
				log.Printf("Chunk %v version %v is stale, ignore\n",
					chunk.Handle, chunk.Version)
				expiredChunks = append(expiredChunks, chunk.Handle)
				chunkMeta.removeChunkserver(args.ServerInfo)
			} else {
				chunkMeta.addChunkserver(args.ServerInfo)
			}
		} else {
			log.Printf("Chunk %v does not exist, ignore\n", chunk.Handle)
		}
		master.chunksLock.Unlock()
	}

	// return expired chunks
	reply.ExpiredChunks = expiredChunks
	return nil
}
