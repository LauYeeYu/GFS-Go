package master

import (
	"errors"
	"gfs"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	server     gfs.ServerInfo
	storageDir string // Path for storing metadata

	namespaces       map[gfs.Namespace]*NamespaceMetadata
	namespacesLock   sync.RWMutex
	chunks           map[gfs.ChunkHandle]*ChunkMetadata
	chunksLock       sync.RWMutex
	chunkservers     []gfs.ServerInfo
	chunkserversLock sync.RWMutex

	// RPC
	listener net.Listener

	// shutdown
	shutdown chan struct{}
}

// MakeMaster creates a new Master instance
func MakeMaster(server gfs.ServerInfo, storageDir string) *Master {
	return &Master{
		server:     server,
		storageDir: storageDir,

		namespaces:   make(map[gfs.Namespace]*NamespaceMetadata),
		chunks:       make(map[gfs.ChunkHandle]*ChunkMetadata),
		chunkservers: make([]gfs.ServerInfo, 0),

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
	}
}
