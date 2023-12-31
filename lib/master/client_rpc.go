package master

import (
	"errors"
	"fmt"
	"gfs"
)

func (master *Master) NamespaceExistsRPC(
	args gfs.NamespaceExistsArgs,
	reply *gfs.NamespaceExistsReply,
) error {
	master.namespacesLock.RLock()
	_, reply.Exists = master.namespaces[args.Namespace]
	master.namespacesLock.RUnlock()
	return nil
}

func (master *Master) CreateNamespaceRPC(
	args gfs.CreateNamespaceArgs,
	reply *gfs.CreateNamespaceReply,
) error {
	master.namespacesLock.Lock()
	_, exists := master.namespaces[args.Namespace]
	master.namespacesLock.Unlock()
	if exists {
		reply.Success = false
		reply.Message = "namespace already exists"
		return nil
	}
	err := master.appendLog(
		MakeOperationLogEntryHeader(CreateNamespaceOperation),
		&CreateNamespaceOperationLogEntry{args.Namespace},
	)
	if err != nil {
		reply.Success = false
		reply.Message = err.Error()
	} else {
		reply.Success = true
		reply.Message = ""
	}
	return nil
}

func (master *Master) GetChunkReplicasRPC(
	args gfs.GetChunkReplicasArgs,
	reply *gfs.GetChunkReplicasReply,
) error {
	master.chunksLock.RLock()
	chunk, exist := master.chunks[args.ChunkHandle]
	master.chunksLock.RUnlock()
	if !exist {
		reply.Valid = false
		return nil
	}
	chunk.RLock()
	reply.Shared = chunk.RefCount > 1
	if chunk.Servers.Empty() {
		reply.Version = chunk.Version
		chunk.Unlock()
		reply.Valid = true
		reply.Orphan = true
		reply.HasPrimary = false
		reply.Locations = []gfs.ServerInfo{}
		gfs.Log(gfs.Warning, fmt.Sprintf("chunk %d is orphan", args.ChunkHandle))
		return nil
	}
	if chunk.RefCount > 1 {
		reply.Locations = chunk.Servers.ToSlice()
		reply.Version = chunk.Version
		chunk.Unlock()
		reply.Valid = true
		reply.Orphan = false
		reply.HasPrimary = false
		return nil
	}
	if chunk.hasLeaseHolder() {
		reply.Locations = chunk.Servers.ToSlice()
		if chunk.hasLeaseHolder() {
			reply.Primary = *chunk.Leaseholder
			reply.PrimaryExpireTime = chunk.LeaseExpire
		}
		reply.Version = chunk.Version
		chunk.RUnlock()
		reply.Valid = true
		reply.Orphan = false
		reply.HasPrimary = true
		return nil
	} else if args.ReadOnly {
		reply.Locations = chunk.Servers.ToSlice()
		reply.Version = chunk.Version
		chunk.RUnlock()
		reply.Valid = true
		reply.Orphan = false
		reply.HasPrimary = false
		return nil
	}
	chunk.RUnlock()
	if err := master.grantLease(args.ChunkHandle); err != nil {
		reply.Valid = false
		return err
	}
	chunk.RLock()
	if !chunk.hasLeaseHolder() {
		// This case is very rare because it is hardly possible that the
		// lease expired or revoked between the two locks. If this branch
		// is often taken, maybe there are some bugs in terms of the locks.
		chunk.RUnlock()
		reply.Valid = false
		gfs.Log(gfs.Warning, "chunk has no lease holder")
		return nil
	}
	reply.Valid = true
	reply.Orphan = false
	reply.HasPrimary = true
	reply.Locations = chunk.Servers.ToSlice()
	reply.Primary = *chunk.Leaseholder
	reply.PrimaryExpireTime = chunk.LeaseExpire
	reply.Version = chunk.Version
	chunk.RUnlock()
	return nil
}

func (master *Master) GetFileChunksRPC(
	args gfs.GetFileChunksArgs,
	reply *gfs.GetFileChunksReply,
) error {
	master.namespacesLock.RLock()
	namespace, exist := master.namespaces[args.Namespace]
	master.namespacesLock.RUnlock()
	if !exist {
		reply.Valid = false
		return errors.New("namespace does not exist")
	}
	namespace.RLock()
	defer namespace.RUnlock()
	file, err := namespace.lockAndGetFile(args.Filename, true)
	if err != nil {
		reply.Valid = false
		return err
	}
	reply.Valid = true
	reply.Chunks = file.Chunks
	_ = namespace.UnlockFileOrDirectory(args.Filename, true)
	return nil
}

func (master *Master) CreateFileRPC(
	args gfs.CreateFileArgs,
	reply *gfs.CreateFileReply,
) error {
	master.namespacesLock.RLock()
	namespace, exist := master.namespaces[args.Namespace]
	master.namespacesLock.RUnlock()
	if !exist {
		reply.Successful = false
		reply.ErrorMsg = "namespace does not exist"
		return nil
	}
	namespace.Lock()
	_, err := namespace.lockAndGetFile(args.Filename, true)
	namespace.Unlock()
	if err == nil {
		reply.Successful = false
		reply.ErrorMsg = "file already exists"
		return nil
	}
	err = master.appendLog(
		MakeOperationLogEntryHeader(CreateFileOperation),
		&CreateFileOperationLogEntry{
			Namespace: args.Namespace,
			Pathname:  args.Filename,
		},
	)
	if err != nil {
		reply.Successful = false
		reply.ErrorMsg = err.Error()
		return nil
	}
	reply.Successful = true
	reply.ErrorMsg = ""
	return nil
}

func (master *Master) AddNewChunkToFileRPC(
	args gfs.AddNewChunkToFileArgs,
	reply *gfs.AddNewChunkToFileReply,
) error {
	master.namespacesLock.RLock()
	namespace, exist := master.namespaces[args.Namespace]
	master.namespacesLock.RUnlock()
	if !exist {
		reply.Successful = false
		reply.ErrorMsg = "namespace does not exist"
		return nil
	}
	namespace.Lock()
	_, err := namespace.lockAndGetFile(args.Filename, true)
	namespace.Unlock()
	if err == nil {
		reply.Successful = false
		reply.ErrorMsg = "file already exists"
		return nil
	}
	handle := master.getNextChunkHandle()
	err = master.appendLog(
		MakeOperationLogEntryHeader(AddChunkToFile),
		&AddChunkToFileOperationLogEntry{
			Namespace: args.Namespace,
			Pathname:  args.Filename,
			Chunk:     handle,
		},
	)
	if err != nil {
		reply.Successful = false
		reply.ErrorMsg = err.Error()
		return nil
	}
	reply.Successful = true
	reply.ErrorMsg = ""
	return nil
}
