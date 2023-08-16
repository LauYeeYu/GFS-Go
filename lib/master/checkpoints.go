package master

import (
	"encoding/gob"
	"errors"
	"fmt"
	"gfs"
	"gfs/utils"
	"os"
	"time"
)

type Checkpoint struct {
	Namespaces      map[gfs.Namespace]PersistentNamespaceMetadata
	Chunks          map[gfs.ChunkHandle]PersistentChunkMetadata
	NextChunkHandle gfs.ChunkHandle
}

type FileChunks []gfs.ChunkHandle

type PersistentNamespaceMetadata struct {
	Files map[string]FileChunks
}

type PersistentChunkMetadata struct {
	Version     gfs.ChunkVersion
	RefCount    int64
	LeaseHolder *gfs.ServerInfo
	LeaseExpire time.Time
}

func (dir *DirectoryInfo) addFileWithoutLock(fileName []string, chunks FileChunks) {
	switch len(fileName) {
	case 0:
		panic("should not happen")
	case 1:
		dir.Files[fileName[0]] = &FileMetadata{Chunks: chunks}
	default:
		subDir, exists := dir.Directories[fileName[0]]
		if !exists {
			subDir = &DirectoryInfo{
				Parent:      dir,
				Files:       make(map[string]*FileMetadata),
				Directories: make(map[string]*DirectoryInfo),
			}
			dir.Directories[fileName[0]] = subDir
		}
		subDir.addFileWithoutLock(fileName[1:], chunks)
	}
}

func (data *PersistentNamespaceMetadata) toNamespaceMetadata() *NamespaceMetadata {
	namespaceMetadata := MakeNamespace()
	for path, fileChunks := range data.Files {
		namespaceMetadata.Root.addFileWithoutLock(utils.ParsePath(path), fileChunks)
	}
	return namespaceMetadata
}

func (data *PersistentChunkMetadata) toChunkMetadata() *ChunkMetadata {
	return &ChunkMetadata{
		Version:     data.Version,
		RefCount:    data.RefCount,
		LeaseHolder: data.LeaseHolder,
		LeaseExpire: data.LeaseExpire,
		Servers:     make(map[gfs.ServerInfo]bool),
	}
}

func (checkpoint *Checkpoint) toMasterStruct(
	serverInfo gfs.ServerInfo,
	masterRoot string,
	checkpointIndex int64,
) *Master {
	master := MakeMaster(serverInfo, masterRoot)
	for namespace, namespaceData := range checkpoint.Namespaces {
		master.namespaces[namespace] = namespaceData.toNamespaceMetadata()
	}
	for chunkHandle, chunkData := range checkpoint.Chunks {
		master.chunks[chunkHandle] = chunkData.toChunkMetadata()
	}
	master.nextChunkHandle = checkpoint.NextChunkHandle
	master.nextLogIndex = checkpointIndex + 1
	return master
}

func (namespace *NamespaceMetadata) getPersistentNamespaceMetaData() PersistentNamespaceMetadata {
	persistentNamespaceMetadata := PersistentNamespaceMetadata{
		Files: make(map[string]FileChunks),
	}
	if namespace.Root != nil {
		namespace.Root.putAllFilesTogether(&persistentNamespaceMetadata, "/")
	}
	return persistentNamespaceMetadata
}

func (dir *DirectoryInfo) putAllFilesTogether(
	fileMap *PersistentNamespaceMetadata,
	directoryPath string,
) {
	for name, file := range dir.Files {
		fileMap.Files[utils.MergePath(directoryPath, name)] = file.Chunks
	}
	for name, subDir := range dir.Directories {
		subDir.putAllFilesTogether(fileMap, utils.MergePath(directoryPath, name))
	}
}

func (chunkMeta *ChunkMetadata) getPersistentChunkMeta() PersistentChunkMetadata {
	return PersistentChunkMetadata{
		Version:     chunkMeta.Version,
		RefCount:    chunkMeta.RefCount,
		LeaseHolder: chunkMeta.LeaseHolder,
		LeaseExpire: chunkMeta.LeaseExpire,
	}
}

func (master *Master) toCheckpointType() Checkpoint {
	checkpoint := Checkpoint{
		Namespaces:      make(map[gfs.Namespace]PersistentNamespaceMetadata),
		Chunks:          make(map[gfs.ChunkHandle]PersistentChunkMetadata),
		NextChunkHandle: master.nextChunkHandle,
	}
	for namespaceName, namespaceMetadata := range master.namespaces {
		checkpoint.Namespaces[namespaceName] = namespaceMetadata.getPersistentNamespaceMetaData()
	}
	for chunkHandle, chunkMetadata := range master.chunks {
		checkpoint.Chunks[chunkHandle] = chunkMetadata.getPersistentChunkMeta()
	}
	return checkpoint
}

func getLastCheckpoint(serverInfo gfs.ServerInfo, masterRoot string) (*Master, int64, error) {
	checkpointDir := utils.MergePath(masterRoot, "checkpoints")
	checkpointNum := utils.MergePath(checkpointDir, "last_checkpoint_index")
	index, err := utils.ReadTextInt64FromFile(checkpointNum)
	if err != nil {
		return nil, 0, err
	}
	if index == 0 {
		return MakeMaster(serverInfo, masterRoot), 0, nil
	}
	fileName := utils.MergePath(checkpointDir, fmt.Sprintf("%d.checkpoint", index))
	file, err := os.OpenFile(fileName, os.O_WRONLY, 0644)
	if err != nil {
		return nil, 0, err
	}
	decoder := gob.NewDecoder(file)
	var checkpoint Checkpoint
	if err = decoder.Decode(&checkpoint); err != nil {
		return nil, 0, err
	}

	return nil, 0, nil

}

func (master *Master) addNewCheckpoint(index int64) error {
	// Make sure that there is only one checkpoint goroutines in case there
	// are too many checkpoint routines working together, causing conflict
	// on the last_checkpoint_index file.
	master.checkpointLock.Lock()
	defer master.checkpointLock.Unlock()

	// Get last checkpoint
	lastCheckpoint, oldIndex, err := getLastCheckpoint(master.server, master.storageDir)
	if err != nil {
		oldIndex = 0
		lastCheckpoint = MakeMaster(master.server, master.storageDir)
	}
	if oldIndex >= index {
		return errors.New("the latest checkpoint is newer")
	}

	// Replay the remained logs
	for i := oldIndex + 1; i <= index; i++ {
		if err = lastCheckpoint.replayLog(i); err != nil {
			return errors.New(fmt.Sprintf("an error occurred when replaying log %d: %v", i, err.Error()))
		} else {
			master.nextLogIndex++
		}
	}

	// Make another checkpoint
	fileName := utils.MergePath(master.checkpointDir, fmt.Sprintf("%d.checkpoint", index))
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if file.Truncate(0) != nil {
		return err
	}
	if _, err = file.Seek(0, 0); err != nil {
		_ = file.Close()
		return err
	}
	encoder := gob.NewEncoder(file)
	if err = encoder.Encode(master.toCheckpointType()); err != nil {
		_ = file.Close()
		return err
	}
	_ = file.Close()

	// Update the index file
	err = utils.WriteTextInt64ToFile(
		utils.MergePath(master.checkpointDir, "last_checkpoint_index"),
		index,
	)
	if err != nil {
		return err
	}
	master.lastCheckpoint = index

	// Remove the last checkpoint
	// If we remove the last checkpoint every time we add a new one, there
	// should be only one checkpoint.
	if oldIndex != 0 {
		oldCheckpointName := utils.MergePath(master.checkpointDir, fmt.Sprintf("%d.checkpoint", oldIndex))
		_ = os.Remove(oldCheckpointName)
	}

	// TODO: compress the logs in range (oldIndex, newIndex)
	return nil
}
