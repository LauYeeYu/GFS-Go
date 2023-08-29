package gfs

import (
	"errors"
	"strconv"
	"strings"
)

type Namespace string

// Chunk & file commons

type ChunkHandle int64  // global unique identifier for a chunk
type ChunkVersion int64 // version number for a chunk
type ChunkIndex int64   // index of a chunk in a file
type Length int64       // length of a chunk or file

func (chunkHandle ChunkHandle) String() string {
	return strconv.FormatInt(int64(chunkHandle), 10)
}

// Server commons

type ServerType int

const (
	MASTER ServerType = iota
	CHUNKSERVER
)

type ServerInfo struct {
	ServerType ServerType
	ServerAddr string
}

// Path methods

type PathInfo struct {
	Pathname string
	IsDir    bool
}

func (pathInfo *PathInfo) Parent() (*PathInfo, error) {
	i := strings.LastIndexByte(pathInfo.Pathname, '/')
	if i == -1 {
		return nil, errors.New("PathInfo.Parent: no parent")
	}
	return &PathInfo{Pathname: pathInfo.Pathname[:i], IsDir: true}, nil
}

func MakePathInfo(pathname string, isDir bool) *PathInfo {
	if pathname == "/" {
		return &PathInfo{IsDir: true, Pathname: "/"}
	} else if strings.Contains(pathname, "//") {
		return nil
	} else if strings.Contains(pathname, "/.") {
		return nil
	} else if strings.HasPrefix(pathname, "/") == false {
		return nil
	} else {
		return &PathInfo{IsDir: isDir, Pathname: strings.TrimSuffix(pathname, "/")}
	}
}

// Errors

type FatalError struct {
	error
}

func NewFatalError(err error) *FatalError {
	return &FatalError{err}
}

// Chunk-client status
const (
	Successful = iota
	WrongServer
	ChunkNotExist
	ChunkVersionNotMatch
	NotPrimary
	ExceedLengthOfChunk
	Failed // Error caused by concurrent writes
)
