package gfs

import "strings"

// Chunk & file commons

type ChunkHandle int64  // global unique identifier for a chunk
type ChunkVersion int64 // version number for a chunk
type ChunkIndex int64   // index of a chunk in a file
type Offset int64       // offset in a file

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

func (pathInfo *PathInfo) Parent() (*PathInfo, *Error) {
	i := strings.LastIndexByte(pathInfo.Pathname, '/')
	if i == -1 {
		return nil, &Error{Code: -1, Message: "No parent"}
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

type ErrorCode int

type Error struct {
	Code    ErrorCode
	Message string
}
