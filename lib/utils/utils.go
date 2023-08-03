package utils

import (
	"errors"
	"strings"
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

func Keys[Key comparable, Value any](m *map[Key]Value) []Key {
	keys := make([]Key, len(*m))
	i := 0
	for key := range *m {
		keys[i] = key
		i++
	}
	return keys
}

func ParsePath(pathname string) []string {
	if pathname == "/" {
		return []string{}
	} else {
		return strings.Split(strings.TrimLeft(pathname, "/"), "/")
	}
}

func Parent(pathname string) (string, error) {
	i := strings.LastIndexByte(pathname, '/')
	if i == -1 {
		return "", errors.New("no parent")
	}
	return pathname[:i], nil
}

func LastSegment(pathname string) string {
	i := strings.LastIndexByte(pathname, '/')
	if i == -1 {
		return pathname
	}
	return pathname[i+1:]
}

func MergePath(parent string, child string) string {
	if strings.HasSuffix(parent, "/") {
		return parent + child
	} else {
		return parent + "/" + child
	}
}
