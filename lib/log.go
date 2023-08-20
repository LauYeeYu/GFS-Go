package gfs

import (
	"log"
	"sync"
)

const (
	Info = iota
	Warning
	Error
)

var logLevel = Info
var lock sync.Mutex

func SetLogLevel(level int) {
	logLevel = level
}

func Log(level int, msg string) {
	lock.Lock()
	if level >= logLevel {
		log.Println(msg)
	}
	lock.Unlock()
}
