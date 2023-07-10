package utils

import (
	"gfs"
	"net/rpc"
)

func RemoteCall(
	server gfs.ServerInfo,
	method string,
	args interface{},
	reply interface{},
) error {
	client, err := rpc.Dial("tcp", server.ServerAddr)
	if err != nil {
		return err
	}
	err = client.Call(method, args, reply)
	if err != nil {
		return err
	}
	_ = client.Close()
	return nil
}
