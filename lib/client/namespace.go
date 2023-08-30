package client

import (
	"errors"
	"gfs"
	"gfs/utils"
)

func (state *WorkingState) ChangeNamespace(namespace gfs.Namespace) error {
	exists, err := state.client.NamespaceExists(namespace)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("namespace does not exist")
	}
	state.currentNamespace = namespace
	return nil
}

func (client *Client) NamespaceExists(namespace gfs.Namespace) (bool, error) {
	reply := gfs.NamespaceExistsReply{}
	err := utils.RemoteCall(
		client.master, "Master.NamespaceExistsRPC",
		gfs.NamespaceExistsArgs{Namespace: namespace},
		&reply,
	)
	if err != nil {
		return false, err
	}
	return reply.Exists, nil
}

func (client *Client) CreateNamespace(namespace gfs.Namespace) error {
	reply := gfs.CreateNamespaceReply{}
	err := utils.RemoteCall(
		client.master, "Master.CreateNamespaceRPC",
		gfs.CreateNamespaceArgs{Namespace: namespace},
		&reply,
	)
	if err != nil {
		return err
	}
	if !reply.Success {
		return errors.New(reply.Message)
	}
	return nil
}
