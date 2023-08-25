package master

import "gfs"

func (master *Master) NamespaceExistsRPC(
	args gfs.NamespaceExistsArgs,
	reply *gfs.NamespaceExistsReply,
) error {
	master.namespacesLock.RLock()
	_, reply.Exists = master.namespaces[args.Namespace]
	master.namespacesLock.RUnlock()
	return nil
}

func (master *Master) MakeNamespaceRPC(
	args gfs.MakeNamespaceArgs,
	reply *gfs.MakeNamespaceReply,
) error {
	master.namespacesLock.Lock()
	defer master.namespacesLock.Unlock()
	_, exists := master.namespaces[args.Namespace]
	if exists {
		reply.Success = false
		reply.Message = "namespace already exists"
		return nil
	}
	master.namespaces[args.Namespace] = MakeNamespace()
	reply.Success = true
	return nil
}
