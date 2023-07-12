# Master

Master is the controller of all data.

## Metadata

The master stores three major types of metadata:

1. The file and chunk namespace (persistent);
2. The mapping from files to chunks (persistent);
3. The locations of each chunk's replicas (memory only).

### Namespace Management

A namespace represents a lookup table mapping full pathname to metadata.

To achieve finer granularity, each directory and file has its own read-write
lock. When there is a mutation on the metadata of a certain file or a
directory, we need to acquire the read locks of all its prefix
directories and either the write lock or the read lock of itself.

Different from the vanilla GFS, file creation and deletion needs to require
a write lock on the parent directory.

To avoid deadlock, we need to acquire in a consistent total order: they are
first ordered by level in the namespace tree and lexicographically within
the same level.

### Mapping from Files to Chunks

The master should keep an ordered list of the chunks that make up each file.

### Locations of Each Chunk's Replicas

The master asks each chunkserver about its chunks at master startup and
whenever a chunkserver joins the cluster.

## Operation Log

The operation log serves as a historical record of critical metadata changes.

The operation log needs to be replicated to multiple machines for reliability.
The master responds to a client's request only after the log record to disk
both locally and remotely.

The operation log must be written before it applies the log to its in-memory
state.

## Lease Management

The master needs to grant a lease to one of the replicas, which is called
*primary*.

The master may sometimes try to revoke a lease before it expires.

Even if the master loses contact with the primary, the master can still
safely grant a new lease to another replica after the old lease expires.

To avoid multiple primaries, the master should record the lease information
in the operation log.

## Client Requests

### File Creation

The client sends a request to the master to create a file. The master will
write the operation to its operation log, lock the concerned directories
and apply it to its in-memory state. The file has no chunks when created.

Then, the master replies the client, indicating whether the creation is
successful or not.

### Deletion

The client sends a request to the master to delete a file. The master will
write the operation to its operation log, lock the concerned directories
and apply it to its in-memory state. The file has no chunks when deleted.

Then, the master replies the client, indicating whether the deletion is
successful or not.

### Write Request

The master checks whether the reference count is greater than 1. If so, pick a
new chunk handle C'. It then asks each chunkserver that has a current replica
of C to create a new chunk called C'.

Reply with the identity of the primary and the locations of the other replicas.

### Snapshot

For the overview of snapshot, see the
[snapshot section in the overview document](overview.md#snapshot).

When the master receives a snapshot request, it first revokes the lease of the
chunks concerned.

After that, the master logs the operation to disk. Then, it applies this log
record to its in-memory state. The metadata of the file is made and the
reference count is incremented by 1.

Then, the master replies the client, indicating whether the snapshot operation
is successful or not.

## Replica Placement

The chunk replica placement policy servers two purposes:

1. maximizing the data reliability and availability;
2. maximizing network bandwidth utilization.

Chunk replica are created for three reasons:
[chunk creation](#chunk-creation), [re-replication](#re-replication), and
[rebalancing](#rebalancing).

### Chunk Creation

Factors to consider:

1. We want to place new replicas on chunkservers with below-average disk space
   utilization.
2. We want to limit the number of recent creations on each chunkserver to avoid
   hot spots.
3. We want to spread replicas of a chunk across racks.

### Re-replication

The master re-replicates a chunk as soon as the number of available replicas
falls below the user-specified level.

The master re-replicates a chunk when:
- a chunkserver becomes unavailable;
- a chunkserver reports that its replica may be corrupted;
- one of its disk is disabled because of errors;
- the replication goal is increased.

Each chunk that needs to be replicated is prioritized based on several factors:
- how far it is from its replication goal;
- live files as opposed to deleted files;
- the chunk that is blocking client progress.

Then master picks the highest priority chunk and instruct some chunkserver to
copy the chunk data directly from an existing valid replica. The factor is
similar to the one used in [chunk creation](#chunk-creation). To keep cloning
traffic from overwhelming the network, the master limits the number of active
clone operations both for the clusters and for each chunkserver.

### Rebalancing

The criteria are similar to the ones used in [re-replication](#re-replication).
The master will gradually fill a new server and remove the server with
below-average disk space usage.

## Garbage Collection

When a file is deleted by the application, the master logs the deletion
immediately. But instead of reclaiming resources immediately, the file
is just renamed to a hidden name with the deletion timestamp.

During the master's regular scan, it removes ant such hidden files if they
have existed for more than a user-specified time.

Similarly, the master identifies orphaned chunks and erases the metadata for
those chunks. When the master receives a *HeartBeat* message, the master
replies with the identity of all chunks that are no longer present in the
master's metadata.

## Stale Replica Detection

Whenever the master grants a new lease, it increments the version number and
informs the up-to-date replicas.

The master removes stale replicas in regular garbage collection. Before that,
it effectively considers it as not existing.

## Periodic Scans

- [Garbage collection](#garbage-collection);
- [Re-replication](#re-replication);
- [Rebalancing](#rebalancing).
