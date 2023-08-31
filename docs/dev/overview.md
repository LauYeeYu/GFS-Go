# Overview

Google File System (GFS) is distributed file system to keep multiple copies of
data with a centralized master server.

## Key Concepts

### Chunks

Chunk is the basic unit of data in GFS. A file consists of multiple fixed-size
chunks. The master server keeps track of the metadata of all chunks as well as
all locations of the replicas.

### Chunkservers

Chunkservers are the servers that store the chunks. They are responsible for
storing the chunks, checking the integrity of chunks and responding to the
read/write requests from clients.

Chunkservers are considered as unreliable. They can fail at any time. The
probability of failure is high because there are many chunkservers.

### Master

Master controls all the metadata of the chunks. This includes
1. namespace,
2. access control information,
3. the mapping from files to chunks,
4. the current locations of chunks.

Master also controls system-wide activities such as chunk lease management,
garbage collection, and chunk migration.

Master periodically communicates with each chunkserver in *HeartBeat* messages
to give it instructions and collect its state.

The master uses operation log to recover the state. See the
[operation log section](master.md#operation-log) for more details.

### Lease and Chunk Versioning

The design of lease and chunk version doesn't look reasonable until we go deep
into the details of the system. The paper says, "The lease mechanism is
designed to minimize management overhead at the master." But why and how? Let
me explain this in detail.

For the GFS, the most often write pattern is one client writes multiple times
on the same chunk, at least the paper thinks so. Thus, before the lease
expires, the client will contact the master only once. This reduces the load
on master dramatically. Even if multiple clients write to the same chunk, the
lease mechanism works better than without lease because the master only needs
to contact the replicas once.

Then comes the chunk versioning. You may ask why we need such strange
versioning strategy. The versioning strategy cannot guarantee the consistency,
of course. It is actually designate for the copy-on-write scheme. When the
master grants lease, they must make sure that the reference count is exactly
one. If not, it will let the replica copy the chunk before granting the lease.
Note that after this operation, some client may still know the old chunk
handle. This will cause the client to read the wrong chunk. With versioning,
the chunkserver can easily detect this problem and report this to the client,
and then the client will get the up-to-data information from the master.

## Metadata

The master stores three major types of metadata:

1. The file and chunk namespace (persistent);
2. The mapping from files to chunks (persistent);
3. The locations of each chunk's replicas (memory only).

## Features

- create
- delete
- read
- write
- [**snapshot**](#snapshot): create a copy of a file or a directory at a low
  cost
- [**record append**](#atomic-record-append): allows multiple clients to
  append data to the same file concurrently while guaranteeing the atomicity
  of each client's append

## Guarantees

### File Namespace Mutation

File namespace mutation is atomic with the help of locking as well as the
master's operation log.

### Data Mutation

Data mutation depends on the type of mutation, whether it succeeds or fails,
and whether there are concurrent mutations.

There are a few states of a file region:
- consistent: all replicas have the same data,
- defined: consistent and written entirely,
- inconsistent: inconsistent replicas.

Writes are defined in serial success and undefined but consistent in
concurrent success. Record appends are defined in serial success and
concurrent success.

## Leases and Mutation Order

The master grants a lease for each chunk to one of the replicas, which
is called the *primary*. The primary picks a serial order for all
mutations to the chunk.

## Data Flow

The data is pushed linearly along a chain of chunkservers. Every chunkserver
pushes the data to the closest server in the chain.

Without network congestion, the ideal elapsed time for transferring $B$ bytes
to $R$ replicas is $B/T+RL$ where $T$ is the network throughput and $L$ is the
latency to transfer bytes between two replicas.

## Atomic Record Append

Different from traditional append, the client only specifies the data. GFS
appends it to the file at least once atomically at an offset of GFS choosing
and returns the offset to the client.

The data to be appended must be less than the size of a chunk. In practice,
the data size is restricted to be at most one-fourth of the maximum chunk size
to keep worst-case fragmentation at an acceptable level.

If a record append fails at any replica, the client retries the operation. As
a result, the same record may be appended multiple times.

Please note that record appends may cause some padding regions in the chunk.
It is guaranteed that the data from the offset that returns to the client is
always defined, but the place where the primary attempts to write is undefined.

## Snapshot

GFS uses copy-on-write scheme to implement snapshot. When the master revokes
any outstanding leases on the chunks in the file it is about to snapshot.

After the leases have been revokes or have expired, the master logs the
operation to disk. Then it applies this record to its in-memory state by
duplicating the metadata.

The first time a client wants to write to a chunk C after the snapshot
operation, it sends a request to the master to find the current leaseholder.
The master found that the reference count for C is greater than one. So
it picks a new chunk handle C' and ask each chunkserver that has a current
replica of C to create a new chunk C' by duplicating C to avoid data
transmission through network. After that, the master replies to the client.

## Namespace Management and Locking

See the [file and chunk namespace section](master.md#namespace-management)
in the document for master.

## User-Specified Constants

- chunk size (64MiB recommended)
- number of replicas
- garbage collection interval
- *HeartBeat* interval
- lease duration

## Changes from the Vanilla GFS

### Directory Structure as a Tree

We use a tree structure to represent the directory structure. Each directory
has a map of directories and files. This means we should have the write lock
of the parent directory to create a new file or directory.

The reason why we make this change as opposed to the large flat namespace is
that
1. it is easier to implement;
2. this can avoid the busy race condition on the lock of the namespace map;
3. memory efficiency is no longer a important thing nowadays;
4. This provides more functions concerned with the directory structure.


### Lease Management

We do not log the lease operation.

The first reason is that, if we log all lease operation, there might be a
significant performance drop. Not logging these operation will avoid that.
This seems to be not fault-tolerant anymore, but we notice that a lease
will usually expire sooner than the master comes back to operation.

Another reason is about the chunk version. Whenever the master grant a
new lease to a chunkserver, it increments the version number. However,
if the master commits the operation log before letting the chunkservers
know the new version number, a fatal problem may occur.

Consider the following scenario:
A master grants a new lease to a chunkserver. It has committed the log first
and crashes before it is possible to let the chunkservers know the new version
number. Then, when the master recovers, it will consider the new version as
the version of the chunk, but none of these chunkservers know the new version
number. As a result, the master will inform the chunkservers to delete the
chunk.

I think the vanilla GFS uses a more complicated operation log to avoid this
problem by using transaction.
