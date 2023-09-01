# Explaining the Google File System (GFS)

## About the Google File System

The Google File System (GFS) is a distributed file system that features
scalability, consistency, reliability, and high performance. It was
designed to store and access data concurrently. It provides usual operations
to create, delete, open, close, read, and write files. And it also supports
some special operations such as atomic record append and snapshot.

## Separating the Data Plane from the Control Plane

A distributed system always has a big problem - the connection between servers
are not reliable. To solve this problem, a good way is to set up a leader.
However, this will make the leader a bottleneck because the leader will usually
receive more request than others. On top of that, if the leader crashes, the
system will fail, which actually shadows the reliability provided by
distributed systems.

The GFS solves this problem by separating the control plane from the data
plane. This will dramatically shrink the amount of pressure on the master.
The master only needs to handle the metadata, which is much smaller than the
data. This is based on the fact that the metadata has nothing to do with the
data.

## Chunks

To store the files, GFS divides the files into chunks. Each chunk is of fixed
maximum size. A file should have a couple of full chunks and a chunk that
is not of full size. Chunk makes it easy to manage the data. Keeping the file
as a whole will make writing very difficult if there are too many clients
writing to the same file. The idea of chunk avoid that.

The size of chunk should be not too large or too small. If it is too large,
too many write requests will fall down on several servers, which will cause
the servers to be overloaded. If it is too small, a file will have too many
chunks and thus causing the master to be overloaded.

## Special Interfaces

### Atomic Record Append

GFS provides an atomic write operation, which is called atomic record append.
A normal concurrent write operation will cause undefined behavior because
it is impossible to determine the order of the write operations. Therefore,
GFS introduces the atomic record append operation to solve this problem.

In atomic record append, the client do not need to specify the offset. It only
needs to send the data to the master, then the primary will append the data to
the end of the chunk if the chunk doesn't exceed the maximum size. If this
operation is successfully done, the chunkserver will send the offset of the
data to the client. The client can use this offset to read the data.

### Snapshot

Snapshot is a special operation that allows the client to copy the files at
low cost. It is implemented by copy-on-write. When a snapshot is created, the
master will add the reference count of the chunk. When a chunk is to be
modified, the master will first copy the chunk and then let chunkservers to
modify the copy.

## Lease

Lease is a mechanism that reduce the pressure on the master. Normally, without
a lease, if the master receives a write request, it will first contact the
chunkservers, and redirect the client to the primary. In most cases, the write
operation on the same chunk is from several certain client. Therefore, with the
help of lease, the master will only contact the chunkservers once during the
lease period. This will reduce the pressure on the master.

The lease can be granted only when the chunk is not shared through files.
Therefore, if the master receives a snapshot request, it will first revoke the
lease of the chunk, and add the reference count of the chunk. When the master
receives a write request, it will first check the reference count of the chunk.
If the reference count is greater than 1, it will first copy the chunk and then
grant the lease.

## Consistency Model

The consistency model of GFS is really weak. It only guarantees the consistency
if the write operations are successful. Any error will cause the file
inconsistent. But GFS provides a measure to mitigate this problem. It
recommends the client to use record append to write data and use checksum to
check the data. If the checksum is not correct, the client will move on to the
next part of the file.

It is really normal to see a chunk has different data through replicas. Any
version of that chunk is valid. GFS leaves a lot of stuff to the client and
its applications.
