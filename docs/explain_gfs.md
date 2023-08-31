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
