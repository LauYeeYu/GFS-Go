# Chunkserver

Chunkserver stores the chunks.

## Data

No need to cache data. The page cache is enough.

## *HeartBeat*

The chunkserver send a *HeartBeat* to master periodically to give it
a subset of the chunks. The master will reply with the identity of
orphan chunks.

## Lease

The master may grant a lease to the chunkserver or revoke it. If the
chunkserver holds a lease, it is the primary replica for the chunk.

The lease can be persisted in a file.

## Rejoin

When the chunkserver is able to contact the master after a failure, it
needs to set the clock.

## Attempt to Write

The client might send some data that is used for write operation. The
chunkserver will store the data in an internal LRU buffer cache until
the data is used or aged out.

Meanwhile, the chunkserver also forward the data the closest replica.

## Client Request

### Write Request

#### Primary

The client sends a write request to the primary once all the replicas
have acknowledged receiving the data. The request identifies the data pushed
earlier.

Then the primary assigns consecutive serial numbers to all the mutations.
It applies the mutations to its own local state in serial number order.

Then the primary forwards the write request to all secondary replicas with
the serial numbers assigned by the primary.

The primary replies to the client. Any error encountered at any of the
replicas are reported to the client.

#### Secondary

When a secondary server receives a write request, it applies the mutations
to its own local state in serial number order and reply to the primary
indicating that they have completed the operation.

### Record Append Request

The client sends a record append request to the primary once all the
replicas have acknowledged receiving the data. The request identifies
the data pushed earlier.

Then the primary checks whether appending the record to the current chunk
would cause the chunk to exceed the maximum chunk size. If so, the primary
pads the current chunk to the maximum chunk size, and tell secondaries to
do the same, and replies to the client indicating that the operation should
be retired on the next chunk.

If not, the primary forwards the write request to all secondary replicas
with the offset it defined.

## Chunkserver Request

### Padding the Chunk

When the primary receives a record append request, it will send a padding
request to all the secondaries if the appending will cause the chunk to
exceed the maximum chunk size.

In this case, the chunkserver should pad the chunk to the maximum chunk.

## Master Request

### Copy Chunk Locally

When the client attempts to write a chunk with reference count greater than 1,
the master will send request to all chunkservers that has a current replica
of that chunk. The chunkservers should copy the chunk to a new one with a
name specified by the master.

### Copy Chunk Remotely

When the master tried to re-replicate a chunk, it will send request to the
chunkserver. The request contains the name of the chunk to be copied and
the valid replicas of the chunk. The chunkserver should get the chunk from
one of these replicas and store it. After that, the chunkserver replies to
the master.
