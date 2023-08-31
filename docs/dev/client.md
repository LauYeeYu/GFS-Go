# Client

## Data

No need to cache data.

## Operations

- [create](#create)
- [delete](#delete)
- [read](#read)
- [write](#write)
- [snapshot](#snapshot)
- [record append](#record-append)

### Create

The client send a create request with the namespace and file name (including
the absolute file directory) to the master. The master will then create a new
file and return whether the creation is successful or not.

### Delete

The client send a delete request with the namespace and file name (including
the absolute file directory) to the master. The master will then delete the
file and return whether the deletion is successful or not.

### Read

1. Check whether the information of a chunk is cached and not outdated. Also
   check whether the primary is reachable. If everything is fine, go to step 4.
2. Ask master for the locations of the replicas and which chunkserver holds
   the current lease for the chunk.
3. Cache the data that master has replied for future mutation.
4. Send a read request to any one of the replicas.

### Write

1. Check whether the information of a chunk is cached and not outdated. Also
   check whether the primary is reachable. If everything is fine, go to step 4.
2. Ask master for the locations of the replicas and which chunkserver holds
   the current lease for the chunk.
3. Cache the data that master has replied for future mutation.
4. Push the data to the closest replica.
5. Send a write request to the primary.
6. Receive a reply from the primary. If an error has occurred, retry from 4.

### Snapshot

The client send a snapshot request to the master. The master will then create
a snapshot and return whether the creation is successful or not.

### Record Append

1. Ask master for the locations of the replicas for the last chunk of a file
   and which chunkserver holds the current lease for that chunk.
2. Cache the data that master has replied for future mutation.
3. Push the data to the closest replica.
4. Send a record append request to the primary.
5. Receive a reply from the primary. If an error has occurred, retry from 3.
