package chunkserver

import (
	"container/heap"
	"math/rand"
)

// A queue for chunks in order to decide which chunk to be sent to the master

type chunkPriority struct {
	count  int64 // the number of times this chunk has been sent to the master
	random int64
	chunk  *Chunk
}

func makeChunkPriority(chunk *Chunk) *chunkPriority {
	return &chunkPriority{
		count:  0,
		random: rand.Int63(),
		chunk:  chunk,
	}
}

type chunkQueue []*chunkPriority

func makeChunkQueue(elements ...*chunkPriority) chunkQueue {
	queue := make(chunkQueue, len(elements))
	copy(queue, elements)
	heap.Init(&queue)
	return queue
}

func (queue chunkQueue) Len() int {
	return len(queue)
}

func (queue chunkQueue) Less(i, j int) bool {
	if queue[i].count != queue[j].count {
		return queue[i].count < queue[j].count
	} else {
		return queue[i].random < queue[j].random
	}
}

func (queue chunkQueue) Swap(i, j int) {
	queue[i], queue[j] = queue[j], queue[i]
}

func (queue *chunkQueue) Push(x any) {
	*queue = append(*queue, x.(*chunkPriority))
}

func (queue *chunkQueue) Pop() any {
	old := *queue
	n := len(old)
	x := old[n-1]
	*queue = old[0 : n-1]
	return x
}
