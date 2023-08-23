package utils

import "container/list"

type UnlimitedBufferedChannel[T any] struct {
	In     chan<- T
	Out    <-chan T
	buffer list.List
}

func MakeUnlimitedBufferedChannel[T any](initBufferSize int) UnlimitedBufferedChannel[T] {
	in := make(chan T, initBufferSize)
	out := make(chan T, initBufferSize)
	channel := UnlimitedBufferedChannel[T]{
		In:     in,
		Out:    out,
		buffer: list.List{},
	}
	channel.buffer.Init()
	go func() {
	loop:
		for {
			val, ok := <-in
			if !ok { // If In has been closed, exit loop
				break loop
			}
			// out is not full
			select {
			case out <- val:
				continue
			default:
			}
			// out is full, put val into buffer
			channel.buffer.PushBack(val)
			// In this case, out is full
			for channel.buffer.Len() > 0 {
				select {
				case val, ok := <-in:
					if !ok { // If In has been closed, exit loop
						break loop
					}
					channel.buffer.PushBack(val)
				case out <- channel.buffer.Front().Value.(T):
					channel.buffer.Remove(channel.buffer.Front())
				}
			}
		}
		for channel.buffer.Len() > 0 {
			out <- channel.buffer.Front().Value.(T)
			channel.buffer.Remove(channel.buffer.Front())
		}
		close(out)
	}()
	return channel
}

func (channel *UnlimitedBufferedChannel[T]) Close() {
	close(channel.In)
}
