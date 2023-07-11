package utils

import "container/list"

type Queue[T any] struct {
	list list.List
}

func (queue *Queue[T]) Push(value T) {
	queue.list.PushBack(value)
}

func (queue *Queue[T]) Pop() T {
	element := queue.list.Front()
	queue.list.Remove(element)
	return element.Value.(T)
}

func (queue *Queue[T]) Front() T {
	element := queue.list.Front()
	return element.Value.(T)
}

func (queue *Queue[T]) Len() int {
	return queue.list.Len()
}

func (queue *Queue[T]) Empty() bool {
	return queue.list.Len() == 0
}
