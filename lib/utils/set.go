package utils

type Set[T comparable] map[T]struct{}

func MakeSet[T comparable]() Set[T] {
	return make(Set[T])
}

func (set Set[T]) Add(value T) {
	set[value] = struct{}{}
}

func (set Set[T]) Remove(value T) {
	delete(set, value)
}

func (set Set[T]) Contains(value T) bool {
	_, ok := set[value]
	return ok
}

func (set Set[T]) Size() int {
	return len(set)
}
