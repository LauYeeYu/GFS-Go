package utils

type Set[T comparable] map[T]struct{}

func MakeSet[T comparable](elements ...T) Set[T] {
	var set Set[T] = make(map[T]struct{})
	for _, element := range elements {
		set.Add(element)
	}
	return set
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
