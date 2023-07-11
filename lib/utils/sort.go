package utils

import "sort"

type Sorter[T any] struct {
	slice   []T
	compare func(T, T) bool
}

func (sorter Sorter[T]) Len() int {
	return len(sorter.slice)
}

func (sorter Sorter[T]) Less(i, j int) bool {
	return sorter.compare(sorter.slice[i], sorter.slice[j])
}

func (sorter Sorter[T]) Swap(i, j int) {
	sorter.slice[i], sorter.slice[j] = sorter.slice[j], sorter.slice[i]
}

func Sort[T any](slice []T, compare func(T, T) bool) {
	sorter := Sorter[T]{slice, compare}
	sort.Sort(sorter)
}

func SortByDefault[T Ordered](slice []T) {
	Sort[T](slice, func(a, b T) bool {
		return a < b
	})
}
