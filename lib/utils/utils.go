package utils

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

func Keys[Key comparable, Value any](m *map[Key]Value) []Key {
	keys := make([]Key, len(*m))
	i := 0
	for key := range *m {
		keys[i] = key
		i++
	}
	return keys
}
