package test

import (
	"math/rand"
	"strconv"
	"time"
)

type Key_t = int
type Value_t = int

func _Rdtsc() int {
	return int(time.Now().UnixNano())
}

func generateShuffledKeys(n int) []Key_t {
	keys := make([]Key_t, n)
	for i := 0; i < n; i++ {
		keys[i] = Key_t(i + 1)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return keys
}

func generateSerializedKeys(n int) []Key_t {
	keys := make([]Key_t, n)
	for i := 0; i < n; i++ {
		keys[i] = Key_t(i + 1)
	}
	return keys
}

func atoi(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return val
}
