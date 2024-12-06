package blinkhash

import (
	"encoding/binary"
	"github.com/cespare/xxhash/v2"
	"hash/fnv"
)

// HashFunction type for making an array of hash functions
type HashFunction func(data []byte, seed uint64) uint64

// Jenkins hash function in Go
func Jenkins(data []byte, seed uint64) uint64 {
	var hash uint64 = seed
	for _, b := range data {
		hash += uint64(b)
		hash += (hash << 10)
		hash ^= (hash >> 6)
	}
	hash += (hash << 3)
	hash ^= (hash >> 11)
	hash += (hash << 15)
	return hash
}

// MurmurHash2 in Go
func Murmur2(data []byte, seed uint64) uint64 {
	var (
		m uint32 = 0x5bd1e995
		r        = 24
		h uint32 = uint32(seed) ^ uint32(len(data))
	)
	dataLen := len(data)
	i := 0
	for ; dataLen >= 4; dataLen -= 4 {
		k := binary.LittleEndian.Uint32(data[i:])
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
		i += 4
	}

	switch dataLen {
	case 3:
		h ^= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[i])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return uint64(h)
}

// Xxhash in Go using the third-party library
func XxhashFunc(data []byte, seed uint64) uint64 {
	return xxhash.Sum64(data)
}

// Standard (FNV) hash function in Go
func Standard(data []byte, seed uint64) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64() ^ seed
}

// hashCompute 是一个示例函数，你可以根据需要实现更复杂的逻辑
func hashCompute(data []byte, length, seed uint64) uint64 {
	// 示例实现，实际需要根据 C++ 版本的 hash_compute 逻辑调整
	hash := seed
	for i := 0; i < len(data); i++ {
		hash ^= uint64(data[i])
		hash = (hash << 31) | (hash >> (64 - 31))
		hash *= 11400714785074694791 // NUMBER64_1
	}
	return hash
}

// 定义 Hash 函数数组
var hashFunctions = []HashFunction{
	Standard,   // 0
	Murmur2,    // 1
	Jenkins,    // 2
	XxhashFunc, // 3
}

// General hash function selector
func h(key interface{}, funcNum int, seed uint64) uint64 {
	var data []byte
	switch k := key.(type) {
	case int:
		data = make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(k))
	case string:
		data = []byte(k)
	// Add other key types as needed
	default:
		// Handle other types or panic
		panic("Unsupported key type for hashing")
	}
	if funcNum >= 0 && funcNum < len(hashFunctions) {
		return hashFunctions[funcNum](data, seed)
	}
	return 0
}
