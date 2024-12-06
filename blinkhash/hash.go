package blinkhash

import (
	"encoding/binary"
	"hash/fnv"
	"unsafe"
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

func Xxhash(data []byte, seed uint64) uint64 {
	if uintptr(unsafe.Pointer(&data[0]))&7 == 0 {
		return hashCompute(data, uint64(len(data)), seed)
	}
	return hashCompute(data, uint64(len(data)), seed)
}

func Standard(data []byte, seed uint64) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64() ^ seed
}

func hashRead64Align(p unsafe.Pointer, align uint32) uint64 {
	if align == 0 {
		return *(*uint64)(p)
	}
	return binary.LittleEndian.Uint64((*[8]byte)(p)[:])
}

func hashRead32Align(p unsafe.Pointer, align uint32) uint32 {
	if align == 0 {
		return *(*uint32)(p)
	}
	return binary.LittleEndian.Uint32((*[4]byte)(p)[:])
}

func hashCompute(data []byte, length, seed uint64) uint64 {
	var hash uint64

	// 示例实现，实际的转换可能需要根据函数逻辑精确调整
	for i := 0; i+8 <= len(data); i += 8 {
		val := binary.LittleEndian.Uint64(data[i : i+8])
		hash ^= val
		hash = (hash << 31) | (hash >> (64 - 31))
		hash *= Number64_1
	}

	return hash
}

var hashFunctions = []HashFunction{
	Standard, // This should be replaced by an actual implementation
	Murmur2,
	Jenkins,
	Xxhash, // Assume xxhash has been implemented
}

// General hash function selector
func h(data []byte, funcNum int, seed uint64) uint64 {
	if funcNum >= 0 && funcNum < len(hashFunctions) {
		return hashFunctions[funcNum](data, seed)
	}
	return 0
}
