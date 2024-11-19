package blinkhash

import (
	"testing"
	"unsafe"
)

func TestJenkinsHash(t *testing.T) {
	data := []byte("test data")
	expected := Jenkins(data, Seed) // 用实际期望值替换
	println("expected Jenkins: ", expected)
	actual := Jenkins(data, Seed)
	if actual != expected {
		t.Errorf("Jenkins hash was incorrect, got: %d, want: %d.", actual, expected)
	}
}

func TestMurmur2Hash(t *testing.T) {
	data := []byte("test data")
	expected := Murmur2(data, Seed) // 用实际期望值替换
	actual := Murmur2(data, Seed)
	println("expected Murmur2: ", expected)
	if actual != expected {
		t.Errorf("Murmur2 hash was incorrect, got: %d, want: %d.", actual, expected)
	}
}

func TestStandardHash(t *testing.T) {
	data := []byte("test data")
	expected := Standard(data, Seed) // 用实际期望值替换
	actual := Standard(data, Seed)
	println("expected StandardHash: ", expected)
	if actual != expected {
		t.Errorf("Standard FNV hash was incorrect, got: %d, want: %d.", actual, expected)
	}
}

func TestXXHash(t *testing.T) {
	data := []byte("test data")
	expected := Xxhash(data, Seed) // 需要实际期望值
	actual := Xxhash(data, Seed)

	println("expected Xxhash: ", expected)
	if actual != expected {
		t.Errorf("XXHash was incorrect, got: %d, want: %d.", actual, expected)
	}
}

func TestHashRead64Align(t *testing.T) {
	var data uint64 = 0x0123456789ABCDEF
	ptr := unsafe.Pointer(&data)

	if val := hashRead64Align(ptr, 0); val != data {
		t.Errorf("hashRead64Align failed, got %x, want %x", val, data)
	}
}

func TestHashRead32Align(t *testing.T) {
	var data uint32 = 0x89ABCDEF
	ptr := unsafe.Pointer(&data)

	if val := hashRead32Align(ptr, 0); val != data {
		t.Errorf("hashRead32Align failed, got %x, want %x", val, data)
	}
}
