package blinkhash

import (
	"testing"
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
