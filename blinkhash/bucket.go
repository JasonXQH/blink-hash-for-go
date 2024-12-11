package blinkhash

import (
	"fmt"
	"sync/atomic"
)

type State int

const (
	STABLE State = iota
	LINKED_LEFT
	LINKED_RIGHT
)

type Bucket struct {
	lock         uint32
	state        State
	fingerprints []uint8
	entries      []Entry
}

func NewBucket(entryNum int) *Bucket {
	return &Bucket{
		fingerprints: make([]uint8, entryNum),
		entries:      make([]Entry, entryNum),
	}
}

func (b *Bucket) TryLock() bool {
	version := atomic.LoadUint32(&b.lock)
	if b.isLocked(version) {
		return false
	}
	return atomic.CompareAndSwapUint32(&b.lock, version, version+0b10)
}

func (b *Bucket) Unlock() {
	atomic.AddUint32(&b.lock, 0b10)
}

func (b *Bucket) Insert(key, value interface{}) bool {
	for i := range b.entries {
		if b.entries[i].Key == nil { // assuming nil as empty
			b.entries[i].Key = key
			b.entries[i].Value = value
			return true
		}
	}
	return false
}

//-------------------------------------------
// 有Fingerprint版本的函数
//-------------------------------------------

func (b *Bucket) InsertWithFingerprint(key, value interface{}, fingerprint, empty uint8) bool {
	for i := range b.entries {
		// 检查fingerprints[i]是否等于empty以表示空闲槽位
		if b.fingerprints[i] == empty {
			b.fingerprints[i] = fingerprint
			b.entries[i].Key = key
			b.entries[i].Value = value
			return true
		}
	}
	return false
}

// Find 在没有Fingerprint的情况下查找
func (b *Bucket) Find(key interface{}) (interface{}, bool) {
	for i := range b.entries {
		if b.entries[i].Key == key {
			return b.entries[i].Value, true
		}
	}
	return nil, false
}

//-------------------------------------------
// 有Fingerprint版本的函数
//-------------------------------------------

// FindWithFingerprint 带Fingerprint的查找
func (b *Bucket) FindWithFingerprint(key interface{}, fingerprint uint8) (interface{}, bool) {
	for i := range b.entries {
		if b.fingerprints[i] == fingerprint && b.entries[i].Key == key {
			return b.entries[i].Value, true
		}
	}
	return nil, false
}

// Collect gathers entries from the bucket where the key is not considered empty and the key is greater than or equal to the given key.
func (b *Bucket) Collect(key interface{}) []Entry {
	var buf []Entry
	// 假设key为int
	for _, entry := range b.entries {
		if entry.Key != nil && entry.Key.(int) >= key.(int) {
			buf = append(buf, entry)
		}
	}
	return buf
}

//-------------------------------------------
// 有Fingerprint版本的函数
//-------------------------------------------

// CollectWithFingerprint 带Fingerprint的收集 >= key的entry
func (b *Bucket) CollectWithFingerprint(key interface{}, empty uint8) []Entry {
	var buf []Entry
	for i, e := range b.entries {
		if b.fingerprints[i] != empty && e.Key.(int) >= key.(int) {
			buf = append(buf, e)
		}
	}
	return buf
}

// CollectAll gathers all non-empty entries from the bucket.
func (b *Bucket) CollectAll() []Entry {
	var buf []Entry
	for _, entry := range b.entries {
		if entry.Key != nil {
			buf = append(buf, entry)
		}
	}
	return buf
}

//-------------------------------------------
// 有Fingerprint版本的函数
//-------------------------------------------

// CollectAllWithFingerprint 带Fingerprint收集所有非空entry
func (b *Bucket) CollectAllWithFingerprint(empty uint8) []Entry {
	var buf []Entry
	for i, e := range b.entries {
		if b.fingerprints[i] != empty {
			buf = append(buf, e)
		}
	}
	return buf
}

// Update updates the value for a given key if it exists in the bucket.
func (b *Bucket) Update(key, value interface{}) bool {
	for i := range b.entries {
		if compareIntKeys(b.entries[i].Key, key) == 0 {
			b.entries[i].Value = value
			return true
		}
	}
	return false
}

//-------------------------------------------
// 有Fingerprint版本的函数
//-------------------------------------------

// UpdateWithFingerprint 带Fingerprint的更新
func (b *Bucket) UpdateWithFingerprint(key, value interface{}, fingerprint uint8) bool {
	for i := range b.entries {
		if b.fingerprints[i] == fingerprint && b.entries[i].Key == key {
			b.entries[i].Value = value
			return true
		}
	}
	return false
}

// Remove 从桶中移除指定键的条目
func (b *Bucket) Remove(key interface{}) bool {
	for i := range b.entries {
		if compareIntKeys(b.entries[i].Key, key) == 0 {
			b.entries[i].Key = nil // 假设 nil 表示空
			return true
		}
	}
	return false
}

//-------------------------------------------
// 有Fingerprint版本的函数
//-------------------------------------------

// RemoveWithFingerprint 带Fingerprint的移除
func (b *Bucket) RemoveWithFingerprint(key interface{}, fingerprint uint8) bool {
	for i := range b.entries {
		if b.fingerprints[i] == fingerprint && b.entries[i].Key == key {
			b.fingerprints[i] = 0
			b.entries[i].Key = nil
			b.entries[i].Value = nil
			return true
		}
	}
	return false
}

// CollectKeys collects keys up to a specified cardinality and returns true if it collects exactly the cardinality.
func (b *Bucket) CollectKeys(cardinality int) ([]interface{}, bool) {
	keys := make([]interface{}, 0, cardinality)
	for _, entry := range b.entries {
		if !IsEmptyKey(entry.Key) {
			keys = append(keys, entry.Key)
			if len(keys) == cardinality {
				return keys, true
			}
		}
	}
	return keys, false
}

//-------------------------------------------
// 有Fingerprint版本的函数
//-------------------------------------------

// CollectKeysWithFingerprint 带Fingerprint收集最多cardinality个key
func (b *Bucket) CollectKeysWithFingerprint(cardinality int, empty uint8) ([]interface{}, bool) {
	keys := make([]interface{}, 0, cardinality)
	for i, e := range b.entries {
		if b.fingerprints[i] != empty {
			keys = append(keys, e.Key)
			if len(keys) == cardinality {
				return keys, true
			}
		}
	}
	return keys, false
}

// CollectAllKeys collects all keys that are not empty.
func (b *Bucket) CollectAllKeys() []interface{} {
	keys := make([]interface{}, 0, len(b.entries))
	for _, entry := range b.entries {
		if !IsEmptyKey(entry.Key) {
			keys = append(keys, entry.Key)
		}
	}
	return keys
}

//-------------------------------------------
// 有Fingerprint版本的函数
//-------------------------------------------

// CollectAllKeysWithFingerprint 带Fingerprint收集所有非空key
func (b *Bucket) CollectAllKeysWithFingerprint(empty uint8) []interface{} {
	keys := make([]interface{}, 0, len(b.entries))
	for i, entry := range b.entries {
		if b.fingerprints[i] != empty {
			keys = append(keys, entry.Key)
		}
	}
	return keys
}

// Footprint calculates the memory usage of keys and fingerprints.
func (b *Bucket) Footprint(metrics *FootprintMetrics) {
	// meta最初存锁和状态的大小(假设8字节锁)
	metrics.Meta += 8
	if LINKED {
		// 假设state是int型，增加相应内存统计
		// 具体根据您的实际State类型大小来定，这里假设8字节
		metrics.Meta += 8
	}

	for i := 0; i < len(b.entries); i++ {
		if FINGERPRINT {
			// 按原C++逻辑:
			// if((fingerprints[i] & 0b1) == 0b1)表示occupied
			if b.fingerprints[i]&0b1 == 0b1 {
				metrics.StructuralDataOccupied += 1 // fingerprint占1字节
				// 假设每个entry 16字节 (可根据实际键值大小调整)
				metrics.KeyDataOccupied += 16
			} else {
				metrics.StructuralDataUnoccupied += 1
				metrics.KeyDataUnoccupied += 16
			}
		} else {
			// 无fingerprint时，根据key是否为空
			if !IsEmptyKey(b.entries[i].Key) {
				metrics.KeyDataOccupied += 16
			} else {
				metrics.KeyDataUnoccupied += 16
			}
		}
	}
	return
}

// IsEmptyKey checks if the key is considered empty.
func IsEmptyKey(key interface{}) bool {
	return key == nil
}

func (b *Bucket) Print() {
	// 打印 Bucket 的基本信息
	fmt.Printf("\tLock: %d\n", b.lock)
	fmt.Printf("\tState: %v\n", b.state) // 假设 State 类型可以直接打印，或者需要格式化

	// 打印 fingerprints 切片
	fmt.Printf("\tFingerprints: ")
	if len(b.fingerprints) == 0 {
		fmt.Println("nil")
	} else {
		for i, fingerprint := range b.fingerprints {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%d", fingerprint)
		}
		fmt.Println()
	}

	// 打印 entries 切片
	fmt.Printf("\tEntries: \n")
	if len(b.entries) == 0 {
		fmt.Println("nil")
	} else {
		for i, entry := range b.entries {
			// 假设 Entry 类型有 Key 和 Value 字段
			fmt.Printf("\tEntry %d: Key = %v, Value = %v\n", i, entry.Key, entry.Value)
		}
	}
}

func (b *Bucket) isLocked(version uint32) bool {
	return version&0b10 == 0b10
}

func (b *Bucket) upgradeLock(version uint32) bool {
	for {
		currentVersion := atomic.LoadUint32(&b.lock)
		if currentVersion != version || b.isLocked(currentVersion) {
			return false
		}
		if atomic.CompareAndSwapUint32(&b.lock, currentVersion, currentVersion+0b10) {
			return true
		}
		// Optionally add runtime.Gosched() here to yield the processor, preventing a tight spin loop
	}
}

func (b *Bucket) unlock() {
	atomic.AddUint32(&b.lock, 0b10)
}

func (b *Bucket) getVersion() (version uint32, needRestart bool) {
	version = atomic.LoadUint32(&b.lock)
	needRestart = b.isLocked(version)
	return
}
