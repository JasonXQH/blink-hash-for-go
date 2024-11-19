package blinkhash

import "sync/atomic"

type State int

const (
	Stable State = iota
	LinkedLeft
	LinkedRight
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
	if version&0b10 == 0b10 { // is locked
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

func (b *Bucket) Find(key interface{}) (interface{}, bool) {
	for _, entry := range b.entries {
		if entry.Key == key {
			return entry.Value, true
		}
	}
	return nil, false
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

// Collect gathers entries from the bucket where the key is not considered empty and the key is greater than or equal to the given key.
func (b *Bucket) Collect(key interface{}, buf *[]Entry) {
	for _, entry := range b.entries {
		if entry.Key != nil && entry.Key.(int) >= key.(int) { // Assuming keys are of type int for comparison
			*buf = append(*buf, entry)
		}
	}
}

// CollectAll gathers all non-empty entries from the bucket.
func (b *Bucket) CollectAll(buf *[]Entry) {
	for _, entry := range b.entries {
		if entry.Key != nil {
			*buf = append(*buf, entry)
		}
	}
}

// Update updates the value for a given key if it exists in the bucket.
func (b *Bucket) Update(key, value interface{}) bool {
	for i := range b.entries {
		if b.entries[i].Key == key {
			b.entries[i].Value = value
			return true
		}
	}
	return false
}

// Remove removes an entry with the specified key from the bucket.
func (b *Bucket) Remove(key interface{}) bool {
	for i := range b.entries {
		if b.entries[i].Key == key {
			b.entries[i].Key = nil // Assuming nil represents EMPTY<Key_t>
			return true
		}
	}
	return false
}

// CollectKeys collects keys up to a specified cardinality and returns true if it collects exactly the cardinality.
func (b *Bucket) CollectKeys(keys *[]interface{}, cardinality int) bool {
	for _, entry := range b.entries {
		if !IsEmptyKey(entry.Key) {
			*keys = append(*keys, entry.Key)
			if len(*keys) == cardinality {
				return true
			}
		}
	}
	return false
}

// CollectAllKeys collects all keys that are not empty.
func (b *Bucket) CollectAllKeys(keys *[]interface{}) {
	for _, entry := range b.entries {
		if !IsEmptyKey(entry.Key) {
			*keys = append(*keys, entry.Key)
		}
	}
}

// Footprint calculates the memory usage of keys and fingerprints.
func (b *Bucket) Footprint() (meta, structuralDataOccupied, structuralDataUnoccupied, keyDataOccupied, keyDataUnoccupied uint64) {
	meta += uint64(len(b.entries) * 8) // assuming 64-bit system
	for _, entry := range b.entries {
		if entry.Key != nil { // Using nil to represent EMPTY<Key_t>
			keyDataOccupied += uint64(16) // Assuming each key-value pair takes up 16 bytes
		} else {
			keyDataUnoccupied += uint64(16)
		}
	}
	return
}

// IsEmptyKey checks if the key is considered empty.
func IsEmptyKey(key interface{}) bool {
	return key == nil
}