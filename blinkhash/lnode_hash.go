package blinkhash

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"unsafe"
)

type LNodeHash struct {
	Node
	Type           NodeType
	Cardinality    int
	HighKey        interface{}
	Buckets        []Bucket
	LeftSiblingPtr NodeInterface
}

// NewLNodeHash
//
//	@Description: LNodeHash构造函数
//	@param level
//	@return *LNodeHash
//

func NewLNodeHash(level int) *LNodeHash {
	cardinality := (LeafHashSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Bucket{}))
	return &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       level,
		},
		Type:           HashNode,
		HighKey:        nil, // 需要在 Split 中设置
		Cardinality:    cardinality,
		Buckets:        make([]Bucket, 0, LeafHashSize),
		LeftSiblingPtr: nil,
	}
}

// NewLNodeHashWithSibling 创建一个新的 LNodeHash 节点，并设置兄弟节点、计数和层级
func NewLNodeHashWithSibling(sibling NodeInterface, count, level int) *LNodeHash {
	cardinality := (LeafHashSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Bucket{}))
	newHashNode := &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  sibling,
			leftmostPtr: nil,
			count:       count,
			level:       level,
		},
		Type:           HashNode,
		HighKey:        nil, // 需要在 Split 中设置
		Cardinality:    cardinality,
		Buckets:        make([]Bucket, count, LeafHashSize),
		LeftSiblingPtr: nil,
	}
	// 初始化newRight的buckets
	for i := range newHashNode.Buckets {
		newHashNode.Buckets[i].entries = make([]Entry, EntryNum)
	}
	return newHashNode
}
func (lh *LNodeHash) GetHighKey() interface{} {
	return lh.HighKey
}

// Print
//
//	@Description:函数打印 HashNode 信息
//	@receiver lh
func (lh *LNodeHash) Print() {
	fmt.Printf("LNodeHash Information:\n")
	fmt.Printf("Type: %v\n", lh.Type)
	fmt.Printf("HighKey: %v\n", lh.HighKey)
	fmt.Printf("Cardinality: %d\n", lh.Cardinality)
	lh.Node.Print()
	fmt.Printf("Buckets:\n")
	for i, bucket := range lh.Buckets {
		fmt.Printf("\tBucket %d information: \n", i)
		bucket.Print()
	}
	// 打印 LeftSiblingPtr 信息
	if lh.LeftSiblingPtr != nil {
		fmt.Printf("Left Sibling Pointer: %p\n", lh.LeftSiblingPtr)
	} else {
		fmt.Println("Left Sibling Pointer: nil")
	}
}

func (lh *LNodeHash) SanityCheck(_highKey interface{}, first bool) {
	sibling := lh.siblingPtr
	if sibling != nil {
		sibling.SanityCheck(_highKey, first)
	}
}

// TrySplitLock
//
//	@Description: 尝试分裂锁定，等同于 C++ 的 try_splitlock
//	@receiver lh
//	@param version
//	@return bool
func (lh *LNodeHash) TrySplitLock(version uint64) bool {
	success, needRestart := lh.TryUpgradeWriteLock(version)
	if needRestart || !success {
		return false
	}

	for i := 0; i < lh.Cardinality; i++ {
		for !lh.Buckets[i].TryLock() {
			runtime.Gosched() // 防止紧密循环
		}
	}
	return true
}

// TryConvertLock 尝试转换锁定，等同于 C++ 的 try_convertlock
func (lh *LNodeHash) TryConvertLock(version uint64) bool {
	success, needRestart := lh.TryUpgradeWriteLock(version)
	if needRestart || !success {
		return false
	}

	for i := 0; i < lh.Cardinality; i++ {
		for !lh.Buckets[i].TryLock() {
			runtime.Gosched()
		}
	}
	return true
}

// SplitUnlock 释放分裂时的锁，等同于 C++ 的 split_unlock
func (lh *LNodeHash) SplitUnlock() {
	lh.WriteUnlock()
	for i := 0; i < lh.Cardinality; i++ {
		lh.Buckets[i].Unlock()
	}
}

// SplitUnlockObsolete 以过时方式释放分裂锁，等同于 C++ 的 split_unlock_obsolete
func (lh *LNodeHash) SplitUnlockObsolete() {
	lh.WriteUnlockObsolete()
	for i := 0; i < lh.Cardinality; i++ {
		lh.Buckets[i].Unlock()
	}
}

// TryWriteLock 尝试获取写锁，等同于 C++ 的 try_writelock
func (lh *LNodeHash) TryWriteLock() bool {
	return lh.Node.TryWriteLock()
}

// ConvertUnlock 释放转换锁定，等同于 C++ 的 convert_unlock
func (lh *LNodeHash) ConvertUnlock() {
	lh.WriteUnlock()
	for i := 0; i < lh.Cardinality; i++ {
		lh.Buckets[i].Unlock()
	}
}

// ConvertUnlockObsolete 以过时方式释放转换锁定，等同于 C++ 的 convert_unlock_obsolete
func (lh *LNodeHash) ConvertUnlockObsolete() {
	lh.Node.WriteUnlockObsolete()
}

// Hash 对键进行哈希运算，等同于 C++ 的 _hash 方法
func (lh *LNodeHash) Hash(key interface{}) uint8 {
	keyInt, ok := key.(uint64)
	if !ok {
		panic("Hash: key is not of type int")
	}
	return uint8(keyInt % 256)
}

// Insert 实现 Insertable 接口
// @Description: 实现 Insertable 接口的插入方法
// @receiver lh
// @param key
// @param value
// @param version
// @return int

func (lh *LNodeHash) Insert(key interface{}, value interface{}, version uint64) int {
	//fmt.Println("我是LNodeHash，调用Insert")

	// 根据 FINGERPRINT 设置初始化 empty
	for k := 0; k < HashFuncsNum; k++ {
		hashKey := h(key, k, 0) // 使用默认 seed

		var fingerprint uint8
		if FINGERPRINT {
			fingerprint = lh.Hash(hashKey) | 1
		}

		for j := 0; j < NumSlot; j++ {
			loc := int((hashKey + uint64(j)) % uint64(lh.Cardinality))
			if !lh.Buckets[loc].TryLock() {
				return NeedRestart // 返回 -1
			}

			// 获取节点的当前版本
			currentVersion, needRestart := lh.GetVersion()
			if needRestart || version != currentVersion {
				lh.Buckets[loc].Unlock()
				return NeedRestart // 返回 -1
			}

			// 如果启用了 LINKED
			if LINKED && lh.Buckets[loc].state != STABLE {
				if !lh.StabilizeBucket(loc) {
					lh.Buckets[loc].Unlock()
					return NeedRestart
				}
			}

			// 尝试在槽位中插入
			success := false
			if FINGERPRINT {
				success = lh.Buckets[loc].InsertWithFingerprint(key, value, fingerprint, EmptyFingerprint)
			} else {
				success = lh.Buckets[loc].Insert(key, value)
			}

			if success {
				lh.Buckets[loc].Unlock()
				return InsertSuccess // 返回 0
			}

			// 插入失败，解锁并继续
			lh.Buckets[loc].Unlock()
		}
	}

	// 如果所有哈希函数和槽位都尝试失败，返回 NeedSplit
	return NeedSplit // 返回 1
}

// Split
//
//	@Description: 实现Splittable接口，分裂节点
//	@receiver b
//	@param key
//	@param value
//	@param version
//	@return Splittable
//	@return interface{}
//
// 分裂函数
func (lh *LNodeHash) Split(key interface{}, value interface{}, version uint64) (Splittable, interface{}) {

	newRight := NewLNodeHashWithSibling(lh.siblingPtr, lh.count, lh.level)
	// 初始化newRight的buckets
	newRight.HighKey = lh.HighKey
	newRight.LeftSiblingPtr = lh
	// target结构存储hash位置信息
	type targetT struct {
		loc         uint64
		fingerprint uint64
	}

	var targets [HashFuncsNum]targetT
	for k := 0; k < HashFuncsNum; k++ {
		hv := h(key, 0, uint64(k))
		loc := hv % uint64(lh.Cardinality)
		fp := lh.Hash(hv) | 1
		targets[k] = targetT{loc: loc, fingerprint: uint64(fp)}
	}

	// 如果LINKED启用，这里做stabilize_all检查（假设成功）
	if LINKED {
		if !lh.StabilizeAll(version) {
			return nil, nil
		}
	}
	// 尝试上分裂锁
	if !lh.TrySplitLock(version) {
		return nil, nil
	}

	// 收集keys用于找到splitKey
	temp := make([]interface{}, 0, lh.Cardinality*EntryNum)
	if FINGERPRINT {
		// 收集所有有fingerprint的key
		for i := 0; i < lh.Cardinality; i++ {
			temp = append(temp, lh.Buckets[i].CollectAllKeysWithFingerprint(EmptyFingerprint)...)
		}
	} else {
		for i := 0; i < lh.Cardinality; i++ {
			temp = append(temp, lh.Buckets[i].CollectAllKeys()...)
		}
	}

	// 找中值key作为splitKey
	medianIndex := lh.findMedian(temp)
	medianKey := temp[medianIndex]
	splitKey := medianKey
	lh.HighKey = medianKey

	// 迁移keys到newRight
	if FINGERPRINT {
		// fingerprint版本
		for j := 0; j < lh.Cardinality; j++ {
			for i := 0; i < EntryNum; i++ {
				if lh.Buckets[j].fingerprints != nil && lh.Buckets[j].fingerprints[i] != 0 {
					// slot occupied
					if lh.Buckets[j].entries[i].Key.(int) > medianKey.(int) {
						// migrate to newRight
						newRight.Buckets[j].entries[i] = lh.Buckets[j].entries[i]
						newRight.Buckets[j].fingerprints[i] = lh.Buckets[j].fingerprints[i]
						lh.Buckets[j].fingerprints[i] = 0
						lh.Buckets[j].entries[i] = Entry{Key: nil, Value: nil}
					}
				}
			}
		}
	} else {
		// baseline无fingerprint版本
		for j := 0; j < lh.Cardinality; j++ {
			for i := 0; i < EntryNum; i++ {
				e := lh.Buckets[j].entries[i]
				if !IsEmptyKey(e.Key) && e.Key.(int) > medianKey.(int) {
					newRight.Buckets[j].entries[i] = e
					lh.Buckets[j].entries[i] = Entry{Key: nil, Value: nil}
				}
			}
		}
	}

	// 在分裂后插入新key
	var targetNode = lh
	if key.(int) > medianKey.(int) {
		targetNode = newRight
	}

	needInsert := true

InsertLoop:
	for m := 0; m < HashFuncsNum && needInsert; m++ {
		for s := 0; s < NumSlot && needInsert; s++ {
			loc := (targets[m].loc + uint64(s)) % uint64(lh.Cardinality)
			if FINGERPRINT {
				// 有fingerprint的插入逻辑
				// 我们需要根据fingerprint判断空位: fingerprint[i] == 0表示空位
				// 如果LINKED为true，需要考虑"eager stabilization"逻辑
				if LINKED {
					// LINKED + FINGERPRINT逻辑
					// 遍历entry_num
					for i := 0; i < EntryNum && needInsert; i++ {
						fpOld := targetNode.Buckets[loc].fingerprints[i]
						if fpOld != 0 {
							// slot occupied,检查是否需要迁移
							entryKey := targetNode.Buckets[loc].entries[i].Key
							if entryKey.(int) > medianKey.(int) && targetNode == lh {
								// 需要迁移到newRight
								newRight.Buckets[loc].entries[i] = targetNode.Buckets[loc].entries[i]
								newRight.Buckets[loc].fingerprints[i] = fpOld
								if needInsert {
									// 在当前节点插入？
									if key.(int) <= medianKey.(int) {
										targetNode.Buckets[loc].fingerprints[i] = uint8(targets[m].fingerprint)
										targetNode.Buckets[loc].entries[i].Key = key
										targetNode.Buckets[loc].entries[i].Value = value
										needInsert = false
									} else {
										// 重置迁移key的fingerprint
										targetNode.Buckets[loc].fingerprints[i] = 0
									}
								} else {
									targetNode.Buckets[loc].fingerprints[i] = 0
								}
							} else {
								// slot不需要迁移
								if needInsert {
									if medianKey.(int) < key.(int) && targetNode == lh {
										// 插入到newRight
										newRight.Buckets[loc].fingerprints[i] = uint8(targets[m].fingerprint)
										newRight.Buckets[loc].entries[i].Key = key
										newRight.Buckets[loc].entries[i].Value = value
										needInsert = false
									}
								}
							}
						} else {
							// empty slot
							if needInsert {
								if medianKey.(int) < key.(int) && targetNode == lh {
									// 插入到newRight
									newRight.Buckets[loc].fingerprints[i] = uint8(targets[m].fingerprint)
									newRight.Buckets[loc].entries[i].Key = key
									newRight.Buckets[loc].entries[i].Value = value
								} else {
									// 插入到当前
									targetNode.Buckets[loc].fingerprints[i] = uint8(targets[m].fingerprint)
									targetNode.Buckets[loc].entries[i].Key = key
									targetNode.Buckets[loc].entries[i].Value = value
								}
								needInsert = false
							}
						}
					}
				} else {
					// 非LINKED + FINGERPRINT逻辑（简化对应C++ baseline fingerprint插入逻辑）
					for i := 0; i < EntryNum && needInsert; i++ {
						if targetNode.Buckets[loc].fingerprints[i] == 0 {
							targetNode.Buckets[loc].fingerprints[i] = uint8(targets[m].fingerprint)
							targetNode.Buckets[loc].entries[i].Key = key
							targetNode.Buckets[loc].entries[i].Value = value
							needInsert = false
							break InsertLoop
						}
					}
				}
			} else {
				// 无FINGERPRINT逻辑
				if LINKED {
					// LINKED但无fingerprint逻辑
					for i := 0; i < EntryNum && needInsert; i++ {
						if IsEmptyKey(targetNode.Buckets[loc].entries[i].Key) {
							// empty slot
							if medianKey.(int) < key.(int) && targetNode == lh {
								// 插入到newRight
								newRight.Buckets[loc].entries[i].Key = key
								newRight.Buckets[loc].entries[i].Value = value
							} else {
								targetNode.Buckets[loc].entries[i].Key = key
								targetNode.Buckets[loc].entries[i].Value = value
							}
							needInsert = false
							break InsertLoop
						} else {
							// occupied slot，如果需要迁移参考C++逻辑，这里省略详细迁移步骤
							//（若需要完整迁移逻辑，请参考上面FINGERPRINT+LINKED的迁移思路）
						}
					}
				} else {
					// 非LINKED且非FINGERPRINT baseline逻辑
					for i := 0; i < EntryNum && needInsert; i++ {
						if IsEmptyKey(targetNode.Buckets[loc].entries[i].Key) {
							targetNode.Buckets[loc].entries[i].Key = key
							targetNode.Buckets[loc].entries[i].Value = value
							needInsert = false
							break InsertLoop
						}
					}
				}
			}
		}
	}
	// 更新兄弟指针
	oldSibling := lh.siblingPtr
	lh.siblingPtr = newRight
	if oldSibling != nil {
		if oldSiblingNode, ok := oldSibling.(*LNodeHash); ok {
			oldSiblingNode.LeftSiblingPtr = newRight
		}
	}

	if needInsert {
		fmt.Printf("insert after split failed -- key: %v\n", key)
	}

	if !LINKED {
		//Test
		newRight.Cardinality = len(newRight.Buckets)
		if newRight.siblingPtr == nil {
			util := newRight.Utilization() * 100
			fmt.Println(util, "%")
		}
	}

	return newRight, splitKey
}

// Update
//
//	@Description: 实现Updatable接口定义的更新方法
//	@receiver b
//	@param key
//	@param value
//	@param version
//	@return int
func (lh *LNodeHash) Update(key, value interface{}, vstart uint64) int {
	for k := 0; k < HashFuncsNum; k++ {
		// 假设 h 函数接受 key和seed来计算hash
		hashKey := h(key, 0, uint64(k))

		var fingerprint uint8
		if FINGERPRINT {
			fp := lh.Hash(hashKey) | 1
			fingerprint = uint8(fp)
		}

		for j := 0; j < NumSlot; j++ {
			loc := (hashKey + uint64(j)) % uint64(lh.Cardinality)
			if !lh.Buckets[loc].TryLock() {
				return -1
			}

			vend, needRestart := lh.GetVersion()
			if needRestart || (vstart != vend) {
				lh.Buckets[loc].Unlock()
				return -1
			}

			if LINKED {
				if lh.Buckets[loc].state != STABLE {
					if !lh.StabilizeBucket(int(loc)) {
						lh.Buckets[loc].Unlock()
						return -1
					}
				}
			}

			// 根据FINGERPRINT判断调用不同的update逻辑
			var updated bool
			if FINGERPRINT {
				updated = lh.Buckets[loc].UpdateWithFingerprint(key, value, fingerprint)
			} else {
				updated = lh.Buckets[loc].Update(key, value)
			}

			lh.Buckets[loc].Unlock()

			if updated {
				// 成功更新
				return UpdateSuccess
			}
			// 如果没有更新成功（此位置没有该key），尝试下一个槽位或下一个hash函数
		}
	}
	// 所有hash函数与槽位尝试完毕依然没有找到key
	return UpdateFailure
}

// Remove
//
//	@Description: 实现Removable接口定义的方法
//	@receiver b
//	@param key
//	@param version
//	@return int
func (lh *LNodeHash) Remove(key interface{}, vstart uint64) int {
	for k := 0; k < HashFuncsNum; k++ {
		hashKey := h(key, 0, uint64(k))

		var fingerprint uint8
		if FINGERPRINT {
			fp := lh.Hash(hashKey) | 1
			fingerprint = uint8(fp)
		}

		for j := 0; j < NumSlot; j++ {
			loc := (hashKey + uint64(j)) % uint64(lh.Cardinality)
			if !lh.Buckets[loc].TryLock() {
				return -1
			}

			vend, needRestart := lh.GetVersion()
			if needRestart || (vstart != vend) {
				lh.Buckets[loc].Unlock()
				return -1
			}

			if LINKED {
				if lh.Buckets[loc].state != STABLE {
					if !lh.StabilizeBucket(int(loc)) {
						lh.Buckets[loc].Unlock()
						return -1
					}
				}
			}

			var removed bool
			if FINGERPRINT {
				removed = lh.Buckets[loc].RemoveWithFingerprint(key, fingerprint)
			} else {
				removed = lh.Buckets[loc].Remove(key)
			}

			lh.Buckets[loc].Unlock()

			if removed {
				// 成功删除
				return 0
			}
			// 如果本位置没找到key，继续尝试下一个槽位或下一个hash函数
		}
	}
	// 遍历完所有hash函数与槽位仍未找到key
	return 1
}

// Find
//
//	@Description: 实现Finder接口定义查找方法
//	@receiver b
//	@param key
//	@return interface{}
//	@return bool
func (lh *LNodeHash) Find(key interface{}) (interface{}, bool) {
	var found bool
	for k := 0; k < HashFuncsNum; k++ {
		hashKey := h(key, 0, uint64(k))

		var fingerprint uint8
		if FINGERPRINT {
			fp := lh.Hash(hashKey) | 1
			fingerprint = uint8(fp)
		}

		for j := 0; j < NumSlot; j++ {
			loc := (hashKey + uint64(j)) % uint64(lh.Cardinality)

			bucketVstart, needRestart := lh.Buckets[loc].getVersion()

			if needRestart {
				lh.Buckets[loc].Unlock()
				return nil, found
			}
			if LINKED && lh.Buckets[loc].state != STABLE {
				if !lh.Buckets[loc].upgradeLock(bucketVstart) {
					needRestart = true
					return nil, found
				}

				if !lh.StabilizeBucket(int(loc)) {
					lh.Buckets[loc].Unlock()
					needRestart = true
					return nil, found
				}

				lh.Buckets[loc].Unlock()
				bucketVstart += 0b100
			}

			var ret interface{}

			if FINGERPRINT {
				ret, found = lh.Buckets[loc].FindWithFingerprint(key, fingerprint)
			} else {
				ret, found = lh.Buckets[loc].Find(key)
			}

			if found {
				bucketVend, needRestart := lh.Buckets[loc].getVersion()
				if needRestart || (bucketVstart != bucketVend) {
					return nil, found
				}
				return ret, found
			}

			bucketVend, needRestart := lh.Buckets[loc].getVersion()
			if needRestart || (bucketVstart != bucketVend) {
				return nil, found
			}
		}
	}
	return nil, found // 没找到key
}

// RangeLookUp
//
//	@Description: 实现RangeLookUper接口定义的方法，范围查找
//	@receiver b
//	@param key
//	@param buf
//	@param count
//	@param searchRange
//	@param continued
//	@return int
func (lh *LNodeHash) RangeLookUp(key interface{}, buf *[]interface{}, count int, rrange int, continued bool) int {
	// 收集的条目缓冲区
	var collectedEntries []Entry

	for j := 0; j < lh.Cardinality; j++ {
		bucketVstart, nr := lh.Buckets[j].getVersion()
		if nr {
			return -1
		}

		if LINKED && lh.Buckets[j].state != STABLE {
			if !lh.Buckets[j].upgradeLock(bucketVstart) {
				return -1
			}
			if !lh.StabilizeBucket(j) {
				lh.Buckets[j].Unlock()
				return -1
			}
			lh.Buckets[j].Unlock()
			bucketVstart += 0b100
		}

		var entries []Entry
		if FINGERPRINT {
			entries = lh.Buckets[j].CollectWithFingerprint(key, EmptyFingerprint)
		} else {
			entries = lh.Buckets[j].Collect(key) // 无fingerprint版本，相同逻辑
		}

		collectedEntries = append(collectedEntries, entries...)

		bucketVend, nr := lh.Buckets[j].getVersion()
		if nr || (bucketVstart != bucketVend) {
			return -1
		}
	}

	// 对收集到的条目按key排序
	sort.Slice(collectedEntries, func(i, j int) bool {
		// 假设key为int进行比较
		return collectedEntries[i].Key.(int) < collectedEntries[j].Key.(int)
	})

	// 将排序后的值填入buf
	_count := count
	for i := 0; i < len(collectedEntries); i++ {
		*buf = append(*buf, collectedEntries[i].Value)
		_count++
		if _count == rrange {
			return _count
		}
	}
	return _count
}

// Utilization
//
//	@Description: 实现Utilizer接口
//	@receiver b
//	@return float64
func (lh *LNodeHash) Utilization() float64 {
	// 简单计算利用率：非空key数量/总空间
	totalEntries := lh.Cardinality * EntryNum
	count := 0
	for i := 0; i < lh.Cardinality; i++ {
		for j := 0; j < EntryNum; j++ {
			if FINGERPRINT {
				if lh.Buckets[i].fingerprints[j]&0b1 != 0b1 {
					count++
				}
			} else {
				if lh.Buckets[i].entries[j].Key != nil {
					count++
				}
			}

		}
	}
	return float64(count) / float64(totalEntries)
}

// StabilizeAll 尝试稳定当前节点的所有bucket
// @Description: 对当前节点中所有bucket进行检查和数据迁移（如果是LINKED状态）
// @receiver lh
// @param version 当前节点版本
// @return bool 稳定化是否成功
func (lh *LNodeHash) StabilizeAll(version uint64) bool {
	// 检查是否启用了 LINKED 和 FINGERPRINT
	if !LINKED || !FINGERPRINT {
		fmt.Println("StabilizeAll: cannot be called if FINGERPRINT and LINKED flags are not defined")
		return false
	}

	for loc := 0; loc < lh.Cardinality; loc++ {
		// 如果bucket已经是STABLE则无需处理
		if lh.Buckets[loc].state == STABLE {
			continue
		}

		// 尝试锁定当前bucket
		if !lh.Buckets[loc].TryLock() {
			return false
		}

		// 检查当前版本是否匹配，防止并发修改
		curVersion, needRestart := lh.GetVersion()
		if needRestart || (version != curVersion) {
			lh.Buckets[loc].Unlock()
			return false
		}

		switch lh.Buckets[loc].state {
		case LINKED_LEFT:
			// 从左兄弟迁移
			left, ok := lh.LeftSiblingPtr.(*LNodeHash)
			if !ok {
				fmt.Println("StabilizeAll: left sibling is not LNodeHash")
				lh.Buckets[loc].Unlock()
				return false
			}

			leftVStart, needRestart := left.GetVersion()
			if needRestart {
				lh.Buckets[loc].Unlock()
				return false
			}

			leftBucket := &left.Buckets[loc]
			if !leftBucket.TryLock() {
				lh.Buckets[loc].Unlock()
				return false
			}

			leftVEnd, needRestart := left.GetVersion()
			if needRestart || (leftVStart != leftVEnd) {
				leftBucket.Unlock()
				lh.Buckets[loc].Unlock()
				return false
			}

			if leftBucket.state == LINKED_RIGHT {
				// 迁移数据
				_, ok1 := lh.HighKey.(int)
				leftHighKey, ok2 := left.HighKey.(int)
				if !ok1 || !ok2 {
					fmt.Println("StabilizeAll: type assertion for high keys failed")
					leftBucket.Unlock()
					lh.Buckets[loc].Unlock()
					return false
				}

				for i := 0; i < len(leftBucket.entries); i++ {
					if leftBucket.fingerprints[i] != EmptyFingerprint {
						entryKey, ok3 := leftBucket.entries[i].Key.(int)
						if !ok3 {
							fmt.Println("StabilizeAll: type assertion for entry key failed")
							continue
						}
						// 如果left节点的high_key < entryKey 则迁移到当前节点
						if leftHighKey < entryKey {
							lh.Buckets[loc].fingerprints[i] = leftBucket.fingerprints[i]
							lh.Buckets[loc].entries[i] = leftBucket.entries[i]
							leftBucket.fingerprints[i] = EmptyFingerprint
						}
					}
				}
				// 更新状态
				lh.Buckets[loc].state = STABLE
				leftBucket.state = STABLE
				leftBucket.Unlock()
				lh.Buckets[loc].Unlock()
			} else {
				fmt.Printf("[StabilizeAll]: something wrong!\n")
				fmt.Printf("\t current bucket state: %v, \t left bucket state: %v\n", lh.Buckets[loc].state, leftBucket.state)
				leftBucket.Unlock()
				lh.Buckets[loc].Unlock()
				return false
			}

		case LINKED_RIGHT:
			// 向右兄弟迁移
			right, ok := lh.siblingPtr.(*LNodeHash)
			if !ok {
				fmt.Println("StabilizeAll: right sibling is not LNodeHash")
				lh.Buckets[loc].Unlock()
				return false
			}

			rightVStart, needRestart := right.GetVersion()
			if needRestart {
				lh.Buckets[loc].Unlock()
				return false
			}

			rightBucket := &right.Buckets[loc]
			if !rightBucket.TryLock() {
				lh.Buckets[loc].Unlock()
				return false
			}

			rightVEnd, needRestart := right.GetVersion()
			if needRestart || (rightVStart != rightVEnd) {
				rightBucket.Unlock()
				lh.Buckets[loc].Unlock()
				return false
			}

			if rightBucket.state == LINKED_LEFT {
				currentHighKey, ok1 := lh.HighKey.(int)
				_, ok2 := right.HighKey.(int)
				if !ok1 || !ok2 {
					fmt.Println("StabilizeAll: type assertion for high keys failed")
					rightBucket.Unlock()
					lh.Buckets[loc].Unlock()
					return false
				}

				for i := 0; i < len(lh.Buckets[loc].entries); i++ {
					if lh.Buckets[loc].fingerprints[i] != EmptyFingerprint {
						entryKey, ok3 := lh.Buckets[loc].entries[i].Key.(int)
						if !ok3 {
							fmt.Println("StabilizeAll: type assertion for entry key failed")
							continue
						}
						if currentHighKey < entryKey {
							rightBucket.fingerprints[i] = lh.Buckets[loc].fingerprints[i]
							rightBucket.entries[i] = lh.Buckets[loc].entries[i]
							lh.Buckets[loc].fingerprints[i] = EmptyFingerprint
						}
					}
				}

				lh.Buckets[loc].state = STABLE
				rightBucket.state = STABLE
				rightBucket.Unlock()
				lh.Buckets[loc].Unlock()
			} else {
				fmt.Printf("[StabilizeAll]: something wrong!\n")
				fmt.Printf("\t current bucket state: %v, \t right bucket state: %v\n", lh.Buckets[loc].state, rightBucket.state)
				rightBucket.Unlock()
				lh.Buckets[loc].Unlock()
				return false
			}

		default:
			// 未知状态
			fmt.Printf("[StabilizeAll]: unknown bucket state: %v\n", lh.Buckets[loc].state)
			lh.Buckets[loc].Unlock()
			return false
		}
	}

	return true
}

// StabilizeBucket 稳定指定位置的桶
// @Description: 根据当前桶的状态，尝试从左或右兄弟桶迁移数据
// @receiver lh
// @param loc 桶的位置
// @return bool 是否成功稳定
func (lh *LNodeHash) StabilizeBucket(loc int) bool {
	// 检查是否启用了 LINKED 和 FINGERPRINT
	if !LINKED || !FINGERPRINT {
		fmt.Println("StabilizeBucket: cannot be called if FINGERPRINT and LINKED flags are not defined")
		return false
	}

	// 检查当前桶的状态
	switch lh.Buckets[loc].state {
	case LINKED_LEFT:
		// 处理 LINKED_LEFT 状态，尝试从左兄弟桶迁移数据
		left, ok := lh.LeftSiblingPtr.(*LNodeHash)
		if !ok {
			fmt.Println("StabilizeBucket: left sibling is not LNodeHash")
			return false
		}

		leftVStart, needRestart := left.GetVersion()
		if needRestart {
			return false
		}

		leftBucket := &left.Buckets[loc]
		if !leftBucket.TryLock() {
			return false
		}

		leftVEnd, needRestart := left.GetVersion()
		if needRestart || (leftVStart != leftVEnd) {
			leftBucket.Unlock()
			return false
		}

		if leftBucket.state == LINKED_RIGHT {
			// 迁移数据
			for i := 0; i < len(leftBucket.entries); i++ {
				if leftBucket.fingerprints[i] != EmptyFingerprint {
					currentHighKey, ok1 := lh.HighKey.(int)
					_, ok2 := left.HighKey.(int)
					entryKey, ok3 := leftBucket.entries[i].Key.(int)
					if !ok1 || !ok2 || !ok3 {
						fmt.Println("StabilizeBucket: type assertion failed")
						continue
					}
					if currentHighKey < entryKey {
						lh.Buckets[loc].fingerprints[i] = leftBucket.fingerprints[i]
						lh.Buckets[loc].entries[i] = leftBucket.entries[i]
						leftBucket.fingerprints[i] = EmptyFingerprint
					}
				}
			}

			// 更新状态
			lh.Buckets[loc].state = STABLE
			leftBucket.state = STABLE
			leftBucket.Unlock()
		} else {
			fmt.Printf("[StabilizeBucket]: something wrong!\n")
			fmt.Printf("\t current bucket state: %v, \t left bucket state: %v\n", lh.Buckets[loc].state, leftBucket.state)
			leftBucket.unlock()
			return false
		}

	case LINKED_RIGHT:
		// 处理 LINKED_RIGHT 状态，尝试向右兄弟桶迁移数据
		right, ok := lh.siblingPtr.(*LNodeHash)
		if !ok {
			fmt.Println("StabilizeBucket: right sibling is not LNodeHash")
			return false
		}

		rightVStart, needRestart := right.GetVersion()
		if needRestart {
			return false
		}

		rightBucket := &right.Buckets[loc]
		if !rightBucket.TryLock() {
			return false
		}

		rightVEnd, needRestart := right.GetVersion()
		if needRestart || (rightVStart != rightVEnd) {
			rightBucket.Unlock()
			return false
		}

		if rightBucket.state == LINKED_LEFT {
			// 迁移数据
			for i := 0; i < len(lh.Buckets[loc].entries); i++ {
				if lh.Buckets[loc].fingerprints[i] != 0 {
					currentHighKey, ok1 := lh.HighKey.(int)
					entryKey, ok2 := lh.Buckets[loc].entries[i].Key.(int)
					_, ok3 := right.HighKey.(int)
					if !ok1 || !ok2 || !ok3 {
						fmt.Println("StabilizeBucket: type assertion failed")
						continue
					}
					if currentHighKey < entryKey {
						rightBucket.fingerprints[i] = lh.Buckets[loc].fingerprints[i]
						rightBucket.entries[i] = lh.Buckets[loc].entries[i]
						lh.Buckets[loc].fingerprints[i] = 0
					}
				}
			}

			// 更新状态
			lh.Buckets[loc].state = STABLE
			rightBucket.state = STABLE
			rightBucket.Unlock()
		} else {
			fmt.Printf("[StabilizeBucket]: something wrong!\n")
			fmt.Printf("\t current bucket state: %v, \t right bucket state: %v\n", lh.Buckets[loc].state, rightBucket.state)
			rightBucket.Unlock()
			return false
		}

	default:
		// 不需要处理的状态
		fmt.Printf("[StabilizeBucket]: unknown bucket state: %v\n", lh.Buckets[loc].state)
		return false
	}

	return true
}

// 假设：median时对keys进行排序，返回中间位置index
func (lh *LNodeHash) findMedian(keys []interface{}) int {
	sort.Slice(keys, func(i, j int) bool {
		// 假设Key为int进行比较，根据实际情况修改
		return keys[i].(int) < keys[j].(int)
	})
	n := len(keys)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return n / 2
	} else {
		// 偶数个元素，中位数为中间两个的平均位置
		return (n/2 + (n/2 - 1)) / 2
	}
}

// 对应C++中Key_t的类型比较
func less(a, b interface{}) bool {
	return a.(int) < b.(int)
}
func (lh *LNodeHash) swap(keys []interface{}, a, b int) {
	keys[a], keys[b] = keys[b], keys[a]
}

func (lh *LNodeHash) partition(keys []interface{}, left, right int) int {
	last := keys[right]
	i, j := left, left
	for j < right {
		if less(keys[j], last) {
			lh.swap(keys, i, j)
			i++
		}
		j++
	}
	lh.swap(keys, i, right)
	return i
}

func (lh *LNodeHash) randomPartition(keys []interface{}, left, right int) int {
	n := right - left + 1
	pivot := rand.Intn(n)
	lh.swap(keys, left+pivot, right)
	return lh.partition(keys, left, right)
}

// median_util逻辑与C++一致
func (lh *LNodeHash) medianUtil(keys []interface{}, left, right, k int, a, b *int) {
	if left <= right {
		partitionIdx := lh.randomPartition(keys, left, right)
		if partitionIdx == k {
			*b = partitionIdx
			if *a != -1 {
				return
			}
		} else if partitionIdx == k-1 {
			*a = partitionIdx
			if *b != -1 {
				return
			}
		}

		if partitionIdx >= k {
			lh.medianUtil(keys, left, partitionIdx-1, k, a, b)
		} else {
			lh.medianUtil(keys, partitionIdx+1, right, k, a, b)
		}
	}
}

// Convert 将当前哈希节点转换为 B-tree 节点集合
func (lh *LNodeHash) Convert(version uint64) ([]*LNodeBTree, int, error) {
	buf := make([]Entry, 0, lh.Cardinality*EntryNum)
	key := 0
	// 如果启用了 LINKED，进行稳定化
	if LINKED {
		if !lh.StabilizeAll(version) {
			return nil, 0, fmt.Errorf("stabilize_all failed")
		}
	}

	// 尝试获取转换锁
	if !lh.TryConvertLock(version) {
		return nil, 0, fmt.Errorf("try_convertlock failed")
	}

	// 处理左兄弟节点
	left := lh.LeftSiblingPtr
	if left != nil {
		leftHash, ok := left.(*LNodeHash)
		if !ok {
			lh.ConvertUnlock()
			return nil, 0, fmt.Errorf("left sibling is not *LNodeHash")
		}
		if !leftHash.TryWriteLock() {
			lh.ConvertUnlock()
			return nil, 0, fmt.Errorf("failed to write-lock left sibling")
		}
	}

	// 收集所有桶中的条目
	for i := 0; i < lh.Cardinality; i++ {
		var collected []Entry
		if FINGERPRINT {
			collected = lh.Buckets[i].CollectWithFingerprint(key, EmptyFingerprint)
		} else {
			collected = lh.Buckets[i].Collect(key)
		}
		buf = append(buf, collected...)
	}
	idx := len(buf)

	// 按键排序条目
	sort.Slice(buf, func(i, j int) bool {
		return compareIntKeys(buf[i].Key, buf[j].Key) < 0
	})
	FillSize := int(FillFactor * float64(lh.Cardinality))
	// 确定批次大小和叶节点数量
	batchSize := FillSize // 根据实际情况定义 FillSize
	num := idx / batchSize
	if idx%batchSize != 0 {
		num += 1
	}

	// 分配叶节点
	leaves := make([]*LNodeBTree, num)
	for i := 0; i < num; i++ {
		leaves[i] = NewLNodeBTree(lh.level)
	}

	// 将条目插入到叶节点并设置兄弟指针
	from := 0
	for i := 0; i < num; i++ {
		if i < num-1 {
			leaves[i].siblingPtr = leaves[i+1]
		} else {
			leaves[i].siblingPtr = lh.siblingPtr
		}

		to := from + batchSize
		if to > idx {
			to = idx
		}
		leaves[i].BatchInsert(buf[from:to])
		from = to
	}

	// 设置最后一个叶节点的高键
	if num > 0 {
		leaves[num-1].HighKey = lh.HighKey
	}

	// 对第一个叶节点加写锁
	if num > 0 {
		leaves[0].WriteLock()
	}

	// 更新左兄弟节点的兄弟指针
	if left != nil {
		leftHash := left.(*LNodeHash)
		leftHash.siblingPtr = leaves[0]
		leftHash.WriteUnlock()
	}

	// 更新右兄弟节点的左兄弟指针
	right := lh.siblingPtr
	if right != nil {
		if rightHash, ok := right.(*LNodeHash); ok && rightHash.Type == HashNode {
			rightHash.LeftSiblingPtr = leaves[num-1]
		}
	}

	return leaves, num, nil
}

func (lh *LNodeHash) GetNode() Node {
	return lh.Node
}

func (lh *LNodeHash) GetType() NodeType {
	return HashNode
}

// Footprint 计算哈希叶子节点的内存占用。
func (lh *LNodeHash) Footprint(metrics *FootprintMetrics) {
	// 实现具体的内存占用计算逻辑
	// 示例：
	metrics.StructuralDataOccupied += uint64(unsafe.Sizeof(*lh))
	// 根据需求调整
	for i := 0; i < lh.Cardinality; i++ {
		lh.Buckets[i].Footprint(metrics)
	}
}
