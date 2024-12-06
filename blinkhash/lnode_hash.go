package blinkhash

import (
	"fmt"
	"runtime"
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

// TODO: LNodeHash构造函数
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
	return &LNodeHash{
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
}

// GetCount
//
//	@Description: 获得hashnode中的entry条数
//	@receiver lh
//	@return int
func (lh *LNodeHash) GetCount() int {
	return lh.count
}

// GetLevel
//
//	@Description: 获得hashnode位于的层级
//	@receiver lh
//	@return int
func (lh *LNodeHash) GetLevel() int {
	return lh.level
}

func (lh *LNodeHash) GetLock() uint64 {
	return lh.lock
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

func (lh *LNodeHash) WriteUnlock() {
	// 实现具体方法
	lh.WriteUnlock()
}

func (lh *LNodeHash) WriteUnlockObsolete() {
	lh.WriteUnlockObsolete()
}

// TrySplitLock 尝试分裂锁定，等同于 C++ 的 try_splitlock
func (h *LNodeHash) TrySplitLock(version uint64) bool {
	success, needRestart := h.TryUpgradeWriteLock(version)
	if needRestart || !success {
		return false
	}

	for i := 0; i < h.Cardinality; i++ {
		for !h.Buckets[i].TryLock() {
			runtime.Gosched() // 防止紧密循环
		}
	}
	return true
}

// TryConvertLock 尝试转换锁定，等同于 C++ 的 try_convertlock
func (h *LNodeHash) TryConvertLock(version uint64) bool {
	success, needRestart := h.TryUpgradeWriteLock(version)
	if needRestart || !success {
		return false
	}

	for i := 0; i < h.Cardinality; i++ {
		for !h.Buckets[i].TryLock() {
			runtime.Gosched()
		}
	}
	return true
}

// SplitUnlock 释放分裂时的锁，等同于 C++ 的 split_unlock
func (h *LNodeHash) SplitUnlock() {
	h.WriteUnlock()
	for i := 0; i < h.Cardinality; i++ {
		h.Buckets[i].Unlock()
	}
}

// SplitUnlockObsolete 以过时方式释放分裂锁，等同于 C++ 的 split_unlock_obsolete
func (h *LNodeHash) SplitUnlockObsolete() {
	h.WriteUnlockObsolete()
	for i := 0; i < h.Cardinality; i++ {
		h.Buckets[i].Unlock()
	}
}

// TryWriteLock 尝试获取写锁，等同于 C++ 的 try_writelock
func (h *LNodeHash) TryWriteLock() bool {
	return h.Node.TryWriteLock()
}

// ConvertUnlock 释放转换锁定，等同于 C++ 的 convert_unlock
func (h *LNodeHash) ConvertUnlock() {
	h.WriteUnlock()
	for i := 0; i < h.Cardinality; i++ {
		h.Buckets[i].Unlock()
	}
}

// ConvertUnlockObsolete 以过时方式释放转换锁定，等同于 C++ 的 convert_unlock_obsolete
func (h *LNodeHash) ConvertUnlockObsolete() {
	h.Node.WriteUnlockObsolete()
}

// Hash 对键进行哈希运算，等同于 C++ 的 _hash 方法
func (h *LNodeHash) Hash(key interface{}) uint8 {
	keyInt, ok := key.(int)
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
	fmt.Println("我是LNodeHash，调用Insert")

	// 根据 FINGERPRINT 设置初始化 empty
	var empty interface{}
	if FINGERPRINT {
		// 根据 AVX 指令集设置 empty
		// 这里假设没有启用 AVX 指令集
		empty = uint8(0)
	} else {
		// 如果未启用 FINGERPRINT，可以设置为 nil 或其他默认值
		empty = nil
	}

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
				success = lh.Buckets[loc].InsertWithFingerprint(key, value, fingerprint, empty)
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
func (lh *LNodeHash) Split(splitKey interface{}, key interface{}, value interface{}, version interface{}) *Node {
	//TODO implement me
	fmt.Println("我是LNodeHash，调用Split")
	return nil
}

// Update
//
//	@Description: 实现Updatable接口定义的更新方法
//	@receiver b
//	@param key
//	@param value
//	@param version
//	@return int
func (lh *LNodeHash) Update(key interface{}, value interface{}, version uint64) int {
	//TODO implement me
	panic("implement me")
}

// Remove
//
//	@Description: 实现Removable接口定义的方法
//	@receiver b
//	@param key
//	@param version
//	@return int
func (lh *LNodeHash) Remove(key interface{}, version uint64) int {
	//TODO implement me
	panic("implement me")
}

// Find
//
//	@Description: 实现Finder接口定义查找方法
//	@receiver b
//	@param key
//	@return interface{}
//	@return bool
func (lh *LNodeHash) Find(key interface{}) (interface{}, bool) {
	//TODO implement me
	panic("implement me")
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
func (lh *LNodeHash) RangeLookUp(key interface{}, buf *[]interface{}, count int, searchRange int, continued bool) int {
	//TODO implement me
	panic("implement me")
}

// Utilization
//
//	@Description: 实现Utilizer接口
//	@receiver b
//	@return float64
func (lh *LNodeHash) Utilization() float64 {
	//TODO implement me
	panic("implement me")
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
				if leftBucket.fingerprints[i] != 0 {
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
						leftBucket.fingerprints[i] = 0
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
			return false
		}

	default:
		// 不需要处理的状态
		fmt.Printf("[StabilizeBucket]: unknown bucket state: %v\n", lh.Buckets[loc].state)
		return false
	}

	return true
}
