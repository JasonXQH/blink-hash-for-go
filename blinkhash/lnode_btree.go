package blinkhash

import (
	"fmt"
	"unsafe"
)

type LNodeBTree struct {
	Node
	Type        NodeType
	HighKey     interface{}
	Cardinality int
	Entries     []Entry
}

// NewLNodeBTree 创建一个新的 LNodeBTree 节点
func NewLNodeBTree(level int) *LNodeBTree {
	cardinality := LNodeBTreeCardinality
	return &LNodeBTree{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       level,
		},
		Type:        BTreeNode,
		HighKey:     nil, // 需要在 Split 中设置
		Cardinality: cardinality,
		Entries:     make([]Entry, 0, LeafBTreeSize),
	}
}

// NewLNodeBTreeWithLevel 创建一个新的 LNodeBTree 节点，指定层级
func NewLNodeBTreeWithLevel(level int) *LNodeBTree {
	cardinality := LNodeBTreeCardinality
	return &LNodeBTree{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       level,
		},
		Type:        BTreeNode,
		HighKey:     nil, // 需要在 Split 中设置
		Cardinality: cardinality,
		Entries:     make([]Entry, LeafBTreeSize),
	}
}

// NewLNodeBTreeWithSibling 创建一个新的 LNodeBTree 节点，并设置兄弟节点、计数和层级
func NewLNodeBTreeWithSibling(sibling NodeInterface, count int32, level int) *LNodeBTree {
	cardinality := LNodeBTreeCardinality
	return &LNodeBTree{
		Node: Node{
			lock:        0,
			siblingPtr:  sibling,
			leftmostPtr: nil,
			count:       count,
			level:       level,
		},
		Type:        BTreeNode,
		HighKey:     nil, // 需要在 Split 中设置
		Cardinality: cardinality,
		Entries:     make([]Entry, count, LeafBTreeSize),
	}
}

// TODO 实现Node Interface接口：

func (lb *LNodeBTree) GetHighKey() interface{} {
	return lb.HighKey
}

// Print Implement Print 方法 for LNodeBTree
func (lb *LNodeBTree) Print() {
	fmt.Printf("LNodeBTree Information:\n")
	fmt.Printf("Type: %v\n", lb.Type)
	fmt.Printf("HighKey: %v\n", lb.HighKey)
	fmt.Printf("Cardinality: %d\n", lb.Cardinality)
	lb.Node.Print()
	fmt.Printf("Entries:\n")
	for i, entry := range lb.Entries {
		fmt.Printf("\tEntry %d: Key = %v, Value = %v\n", i, entry.Key, entry.Value)
	}
}

// SanityCheck
//
//	@Description: 实现Node基类接口，检查合法性
//	@receiver b
//	@param _highKey
//	@param first
func (lb *LNodeBTree) SanityCheck(_highKey interface{}, first bool) {
	fmt.Printf("我是LNodeBTree 调用 SanityCheck:\n")
	// 检查键值是否有序
	count := int(lb.count)
	for i := 0; i < count-1; i++ {
		for j := i + 1; j < count; j++ {
			keyInt, ok := lb.Entries[i].Key.(int)
			if !ok {
				fmt.Printf("Error: Entry key is not an int: %v\n", lb.Entries[i].Key)
				continue
			}

			highKeyInt, ok := lb.HighKey.(int)
			if !ok {
				fmt.Printf("Error: HighKey is not an int: %v\n", lb.HighKey)
				continue
			}
			if keyInt > highKeyInt { // 假设key是int类型
				fmt.Printf("lnode_t::key order is not preserved!!\n")
				fmt.Printf("[%d].key: %v\t[%d].key: %v\n", i, lb.Entries[i].Key, j, lb.Entries[j].Key)
			}
		}
	}

	// 检查 sibling 和 highKey 的关系
	for i := 0; i < count; i++ {
		entryKey, ok1 := lb.Entries[i].Key.(int)
		highKeyInt, ok2 := lb.HighKey.(int)
		if !ok1 || !ok2 {
			fmt.Printf("Error: Entry key or HighKey is not int type\n")
			continue
		}
		if lb.siblingPtr != nil && entryKey > highKeyInt {
			fmt.Printf("%d lnode_t:: (%v) is higher than high Key %v\n", i, lb.Entries[i].Key, lb.HighKey)
		}
		if !first {
			prevHighKeyInt, ok := _highKey.(int)
			if !ok {
				fmt.Printf("Error: prevHighKey is not int type\n")
				continue
			}
			if lb.siblingPtr != nil && entryKey < prevHighKeyInt {
				fmt.Printf("lnode_t:: %d (%v) is smaller than previous high Key %v\n", i, lb.Entries[i].Key, _highKey)
				fmt.Printf("--------- node_address %v , current high_Key %v\n", lb, lb.HighKey)
			}
		}
	}

	if lb.siblingPtr != nil {
		sibling := lb.siblingPtr
		sibling.SanityCheck(_highKey, first)
	}
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
func (lb *LNodeBTree) Split(key interface{}, value interface{}, version uint64) (Splittable, interface{}) {
	half := len(lb.Entries) / 2
	if half == 0 {
		panic("Split: cannot split a node with zero entries")
	}
	splitKey := lb.Entries[half-1].Key // 确定拆分键
	newCnt := int32(len(lb.Entries) - half)
	// 创建新的兄弟节点
	newLeaf := NewLNodeBTreeWithSibling(lb.siblingPtr, newCnt, lb.level)
	newLeaf.HighKey = lb.HighKey

	// 拷贝后半部分到新叶节点
	copy(newLeaf.Entries, lb.Entries[half:half+int(newCnt)])
	newLeaf.count = newCnt

	// 更新当前节点
	lb.siblingPtr = newLeaf
	lb.HighKey = splitKey
	lb.count = int32(half)
	lb.Entries = lb.Entries[:half]
	// 根据键值确定插入位置
	if compareIntKeys(splitKey, key) < 0 {
		newLeaf.InsertAfterSplit(key, value)
	} else {
		lb.InsertAfterSplit(key, value)
	}
	siblingPtr := newLeaf.siblingPtr
	if hashNode, ok := siblingPtr.(*LNodeHash); ok {
		hashNode.LeftSiblingPtr = newLeaf
	}
	//return &newLeaf.Node
	//fmt.Println("我是LNodeBTree，调用Split")
	return newLeaf, splitKey
}

func (lb *LNodeBTree) InsertAfterSplit(key, value interface{}) {
	pos := lb.FindLowerBound(key)

	// 将元素向后移动，为新元素腾出位置
	// 在 Go 中，我们可以使用切片和 append 函数来处理这个问题
	lb.Entries = append(lb.Entries, Entry{})
	copy(lb.Entries[pos+1:], lb.Entries[pos:len(lb.Entries)-1]) // 复制 pos 位置后的切片
	// 在找到的位置插入新的键值对
	lb.Entries[pos] = Entry{Key: key, Value: value}
	// 更新元素计数
	lb.count++
}

// Insert
//
//	@Description: 实现Insertable接口：
//	@receiver b
//	@param key
//	@param value
//	@param version
//	@return int
func (lb *LNodeBTree) Insert(key interface{}, value interface{}, version uint64) int {
	success, needRestart := lb.TryUpgradeWriteLock(version)
	if needRestart {
		// 如果需要重启，按照 C++ 代码的逻辑返回 -1
		return NeedRestart
	}
	if !success {
		// 如果未能升级写锁成功，也需要处理
		return NeedRestart
	}
	// 检查是否有足够空间进行插入
	if len(lb.Entries) >= lb.Cardinality {
		lb.WriteUnlock() // 释放写锁
		return NeedSplit // 表示需要分裂
	}
	// 执行插入逻辑
	pos := lb.FindLowerBound(key)
	// 边界保护
	if pos < 0 {
		pos = 0
	}
	if pos > len(lb.Entries) {
		pos = len(lb.Entries)
	}

	// 使用Go的切片操作来插入数据
	lb.Entries = append(lb.Entries, Entry{}) // 扩展切片以防止越界
	copy(lb.Entries[pos+1:], lb.Entries[pos:])
	lb.Entries[pos] = Entry{Key: key, Value: value}
	// 更新计数
	lb.count++
	lb.WriteUnlock() // 插入完成后释放写锁
	return InsertSuccess
}

// Update
//
//	@Description: 实现Updatable接口定义的更新方法
//	@receiver b
//	@param key
//	@param value
//	@param version
//	@return int
func (lb *LNodeBTree) Update(key interface{}, value interface{}, version uint64) int {
	needRestart, _ := lb.Node.TryUpgradeWriteLock(version)
	if needRestart {
		return NeedRestart
	}

	// Perform update_linear
	updated := lb.updateLinear(key, value)

	lb.WriteUnlock()

	if updated {
		return UpdateSuccess
	} else {
		return UpdateFailure
	}
}

// updateLinear searches for the key and updates the value if found
func (lb *LNodeBTree) updateLinear(key interface{}, value interface{}) bool {
	for i, entry := range lb.Entries {
		if compareIntKeys(entry.Key, key) == 0 {
			lb.Entries[i].Value = value
			return true
		}
	}
	return false
}

// Remove
//
//	@Description: 实现Removable接口定义的方法
//	@receiver b
//	@param key
//	@param version
//	@return int
func (lb *LNodeBTree) Remove(key interface{}, version uint64) int {
	needRestart, _ := lb.TryUpgradeWriteLock(version)
	if needRestart {
		return NeedRestart
	}

	if lb.count > 0 {
		pos := lb.findPosLinear(key)
		if pos == -1 {
			lb.WriteUnlock()
			return KeyNotFound // Key not found
		}
		// Remove the entry at pos by shifting
		lb.Entries = append(lb.Entries[:pos], lb.Entries[pos+1:]...)
		lb.count--

		lb.WriteUnlock()
		return RemoveSuccess
	}

	lb.WriteUnlock()
	return KeyNotFound
}

// findPosLinear finds the position of the key in Entries
func (lb *LNodeBTree) findPosLinear(key interface{}) int {
	for i, entry := range lb.Entries {
		if compareIntKeys(entry.Key, key) == 0 {
			return i
		}
	}
	return -1 // Not found
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
func (lb *LNodeBTree) RangeLookUp(key interface{}, upTo int, continued bool, version uint64) ([]interface{}, int, int) {
	// LNodeBTree 不需要 version 做并发检测，这里忽略
	// retCode 默认 0 表示正常, NeedRestart/NeedConvert 不适用此实现

	collected := make([]interface{}, 0, upTo)
	currentCount := 0

	// 如果 continued == true，表示我们之前已经搜到一部分了，这次无视 key，直接遍历
	if continued {
		for i := 0; i < int(lb.count); i++ {
			collected = append(collected, lb.Entries[i].Value)
			currentCount++
			if currentCount == upTo {
				return collected, 0, currentCount
			}
		}
		return collected, 0, currentCount
	} else {
		// 未连续查找，则先找到 key 的起点
		pos := lb.FindLowerBound(key)
		// 假设: FindLowerBound 返回小于等于 key 的位置,
		// 我们希望从 pos+1 开始收集
		for i := pos + 1; i < int(lb.count); i++ {
			collected = append(collected, lb.Entries[i].Value)
			currentCount++
			if currentCount == upTo {
				return collected, 0, currentCount
			}
		}
		return collected, 0, currentCount
	}
}

// Find
//
//	@Description: 实现Finder接口定义查找方法
//	@receiver b
//	@param key
//	@return interface{}
//	@return bool
func (lb *LNodeBTree) Find(key interface{}) (interface{}, bool) {
	//TODO implement me
	// 假设 LEAF_BTREE_SIZE 是一个全局常量
	if LeafHashSize < 2048 {
		return lb.findLinear(key)
	} else {
		return lb.findBinary(key)
	}
}

// Utilization
//
//	@Description: 实现Utilizer接口
//	@receiver b
//	@return float64
func (lb *LNodeBTree) Utilization() float64 {
	// 返回B树节点的利用率计算
	return float64(len(lb.Entries)) / float64(lb.Cardinality)
}

// lowerboundLinear
//
//	@Description: 工具函数，线性查找
//	@receiver b
//	@param key
//	@return int
func (lb *LNodeBTree) lowerboundLinear(key interface{}) int {
	keyInt, ok := key.(int)
	if !ok {
		panic("FindLowerBoundLinear: key is not of type int")
	}
	for i, entry := range lb.Entries {
		entryKey, ok := entry.Key.(int)
		if !ok {
			continue
		}
		if keyInt <= entryKey {
			return i
		}
	}
	return len(lb.Entries) // 插入到末尾
}

// lowerboundBinary
//
//	@Description: 工具函数，二分查找key
//	@receiver b
//	@param key
//	@return int
func (lb *LNodeBTree) lowerboundBinary(key interface{}) int {
	lower := 0
	upper := len(lb.Entries)
	for lower < upper {
		mid := (upper-lower)/2 + lower
		if key.(int) < lb.Entries[mid].Key.(int) {
			upper = mid
		} else if key.(int) > lb.Entries[mid].Key.(int) {
			lower = mid + 1
		} else {
			return mid
		}
	}
	return lower
}

// FindLowerBound
//
//	@Description: 工具函数，查找下界
//	@receiver b
//	@param key
//	@return int
func (lb *LNodeBTree) FindLowerBound(key interface{}) int {
	if LeafBTreeSize < 2048 {
		return lb.lowerboundLinear(key)
	} else {
		return lb.lowerboundBinary(key)
	}
}

func (lb *LNodeBTree) findLinear(key interface{}) (interface{}, bool) {
	for i := 0; i < len(lb.Entries); i++ {
		if key == lb.Entries[i].Key {
			return lb.Entries[i].Value, true
		}
	}
	return nil, false // 代替 C++ 中的返回 0，更符合 Go 的惯例
}

func (lb *LNodeBTree) findBinary(key interface{}) (interface{}, bool) {
	lower := 0
	upper := len(lb.Entries)
	for lower < upper {
		mid := (upper-lower)/2 + lower
		if compareIntKeys(key, lb.Entries[mid].Key) < 0 {
			upper = mid
		} else if compareIntKeys(key, lb.Entries[mid].Key) > 0 {
			lower = mid + 1
		} else {
			return lb.Entries[mid].Value, true
		}
	}
	return nil, false // 代替 C++ 中的返回 0
}

// batchInsert
//
//	@Description: 批量插入
//	@receiver b
//	@param buf
//	@param batchSize
//	@param from
//	@param to
func (lb *LNodeBTree) batchInsert(buf []Entry, batchSize int, from *int, to int) {
	// 如果 from + batch_size < to，则拷贝 batch_size 个条目
	if *from+batchSize < to {
		lb.Entries = append(lb.Entries, buf[*from:*from+batchSize]...)
		*from += batchSize
		lb.count += int32(batchSize)
	} else {
		// 否则只拷贝 (to - from) 个条目
		lb.Entries = append(lb.Entries, buf[*from:to]...)
		lb.count += int32(to - *from)
		*from = to
	}
	// 更新 HighKey
	lb.HighKey = lb.Entries[lb.count-1].Key
}

// BatchInsert 批量插入条目到 B-tree 节点
func (lb *LNodeBTree) BatchInsert(entries []Entry) {
	lb.Entries = append(lb.Entries, entries...)
	lb.count += int32(len(entries))
	if lb.count > 0 {
		lb.HighKey = lb.Entries[lb.count-1].Key
	}
}

// Footprint 计算B树叶子节点的内存占用。
func (lb *LNodeBTree) Footprint(metrics *FootprintMetrics) {
	// 实现具体的内存占用计算逻辑
	cnt := lb.count
	invalidNum := lb.Cardinality - int(cnt)
	metrics.KeyDataOccupied += uint64(unsafe.Sizeof(Entry{})) * uint64(cnt)
	metrics.KeyDataUnoccupied += uint64(unsafe.Sizeof(Entry{})) * uint64(invalidNum)

}

func (lb *LNodeBTree) GetNode() *Node {
	return &lb.Node
}

func (lb *LNodeBTree) GetType() NodeType {
	return BTreeNode
}

func (lb *LNodeBTree) GetEntries() []Entry {
	return lb.Entries
}
func (lb *LNodeBTree) SetHighKey(key interface{}) { lb.HighKey = key }

func (lb *LNodeBTree) GetCardinality() int {
	return lb.Cardinality
}
