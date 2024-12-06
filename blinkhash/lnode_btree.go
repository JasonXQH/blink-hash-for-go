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
	cardinality := (LeafBTreeSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Entry{}))
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
	cardinality := (LeafBTreeSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Entry{}))
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

// NewLNodeBTreeWithSibling 创建一个新的 LNodeBTree 节点，并设置兄弟节点、计数和层级
func NewLNodeBTreeWithSibling(sibling NodeInterface, count, level int) *LNodeBTree {
	cardinality := (LeafBTreeSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Entry{}))
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

func (b *LNodeBTree) GetType() NodeType {
	return BTreeNode
}

// TODO 实现Node Interface接口：
func (b *LNodeBTree) GetCount() int {
	return b.count
}

func (b *LNodeBTree) GetLevel() int {
	return b.level
}

func (b *LNodeBTree) GetLock() uint64 {
	return b.lock
}

// Print Implement Print 方法 for LNodeBTree
func (b *LNodeBTree) Print() {
	fmt.Printf("LNodeBTree Information:\n")
	fmt.Printf("Type: %v\n", b.Type)
	fmt.Printf("HighKey: %v\n", b.HighKey)
	fmt.Printf("Cardinality: %d\n", b.Cardinality)
	b.Node.Print()
	fmt.Printf("Entries:\n")
	for i, entry := range b.Entries {
		fmt.Printf("\tEntry %d: Key = %v, Value = %v\n", i, entry.Key, entry.Value)
	}
}

// SanityCheck
//
//	@Description: 实现Node基类接口，检查合法性
//	@receiver b
//	@param _highKey
//	@param first
func (b *LNodeBTree) SanityCheck(_highKey interface{}, first bool) {
	fmt.Printf("我是LNodeBTree 调用 SanityCheck:\n")
	// 检查键值是否有序
	for i := 0; i < b.count-1; i++ {
		for j := i + 1; j < b.count; j++ {
			keyInt, ok := b.Entries[i].Key.(int)
			if !ok {
				fmt.Printf("Error: Entry key is not an int: %v\n", b.Entries[i].Key)
				continue
			}

			highKeyInt, ok := b.HighKey.(int)
			if !ok {
				fmt.Printf("Error: HighKey is not an int: %v\n", b.HighKey)
				continue
			}
			if keyInt > highKeyInt { // 假设key是int类型
				fmt.Printf("lnode_t::key order is not preserved!!\n")
				fmt.Printf("[%d].key: %v\t[%d].key: %v\n", i, b.Entries[i].Key, j, b.Entries[j].Key)
			}
		}
	}

	// 检查 sibling 和 highKey 的关系
	for i := 0; i < b.count; i++ {
		entryKey, ok1 := b.Entries[i].Key.(int)
		highKeyInt, ok2 := b.HighKey.(int)
		if !ok1 || !ok2 {
			fmt.Printf("Error: Entry key or HighKey is not int type\n")
			continue
		}
		if b.siblingPtr != nil && entryKey > highKeyInt {
			fmt.Printf("%d lnode_t:: (%v) is higher than high Key %v\n", i, b.Entries[i].Key, b.HighKey)
		}
		if !first {
			prevHighKeyInt, ok := _highKey.(int)
			if !ok {
				fmt.Printf("Error: prevHighKey is not int type\n")
				continue
			}
			if b.siblingPtr != nil && entryKey < prevHighKeyInt {
				fmt.Printf("lnode_t:: %d (%v) is smaller than previous high Key %v\n", i, b.Entries[i].Key, _highKey)
				fmt.Printf("--------- node_address %v , current high_Key %v\n", b, b.HighKey)
			}
		}
	}

	if b.siblingPtr != nil {
		sibling := b.siblingPtr
		sibling.SanityCheck(_highKey, first)
	}
}

// WriteUnlock
//
//	@Description: 实现基类中的WriteUnlock方法，还是调用Node实现的上锁接口
//	@receiver b
func (b *LNodeBTree) WriteUnlock() {
	b.Node.WriteUnlock()
}

// WriteUnlockObsolete
//
//	@Description: 实现基类中的WriteUnlockObsolete方法
//	@receiver b
func (b *LNodeBTree) WriteUnlockObsolete() {
	b.Node.WriteUnlockObsolete()
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
func (b *LNodeBTree) Split(key interface{}, value interface{}, version uint64) (Splittable, interface{}) {
	half := len(b.Entries) / 2
	if half == 0 {
		panic("Split: cannot split a node with zero entries")
	}
	splitKey := b.Entries[half-1].Key // 确定拆分键
	newCnt := len(b.Entries) - half
	// 创建新的兄弟节点
	newLeaf := NewLNodeBTreeWithSibling(b.siblingPtr, newCnt, b.level)
	newLeaf.HighKey = b.HighKey

	// 拷贝后半部分到新叶节点
	copy(newLeaf.Entries, b.Entries[half:half+newCnt])
	newLeaf.count = newCnt

	// 更新当前节点
	b.siblingPtr = newLeaf
	b.HighKey = splitKey
	b.count = half
	b.Entries = b.Entries[:half]
	// 根据键值确定插入位置
	if compareIntKeys(splitKey, key) < 0 {
		newLeaf.InsertAfterSplit(key, value)
	} else {
		b.InsertAfterSplit(key, value)
	}
	siblingPtr := newLeaf.siblingPtr
	if hashNode, ok := siblingPtr.(*LNodeHash); ok {
		hashNode.LeftSiblingPtr = newLeaf
	}
	//return &newLeaf.Node
	fmt.Println("我是LNodeBTree，调用Split")
	return newLeaf, splitKey
}

func (b *LNodeBTree) InsertAfterSplit(key, value interface{}) {
	pos := b.FindLowerBound(key)

	// 将元素向后移动，为新元素腾出位置
	// 在 Go 中，我们可以使用切片和 append 函数来处理这个问题
	b.Entries = append(b.Entries, Entry{})
	copy(b.Entries[pos+1:], b.Entries[pos:len(b.Entries)-1]) // 复制 pos 位置后的切片
	// 在找到的位置插入新的键值对
	b.Entries[pos] = Entry{Key: key, Value: value}
	// 更新元素计数
	b.count++
}

// Insert
//
//	@Description: 实现Insertable接口：
//	@receiver b
//	@param key
//	@param value
//	@param version
//	@return int
func (b *LNodeBTree) Insert(key interface{}, value interface{}, version uint64) int {
	success, needRestart := b.TryUpgradeWriteLock(version)
	if needRestart {
		// 如果需要重启，按照 C++ 代码的逻辑返回 -1
		return NeedRestart
	}
	if !success {
		// 如果未能升级写锁成功，也需要处理
		return NeedRestart
	}
	// 检查是否有足够空间进行插入
	if len(b.Entries) >= b.Cardinality {
		b.WriteUnlock()  // 释放写锁
		return NeedSplit // 表示需要分裂
	}
	// 执行插入逻辑
	pos := b.FindLowerBound(key)
	// 边界保护
	if pos < 0 {
		pos = 0
	}
	if pos > len(b.Entries) {
		pos = len(b.Entries)
	}

	// 使用Go的切片操作来插入数据
	b.Entries = append(b.Entries, Entry{}) // 扩展切片以防止越界
	copy(b.Entries[pos+1:], b.Entries[pos:])
	b.Entries[pos] = Entry{Key: key, Value: value}
	// 更新计数
	b.count++
	b.WriteUnlock() // 插入完成后释放写锁
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
func (b *LNodeBTree) Update(key interface{}, value interface{}, version uint64) int {
	needRestart, _ := b.Node.TryUpgradeWriteLock(version)
	if needRestart {
		return NeedRestart
	}

	// Perform update_linear
	updated := b.updateLinear(key, value)

	b.WriteUnlock()

	if updated {
		return UpdateSuccess
	} else {
		return UpdateFailure
	}
}

// updateLinear searches for the key and updates the value if found
func (b *LNodeBTree) updateLinear(key interface{}, value interface{}) bool {
	for i, entry := range b.Entries {
		if compareIntKeys(entry.Key, key) == 0 {
			b.Entries[i].Value = value
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
func (b *LNodeBTree) Remove(key interface{}, version uint64) int {
	needRestart, _ := b.TryUpgradeWriteLock(version)
	if needRestart {
		return NeedRestart
	}

	if b.count > 0 {
		pos := b.findPosLinear(key)
		if pos == -1 {
			b.WriteUnlock()
			return KeyNotFound // Key not found
		}
		// Remove the entry at pos by shifting
		b.Entries = append(b.Entries[:pos], b.Entries[pos+1:]...)
		b.count--

		b.WriteUnlock()
		return RemoveSuccess
	}

	b.WriteUnlock()
	return KeyNotFound
}

// findPosLinear finds the position of the key in Entries
func (b *LNodeBTree) findPosLinear(key interface{}) int {
	for i, entry := range b.Entries {
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
func (b *LNodeBTree) RangeLookUp(key interface{}, buf *[]interface{}, count int, searchRange int, continued bool) int {
	currentCount := count // 使用一个单独的变量跟踪填充数量
	if continued {
		for i := 0; i < b.count; i++ {
			*buf = append(*buf, b.Entries[i].Value)
			currentCount++
			if currentCount == searchRange {
				return currentCount
			}
		}
		return currentCount
	} else {
		pos := b.FindLowerBound(key)
		// 从 pos 开始，注意 pos + 1 是否合理，取决于具体需求
		for i := pos + 1; i < b.count; i++ {
			*buf = append(*buf, b.Entries[i].Value)
			currentCount++
			if currentCount == searchRange {
				return currentCount
			}
		}
		return currentCount
	}
}

// Find
//
//	@Description: 实现Finder接口定义查找方法
//	@receiver b
//	@param key
//	@return interface{}
//	@return bool
func (b *LNodeBTree) Find(key interface{}) (interface{}, bool) {
	//TODO implement me
	// 假设 LEAF_BTREE_SIZE 是一个全局常量
	if LeafHashSize < 2048 {
		return b.findLinear(key)
	} else {
		return b.findBinary(key)
	}
}

// Utilization
//
//	@Description: 实现Utilizer接口
//	@receiver b
//	@return float64
func (b *LNodeBTree) Utilization() float64 {
	// 返回B树节点的利用率计算
	return float64(len(b.Entries)) / float64(b.Cardinality)
}

// lowerboundLinear
//
//	@Description: 工具函数，线性查找
//	@receiver b
//	@param key
//	@return int
func (b *LNodeBTree) lowerboundLinear(key interface{}) int {
	keyInt, ok := key.(int)
	if !ok {
		panic("FindLowerBoundLinear: key is not of type int")
	}
	for i, entry := range b.Entries {
		entryKey, ok := entry.Key.(int)
		if !ok {
			continue
		}
		if keyInt <= entryKey {
			return i
		}
	}
	return len(b.Entries) // 插入到末尾
}

// lowerboundBinary
//
//	@Description: 工具函数，二分查找key
//	@receiver b
//	@param key
//	@return int
func (b *LNodeBTree) lowerboundBinary(key interface{}) int {
	lower := 0
	upper := len(b.Entries)
	for lower < upper {
		mid := (upper-lower)/2 + lower
		if key.(int) < b.Entries[mid].Key.(int) {
			upper = mid
		} else if key.(int) > b.Entries[mid].Key.(int) {
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
func (b *LNodeBTree) FindLowerBound(key interface{}) int {
	if LeafBTreeSize < 2048 {
		return b.lowerboundLinear(key)
	} else {
		return b.lowerboundBinary(key)
	}
}

func (b *LNodeBTree) findLinear(key interface{}) (interface{}, bool) {
	for i := 0; i < len(b.Entries); i++ {
		if key == b.Entries[i].Key {
			return b.Entries[i].Value, true
		}
	}
	return nil, false // 代替 C++ 中的返回 0，更符合 Go 的惯例
}

func (b *LNodeBTree) findBinary(key interface{}) (interface{}, bool) {
	lower := 0
	upper := len(b.Entries)
	for lower < upper {
		mid := (upper-lower)/2 + lower
		if compareIntKeys(key, b.Entries[mid].Key) < 0 {
			upper = mid
		} else if compareIntKeys(key, b.Entries[mid].Key) > 0 {
			lower = mid + 1
		} else {
			return b.Entries[mid].Value, true
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
func (b *LNodeBTree) batchInsert(buf []Entry, batchSize int, from *int, to int) {
	// 如果 from + batch_size < to，则拷贝 batch_size 个条目
	if *from+batchSize < to {
		b.Entries = append(b.Entries, buf[*from:*from+batchSize]...)
		*from += batchSize
		b.count += batchSize
	} else {
		// 否则只拷贝 (to - from) 个条目
		b.Entries = append(b.Entries, buf[*from:to]...)
		b.count += (to - *from)
		*from = to
	}
	// 更新 HighKey
	b.HighKey = b.Entries[b.count-1].Key
}
