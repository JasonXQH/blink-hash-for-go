package blinkhash

import (
	"fmt"
	"unsafe"
)

type LNodeBTree struct {
	Cardinality int
	LNode       LNode
	Entries     []Entry
}

func NewLNodeBTree() *LNodeBTree {
	cardinality := (LeafBTreeSize - int(unsafe.Sizeof(LNode{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Entry{}))
	lNodeBTree := &LNodeBTree{
		LNode:       *NewLNode(0, BTreeNode),
		Cardinality: cardinality,
		Entries:     make([]Entry, 0, LeafBTreeSize),
	}
	lNodeBTree.LNode.Behavior = lNodeBTree // 设置 Behavior 指向 lNodeBTree
	return lNodeBTree
}

func NewLNodeBTreeWithLevel(level int) *LNodeBTree {
	cardinality := (LeafBTreeSize - int(unsafe.Sizeof(LNode{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Entry{}))
	lNodeBTree := &LNodeBTree{
		Cardinality: cardinality,
		LNode:       *NewLNode(level, BTreeNode),
		Entries:     make([]Entry, cardinality),
	}
	lNodeBTree.LNode.Behavior = lNodeBTree // 设置 Behavior 指向 lNodeBTree
	return lNodeBTree
}

func NewLNodeBTreeWithSibling(sibling *Node, count, level int) *LNodeBTree {
	cardinality := (LeafBTreeSize - int(unsafe.Sizeof(LNode{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Entry{}))
	lNodeBTree := &LNodeBTree{
		Cardinality: cardinality,
		LNode:       *NewLNodeWithSibling(BTreeNode, sibling, count, level),
		Entries:     make([]Entry, cardinality),
	}
	lNodeBTree.LNode.Behavior = lNodeBTree // 设置 Behavior 指向 lNodeBTree
	return lNodeBTree
}

func (b *LNodeBTree) WriteUnlock() {
	// 实现具体方法
	b.LNode.Node.WriteUnlock()
}

// 实现其他 LNodeInterface 方法...
func (b *LNodeBTree) Utilization() float64 {
	// 返回B树节点的利用率计算
	return float64(len(b.Entries)) / float64(b.Cardinality)
}

func (b *LNodeBTree) ConvertUnlock() {
	//TODO implement me
	fmt.Println("LNodeBTree 执行ConvertUnlock")
}

func (b *LNodeBTree) WriteUnlockObsolete() {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeBTree) Insert(key interface{}, value interface{}, version uint64) int {
	success, needRestart := b.LNode.TryUpgradeWriteLock(version)
	if needRestart {
		// 如果需要重启，按照 C++ 代码的逻辑返回 -1
		return -1
	}
	if !success {
		// 如果未能升级写锁成功，也需要处理
		return -1
	}
	// 检查是否有足够空间进行插入
	if len(b.Entries) >= b.Cardinality {
		b.LNode.WriteUnlock() // 释放写锁
		return 1              // 表示需要分裂
	}

	// 执行插入逻辑
	pos := b.FindLowerBound(key)
	// 使用Go的切片操作来插入数据
	b.Entries = append(b.Entries, Entry{}) // 扩展切片以防止越界
	copy(b.Entries[pos+1:], b.Entries[pos:])
	b.Entries[pos] = Entry{Key: key, Value: value}
	// 更新计数
	b.LNode.count++
	b.LNode.WriteUnlock() // 插入完成后释放写锁
	return 0
}

// InsertAfterSplit
//
//	@Description:
//	@receiver b
//	@param key
//	@param value
func (b *LNodeBTree) InsertAfterSplit(key, value interface{}) {
	pos := b.FindLowerBound(key)

	// 将元素向后移动，为新元素腾出位置
	// 在 Go 中，我们可以使用切片和 append 函数来处理这个问题
	b.Entries = append(b.Entries[:pos+1], b.Entries[pos:]...) // 复制 pos 位置后的切片
	// 在找到的位置插入新的键值对
	b.Entries[pos] = Entry{Key: key, Value: value}
	// 更新元素计数
	b.LNode.count++
}

func (b *LNodeBTree) Split(splitKey interface{}, key interface{}, value interface{}, version interface{}) *LNodeBTree {
	half := len(b.Entries) / 2
	splitKey := b.Entries[half-1].Key // 确定拆分键
	// 创建新的兄弟节点
	newLeaf := NewLNodeBTreeWithSibling(b.LNode.siblingPtr, b.LNode.count, b.LNode.level)
	newLeaf.LNode.HighKey = b.LNode.HighKey
	copy(newLeaf.Entries, b.Entries[half:]) // 拷贝后半部分到新节点
	b.LNode.siblingPtr = &newLeaf.LNode.Node
	b.LNode.HighKey = splitKey
	b.LNode.count = half
	// 根据键值确定插入位置
	if compareIntKeys(splitKey, key) < 0 {
		newLeaf.InsertAfterSplit(key, value)
	} else {
		b.InsertAfterSplit(key, value)
	}
	siblingPtr := newLeaf.LNode.siblingPtr
	if siblingPtr != nil && siblingPtr.Behavior.getLNodeType() == HashNode {
		siblingPtr.set = newLeaf
	}

	return newLeaf
}

func (b *LNodeBTree) Update(key interface{}, value interface{}, version uint64) int {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeBTree) Remove(key interface{}, version uint64) int {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeBTree) Find(key interface{}) (interface{}, bool) {
	//TODO implement me
	// 假设 LEAF_BTREE_SIZE 是一个全局常量
	if LeafHashSize < 2048 {
		return b.findLinear(key)
	} else {
		return b.findBinary(key)
	}
}

func (b *LNodeBTree) RangeLookUp(key interface{}, buf *[]interface{}, count int, searchRange int, continued bool) int {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeBTree) Print() {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeBTree) SanityCheck(key interface{}, first bool) {
	//TODO implement me
	fmt.Println("LNodeBTree 打印 SanityCheck")
	panic("implement me")
}

func (b *LNodeBTree) lowerboundLinear(key interface{}) int {
	for i := 0; i < len(b.Entries); i++ {
		if key.(int) <= b.Entries[i].Key.(int) {
			return i
		}
	}
	return len(b.Entries)
}

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

// compareKeys 比较两个键，需要根据键的实际类型进行具体实现
func compareIntKeys(key1, key2 interface{}) int {
	// 示例实现，假设键类型为 int
	k1 := key1.(int)
	k2 := key2.(int)
	if k1 < k2 {
		return -1
	} else if k1 > k2 {
		return 1
	}
	return 0
}
