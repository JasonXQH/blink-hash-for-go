package blinkhash

import (
	"fmt"
	"unsafe"
)

// INode 结构在 Go 中模仿 inode_t 的功能。
// 由于go中不存在泛型，需要指出的是entry中的value值必须是node类型的
type INode struct {
	Cardinality int
	Node                    // 嵌入 Node 结构以复用基本字段
	HighKey     interface{} // 最高键
	Entries     []Entry     // 条目切片
}

func (in *INode) getType() NodeType {
	return INNERNode
}

// NewINode 创建并初始化一个 INode 实例，适用于各种构造场景
func NewINode(level int, highKey interface{}, sibling, left NodeInterface) *INode {
	cardinality := int((PageSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(new(interface{})))) / int(unsafe.Sizeof(Entry{})))
	inode := &INode{
		Node: Node{
			level:       level,
			siblingPtr:  sibling,
			leftmostPtr: left,
		},
		Cardinality: cardinality,
		HighKey:     highKey,
		Entries:     make([]Entry, cardinality),
	}
	return inode
}

// NewINodeSimple 传入 level 的简单构造函数
func NewINodeForInsertInBatch(level int) *INode {
	cardinality := int((PageSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(new(interface{})))) / int(unsafe.Sizeof(Entry{})))
	return &INode{
		Node: Node{
			level: level,
		},
		Cardinality: cardinality,
		Entries:     make([]Entry, 0, cardinality), // 初始化为空但有容量
	}
}

// NewINodeForHeightGrowth 用于树高度增加时的构造函数
func NewINodeForHeightGrowth(splitKey interface{}, left, right, sibling NodeInterface, level int, highKey interface{}) *INode {
	inode := &INode{
		Node: Node{
			level:       level,
			siblingPtr:  sibling,
			leftmostPtr: left,
			count:       1, // 只有一个条目
		},
		HighKey: highKey,
		Entries: make([]Entry, 1), // 只有一个条目
	}
	// 初始化条目
	inode.Entries[0] = Entry{
		Key:   splitKey,
		Value: right,
	}
	return inode
}

// NewINodeForSplit 用于节点分裂时的构造函数
func NewINodeForSplit(sibling NodeInterface, count int, left NodeInterface, level int, highKey interface{}) *INode {
	return &INode{
		Node: Node{
			siblingPtr:  sibling,
			leftmostPtr: left,
			count:       count,
			level:       level,
		},
		HighKey: highKey,
		Entries: make([]Entry, count),
	}
}

// IsFull 检查节点是否已满
func (in *INode) IsFull() bool {
	return in.count == cap(in.Entries)
}

// findLowerBound 在有序切片中线性搜索，找到第一个不小于给定键的元素位置
func (i *INode) findLowerBound(key interface{}) int {
	for index, entry := range i.Entries {
		// 假设 Key 是 int 类型进行比较，根据实际类型调整
		if entry.Key.(int) >= key.(int) {
			return index
		}
	}
	return len(i.Entries) // 如果所有键都小于给定键，返回切片长度
}

// ScanNode 根据提供的键扫描并返回对应的节点
func (in *INode) ScanNode(key interface{}) *Node {
	if in.siblingPtr != nil {
		// 假设 HighKey 是 int 类型，确保 key 也是 int 类型
		keyInt, ok := key.(int)
		if !ok {
			return nil // 或其他错误处理
		}
		highKeyInt, ok := in.HighKey.(int)
		if !ok {
			return nil // 或其他错误处理
		}

		if highKeyInt < keyInt {
			// 使用类型断言来检查 siblingPtr 是否实际上是 *Node 类型
			if node, ok := in.siblingPtr.(*Node); ok {
				return node
			}
			return nil // 或其他错误处理
		}
	}
	//TODO实现继续
	return nil
}

// Insert 插入新的键值对到节点中，保持键的排序
func (i *INode) Insert(key interface{}, value *Node, left *Node) {
	// 查找插入位置
	pos := i.findLowerBound(key)

	// 插入前先扩展切片以留出空间
	i.Entries = append(i.Entries[:pos+2], i.Entries[pos+1:]...)

	// 更新左节点和当前键值
	i.Entries[pos].Value = left
	i.Entries[pos+1] = Entry{Key: key, Value: value}

	// 增加节点计数
	i.count++
}

// InsertWithLeft 插入新的键值对到节点中，并设置左侧节点
func (i *INode) InsertWithLeft(key interface{}, value, left *Node) {
	pos := i.findLowerBound(key)
	// 在指定位置插入新元素，需要先扩展切片
	i.Entries = append(i.Entries[:pos+1], append([]Entry{{Key: key, Value: value}}, i.Entries[pos+1:]...)...)
	i.Entries[pos].Value = left
}

// Split 分割当前节点，返回新节点
// Split 分裂当前 INode 节点，返回新的节点和分裂键
func (inode *INode) Split() (*INode, interface{}) {
	half := inode.count / 2
	splitKey := inode.Entries[half].Key

	// 创建新节点，容量为剩余条目数
	newCount := inode.count - half - 1
	newNode := NewINodeForSplit(
		inode.siblingPtr,
		newCount,
		inode.Entries[half].Value.(*Node),
		inode.level,
		inode.HighKey,
	)

	// 复制后一半的条目到新节点
	copy(newNode.Entries, inode.Entries[half+1:])

	// 更新当前节点的 sibling 和 highKey
	inode.siblingPtr = &newNode.Node
	inode.HighKey = splitKey
	inode.count = half

	return newNode, splitKey
}

// BatchMigrate 批量迁移条目到当前 INode 节点
func (inode *INode) BatchMigrate(migrate []Entry, migrateIdx *int, migrateNum int) {
	// 更新 leftmost_ptr
	inode.leftmostPtr = migrate[*migrateIdx].Value.(*Node)
	*migrateIdx++

	// 计算需要复制的条目数
	copyNum := migrateNum - *migrateIdx

	// 批量复制条目到当前节点
	inode.Entries = append(inode.Entries[:inode.count], migrate[*migrateIdx:*migrateIdx+copyNum]...)
	inode.count += copyNum

	// 更新 migrateIdx
	*migrateIdx += copyNum
}

// BatchKvPair 将键值对批量填充到 INode 的 Entries 中
func (inode *INode) BatchKvPair(keys []interface{}, values []*Node, idx *int, num int, batchSize int) bool {
	for inode.count < batchSize && *idx < num-1 {
		inode.Entries = append(inode.Entries, Entry{
			Key:   keys[*idx],
			Value: values[*idx],
		})
		inode.count++
		(*idx)++
	}

	if inode.count == batchSize {
		// 达到批量大小，设置 high_key 并返回 true
		inode.HighKey = keys[*idx]
		return true
	}

	// 插入最后一个键值对
	inode.Entries = append(inode.Entries, Entry{
		Key:   keys[*idx],
		Value: values[*idx],
	})
	inode.count++
	(*idx)++
	return false
}

// BatchBuffer 将缓冲区中的键值对批量填充到 INode 的 Entries 中
func (inode *INode) BatchBuffer(buf []Entry, bufIdx *int, bufNum int, batchSize int) {
	for inode.count < batchSize && *bufIdx < bufNum-1 {
		inode.Entries = append(inode.Entries, Entry{
			Key:   buf[*bufIdx].Key,
			Value: buf[*bufIdx].Value,
		})
		inode.count++
		(*bufIdx)++
	}

	if inode.count == batchSize {
		// 达到批量大小，设置 high_key 并返回
		inode.HighKey = buf[*bufIdx].Key
		return
	}

	// 插入最后一个键值对
	inode.Entries = append(inode.Entries, Entry{
		Key:   buf[*bufIdx].Key,
		Value: buf[*bufIdx].Value,
	})
	inode.count++
	(*bufIdx)++
}

// BatchInsertLastLevelWithMigrationAndMovement 批量插入到叶子节点，包括迁移和缓冲区处理
func (inode *INode) BatchInsertLastLevelWithMigrationAndMovement(
	migrate []Entry, migrateIdx *int, migrateNum int,
	keys []interface{}, values []*Node, idx *int, num int, batchSize int,
	buf []Entry, bufIdx *int, bufNum int,
) {
	fromStart := true

	// 如果还有迁移条目，优先处理
	if *migrateIdx < migrateNum {
		fromStart = false
		inode.BatchMigrate(migrate, migrateIdx, migrateNum)
	}

	// 如果还有键值对需要插入，并且当前节点未满
	if *idx < num && inode.count < batchSize {
		if fromStart {
			// 从头开始插入，更新 leftmost_ptr
			inode.leftmostPtr = values[*idx]
			(*idx)++
		}
		fromStart = false

		if *idx < num {
			// 批量插入键值对
			if inode.BatchKvPair(keys, values, idx, num, batchSize) {
				return // 如果达到批量大小，直接返回
			}

			// 处理边界情况：插入完成但需要从缓冲区处理
			if *idx == num && inode.count == batchSize && bufNum != 0 {
				inode.HighKey = buf[*bufIdx].Key
				return
			}
		}
	}

	// 如果还有缓冲区条目需要插入，并且当前节点未满
	if *bufIdx < bufNum && inode.count < batchSize {
		if fromStart {
			// 从缓冲区开始插入，更新 leftmost_ptr
			inode.leftmostPtr = buf[*bufIdx].Value.(*Node)
			(*bufIdx)++
		}
		// 批量插入缓冲区条目
		inode.BatchBuffer(buf, bufIdx, bufNum, batchSize)
	}
}

// 此版本主要处理批量插入，同时考虑起始点设置和缓冲区的批量插入
func (inode *INode) BatchInsertLastLevelWithMovement(
	keys []interface{}, values []*Node, idx *int, num int, batchSize int, // 键值对
	buf []Entry, bufIdx *int, bufNum int, // 缓冲区
) {
	fromStart := true

	// Step 1: 插入键值对
	if *idx < num {
		inode.leftmostPtr = values[*idx]
		(*idx)++
		fromStart = false

		if *idx < num {
			if inode.BatchKvPair(keys, values, idx, num, batchSize) {
				return
			}

			// 如果达到批量大小，设置 HighKey 并退出
			if *idx == num && inode.count == batchSize && bufNum != 0 {
				inode.HighKey = buf[*bufIdx].Key
				return
			}
		}
	}

	// Step 2: 从缓冲区插入条目
	if *bufIdx < bufNum && inode.count < batchSize {
		if fromStart {
			inode.leftmostPtr = buf[*bufIdx].Value.(*Node)
			(*bufIdx)++
		}
		inode.BatchBuffer(buf, bufIdx, bufNum, batchSize)
	}
}

func (inode *INode) BatchInsertLastLevel(keys []interface{}, values []*Node, num int, newNum *int,
) ([]*INode, error) {
	pos := inode.findLowerBound(keys[0])
	batchSize := int(float64(inode.Cardinality) * FillFactor)
	inplace := (inode.count + num) < inode.Cardinality
	moveNum := 0
	idx := 0
	if pos < 0 {
		moveNum = inode.count
	} else {
		moveNum = inode.count - pos - 1
	}

	if inplace { // 正常插入
		inode.moveNormalInsertion(pos, num, moveNum)
		if pos < 0 { // 更新最左指针
			inode.leftmostPtr = values[0]
		} else {
			inode.Entries[pos].Value = values[0]
		}

		for i, idx := pos+1, 0; idx < num; i, idx = i+1, idx+1 {
			inode.Entries[i].Key = keys[idx]
			inode.Entries[i].Value = values[idx]
		}
		inode.count += num
		return nil, nil
	}
	// 需要数据迁移和可能的分裂
	prevHighKey := inode.HighKey
	if pos < 0 {
		inode.leftmostPtr = values[0]
	} else {
		inode.Entries[pos].Value = values[0]
	}

	var migrate []Entry
	if batchSize < pos { // 插入到中间，需要迁移
		migrateNum := pos - batchSize
		migrate = make([]Entry, migrateNum)
		copy(migrate, inode.Entries[batchSize:pos])

		buf := make([]Entry, moveNum)
		copy(buf, inode.Entries[pos+1:pos+1+moveNum])
		inode.count = batchSize

		totalNum := num + moveNum + migrateNum
		lastChunk := 0
		numerator := totalNum / (batchSize + 1)
		remains := totalNum % (batchSize + 1)
		newNumValue := *newNum
		inode.CalculateNodeNum(totalNum, &numerator, &remains, &lastChunk, newNum, batchSize)

		newNodes := make([]*INode, newNumValue)
		for i := range newNodes {
			//newNodes[i] = &INode{Node: Node{level: inode.level}, Cardinality: inode.Cardinality}
			newNodes[i] = NewINodeForInsertInBatch(inode.level)
		}

		oldSibling := inode.siblingPtr
		inode.siblingPtr = &newNodes[0].Node
		for i := 0; i < newNumValue-1; i++ {
			newNodes[i].siblingPtr = &newNodes[i+1].Node
			newNodes[i].BatchInsertLastLevelWithMigrationAndMovement(migrate, &pos, migrateNum, keys, values, &idx, num, batchSize, buf, &pos, moveNum)
		}
		newNodes[newNumValue-1].siblingPtr = oldSibling
		newNodes[newNumValue-1].BatchInsertLastLevelWithMigrationAndMovement(migrate, &pos, migrateNum, keys, values, &idx, num, lastChunk, buf, &pos, moveNum)
		newNodes[newNumValue-1].HighKey = prevHighKey
		return newNodes, nil
	} else {
		moveIdx := 0
		//....
		buf := make([]Entry, moveNum)
		copy(buf, inode.Entries[pos+1:pos+1+moveNum])
		idx := 0
		for i := pos + 1; i < batchSize && idx < num; i, idx = i+1, idx+1 {
			inode.Entries[i].Key = keys[idx]
			inode.Entries[i].Value = values[idx]
		}
		inode.count += idx - moveNum - 1
		for ; inode.count < batchSize; inode.count, moveIdx = inode.count+1, moveIdx+1 {
			inode.Entries[inode.count].Key = buf[moveIdx].Key
			inode.Entries[inode.count].Value = buf[moveIdx].Value
		}
		var newHighKey interface{}
		if idx < num {
			newHighKey = keys[idx]
		} else {
			newHighKey = buf[moveIdx].Key
		}

		totalNum := num - idx + moveNum - moveIdx
		lastChunk := 0
		numerator := totalNum / (batchSize + 1)
		remains := totalNum % (batchSize + 1)
		newNumValue := *newNum
		inode.CalculateNodeNum(totalNum, &numerator, &remains, &lastChunk, newNum, batchSize)

		newNodes := make([]*INode, newNumValue)
		for i := range newNodes {
			newNodes[i] = NewINodeForInsertInBatch(inode.level)
		}

		oldSibling := inode.siblingPtr
		inode.siblingPtr = &newNodes[0].Node
		for i := 0; i < newNumValue-1; i++ {
			newNodes[i].siblingPtr = &newNodes[i+1].Node
			newNodes[i].BatchInsertLastLevelWithMovement(keys, values, &idx, num, batchSize, buf, &moveIdx, moveNum)
		}
		newNodes[newNumValue-1].siblingPtr = oldSibling
		newNodes[newNumValue-1].BatchInsertLastLevelWithMovement(keys, values, &idx, num, lastChunk, buf, &moveIdx, moveNum)
		newNodes[newNumValue-1].HighKey = newHighKey
		return newNodes, nil
	}
}

func (inode *INode) CalculateNodeNum(totalNum int, numerator, remains *int, lastChunk, newNum *int, batchSize int) {
	if *numerator == 0 { // 只需要一个新节点
		*newNum = 1
		*lastChunk = *remains
		return
	}

	// 需要多个新节点
	if *remains == 0 { // 恰好匹配
		*newNum = *numerator
		*lastChunk = batchSize
	} else {
		if *remains < inode.Cardinality-batchSize { // 可以挤进最后一个新节点
			*newNum = *numerator
			*lastChunk = batchSize + *remains
		} else { // 需要额外的新节点
			*newNum = *numerator + 1
			*lastChunk = *remains
		}
	}
}
func (inode *INode) InsertForRoot(keys []interface{}, values []*Node, left *Node, num int) {
	inode.leftmostPtr = left
	for i := 0; i < num; i++ {
		inode.Entries = append(inode.Entries, Entry{
			Key:   keys[i],
			Value: values[i],
		})
		inode.count++
	}
}

func (inode *INode) moveNormalInsertion(pos, num, moveNum int) {
	// 使用 Go 的切片操作来模拟 C++ 的 `memmove`
	copy(inode.Entries[pos+num+1:], inode.Entries[pos+1:pos+1+moveNum])
}

func (inode *INode) RightmostPtr() *Node {
	if inode.count == 0 {
		return nil
	}
	return inode.Entries[inode.count-1].Value.(*Node)
}

func (inode *INode) Print() {
	fmt.Printf("LeftmostPtr: %v\n", inode.leftmostPtr)
	for i, entry := range inode.Entries[:inode.count] {
		fmt.Printf("[%d] Key: %v, Value: %v\n", i, entry.Key, entry.Value)
	}
	fmt.Printf("HighKey: %v\n\n", inode.HighKey)
}

func (inode *INode) SanityCheck(prevHighKey interface{}, first bool) {
	// 检查键的顺序是否正确
	for i := 0; i < inode.count-1; i++ {
		for j := i + 1; j < inode.count; j++ {
			if inode.Entries[i].Key.(int) > inode.Entries[j].Key.(int) {
				fmt.Printf("INode: Key order is not preserved!!\n")
				fmt.Printf("[%d].Key: %v\t[%d].Key: %v at node %p\n", i, inode.Entries[i].Key, j, inode.Entries[j].Key, inode)
			}
		}
	}

	// 检查每个键是否符合 highKey 和 prevHighKey 的约束
	for i := 0; i < inode.count; i++ {
		if inode.siblingPtr != nil && inode.Entries[i].Key.(int) > inode.HighKey.(int) {
			fmt.Printf("INode: %d (%v) is higher than high key %v at node %p\n", i, inode.Entries[i].Key, inode.HighKey, inode)
		}
		if !first {
			if inode.siblingPtr != nil && inode.Entries[i].Key.(int) <= prevHighKey.(int) {
				fmt.Printf("INode: %d (%v) is smaller than previous high key %v\n", i, inode.Entries[i].Key, prevHighKey)
				fmt.Printf("--------- Node Address: %p, Current HighKey: %v\n", inode, inode.HighKey)
			}
		}
	}
	// 如果有 sibling 节点，递归检查下一个节点
	if inode.siblingPtr != nil {
		siblingPtr := inode.siblingPtr
		siblingPtr.SanityCheck(inode.HighKey, false)
	}
}

func (inode *INode) BatchInsertWithMigrationAndMoveMent(
	migrate []Entry, migrateIdx *int, migrateNum int,
	keys []interface{}, values []*Node, idx *int, num int,
	batchSize int, buf []Entry, bufIdx *int, bufNum int,
) {
	fromStart := true

	// 处理迁移逻辑
	if *migrateIdx < migrateNum {
		fromStart = false
		inode.BatchMigrate(migrate, migrateIdx, migrateNum)
	}

	// 批量插入键值对
	if *idx < num && inode.count < batchSize {
		if fromStart {
			inode.leftmostPtr = values[*idx]
			(*idx)++
		}
		fromStart = false
		if *idx < num {
			if inode.BatchKvPair(keys, values, idx, num, batchSize) {
				return
			}
			if *idx == num && inode.count == batchSize && bufNum != 0 {
				inode.HighKey = buf[*bufIdx].Key
			}
		}
	}

	// 从缓冲区插入键值对
	if *bufIdx < bufNum && inode.count < batchSize {
		if fromStart {
			inode.leftmostPtr = buf[*bufIdx].Value.(*Node)
			(*bufIdx)++
		}
		inode.BatchBuffer(buf, bufIdx, bufNum, batchSize)
	}
}

func (inode *INode) BatchInsertWithMovement(
	keys []interface{}, values []*Node, idx *int, num int,
	batchSize int, buf []Entry, bufIdx *int, bufNum int,
) {
	fromStart := true

	// 批量插入键值对
	if *idx < num {
		fromStart = false
		inode.leftmostPtr = values[*idx]
		(*idx)++
		if *idx < num {
			if inode.BatchKvPair(keys, values, idx, num, batchSize) {
				return
			}
			if *idx == num && inode.count == batchSize && bufNum != 0 {
				inode.HighKey = buf[*bufIdx].Key
			}
		}
	}

	// 从缓冲区插入键值对
	if *bufIdx < bufNum && inode.count < batchSize {
		if fromStart {
			inode.leftmostPtr = buf[*bufIdx].Value.(*Node)
			(*bufIdx)++
		}
		inode.BatchBuffer(buf, bufIdx, bufNum, batchSize)
	}
}

func (inode *INode) BatchInsert(
	keys []interface{}, values []*Node, num int, newNum *int,
) []*INode {
	pos := inode.findLowerBound(keys[0])
	batchSize := int(float64(inode.Cardinality) * FillFactor)
	inPlace := (inode.count + num) < inode.Cardinality
	moveNum := 0
	idx := 0

	if pos < 0 {
		moveNum = inode.count
	} else {
		moveNum = inode.count - pos - 1
	}

	// Case 1: Insert in-place
	if inPlace {
		inode.moveNormalInsertion(pos, num, moveNum)
		for i := pos + 1; i < pos+num+1; i, idx = i+1, idx+1 {
			inode.Entries[i] = Entry{Key: keys[idx], Value: values[idx]}
		}
		inode.count += num
		return nil
	}

	// Case 2: Split the node for insertion
	if batchSize < pos {
		// Migrate entries
		migrateNum := pos - batchSize
		migrate := make([]Entry, migrateNum)
		copy(migrate, inode.Entries[batchSize:batchSize+migrateNum])

		// Buffer entries to be moved
		buf := make([]Entry, moveNum)
		copy(buf, inode.Entries[pos+1:pos+1+moveNum])

		// Adjust current node
		inode.count = batchSize
		totalNum := num + moveNum + migrateNum
		lastChunk, numerator, remains := 0, 0, 0
		inode.CalculateNodeNum(totalNum, &numerator, &remains, &lastChunk, newNum, batchSize)

		// Create new sibling nodes
		newNodes := make([]*INode, *newNum)
		for i := 0; i < *newNum; i++ {
			newNodes[i] = NewINodeForInsertInBatch(inode.level)
		}

		// Adjust sibling pointers
		oldSibling := inode.siblingPtr
		inode.siblingPtr = &newNodes[0].Node

		// Insert data into sibling nodes
		migrateIdx, moveIdx := 0, 0
		prevHighKey := inode.HighKey
		inode.HighKey = migrate[migrateIdx].Key

		for i := 0; i < *newNum-1; i++ {
			newNodes[i].siblingPtr = &newNodes[i+1].Node
			newNodes[i].BatchInsertWithMigrationAndMoveMent(
				migrate, &migrateIdx, migrateNum, keys, values, &idx, num,
				batchSize, buf, &moveIdx, moveNum,
			)
		}

		// Last node adjustments
		newNodes[*newNum-1].siblingPtr = oldSibling
		newNodes[*newNum-1].HighKey = prevHighKey
		newNodes[*newNum-1].BatchInsertWithMigrationAndMoveMent(
			migrate, &migrateIdx, migrateNum, keys, values, &idx, num,
			lastChunk, buf, &moveIdx, moveNum,
		)

		return newNodes
	}

	// Case 3: Insert into the middle without migration
	moveIdx := 0
	buf := make([]Entry, moveNum)
	copy(buf, inode.Entries[pos+1:pos+1+moveNum])

	// Fill the current node
	for i := pos + 1; i < batchSize && idx < num; i, idx = i+1, idx+1 {
		inode.Entries[i] = Entry{Key: keys[idx], Value: values[idx]}
	}

	inode.count += (idx - moveNum)
	for inode.count < batchSize {
		inode.Entries[inode.count] = buf[moveIdx]
		inode.count++
		moveIdx++
	}

	prevHighKey := inode.HighKey
	if idx < num {
		inode.HighKey = keys[idx]
	} else {
		inode.HighKey = buf[moveIdx].Key
	}

	totalNum := num - idx + moveNum - moveIdx
	lastChunk, numerator, remains := 0, 0, 0
	inode.CalculateNodeNum(totalNum, &numerator, &remains, &lastChunk, newNum, batchSize)

	// Create new sibling nodes
	newNodes := make([]*INode, *newNum)
	for i := 0; i < *newNum; i++ {
		newNodes[i] = NewINodeForInsertInBatch(inode.level)
	}

	// Adjust sibling pointers
	oldSibling := inode.siblingPtr
	inode.siblingPtr = &newNodes[0].Node

	// Insert data into sibling nodes
	for i := 0; i < *newNum-1; i++ {
		newNodes[i].siblingPtr = &newNodes[i+1].Node
		newNodes[i].BatchInsertWithMovement(
			keys, values, &idx, num, batchSize, buf, &moveIdx, moveNum,
		)
	}

	// Last node adjustments
	newNodes[*newNum-1].siblingPtr = oldSibling
	newNodes[*newNum-1].HighKey = prevHighKey
	newNodes[*newNum-1].BatchInsertWithMovement(
		keys, values, &idx, num, lastChunk, buf, &moveIdx, moveNum,
	)

	return newNodes
}
