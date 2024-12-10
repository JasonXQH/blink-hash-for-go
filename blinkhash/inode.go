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

func (in *INode) GetHighKey() interface{} {
	return in.HighKey
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
		Entries:     make([]Entry, 0, cardinality),
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

// FindLowerBound findLowerBound 在有序切片中线性搜索，找到第一个不小于给定键的元素位置
func (in *INode) FindLowerBound(key interface{}) int {
	keyInt, ok := key.(int)
	if !ok {
		panic("FindLowerBoundLinear: key is not of type int")
	}

	for index, entry := range in.Entries[:in.count] {
		if entry.Key.(int) >= keyInt {
			return index - 1
		}
	}
	return in.count - 1
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
	idx := in.FindLowerBound(key)
	if idx >= 0 && idx < in.count {
		if node, ok := in.Entries[idx].Value.(*Node); ok {
			return node
		}
	} else {
		if node, ok := in.leftmostPtr.(*Node); ok {
			return node
		}
	}
	return nil
}

// Insert 插入新的键值对到节点中，保持键的排序
func (in *INode) Insert(key interface{}, value interface{}, version uint64) int {
	// 查找插入位置
	pos := in.FindLowerBound(key)

	// 确保 pos 不超过当前条目数
	//if pos < 0 || pos > len(in.Entries) {
	//	return fmt.Errorf("invalid insert position: %d", pos)
	//}

	// 将后续元素向后移动一位
	in.Entries = append(in.Entries, Entry{}) // 扩展切片以防止越界
	copy(in.Entries[pos+2:], in.Entries[pos+1:])
	in.Entries[pos+1] = Entry{Key: key, Value: value}
	// 更新计数
	in.count++

	// 更新 HighKey
	if in.count > 0 {
		in.HighKey = in.Entries[in.count-1].Key
	}

	return InsertSuccess
}

// InsertWithLeft 插入新的键值对到节点中，并设置左侧节点
func (in *INode) InsertWithLeft(key interface{}, value *Node, left *Node) error {
	// 查找插入位置
	pos := in.FindLowerBound(key)

	// 确保 pos 不超过当前条目数
	//if pos < 0 || pos > len(in.Entries) {
	//	return fmt.Errorf("invalid insert position: %d", pos)
	//}

	// 在指定位置插入新元素
	in.Entries = append(in.Entries, Entry{}) // 扩展切片以防止越界
	copy(in.Entries[pos+2:], in.Entries[pos+1:])
	in.Entries[pos+1] = Entry{Key: key, Value: value}
	in.Entries[pos].Value = left // 设置左侧指针

	// 增加节点计数
	in.count++

	// 更新 HighKey
	if in.count > 0 {
		in.HighKey = in.Entries[in.count-1].Key
	}

	return nil
}

// Split 分裂当前 INode 节点，返回新的节点和分裂键
func (in *INode) Split() (*INode, interface{}) {
	half := in.count / 2
	splitKey := in.Entries[half].Key

	// 创建新节点，容量为剩余条目数
	newCount := in.count - half - 1
	newNode := NewINodeForSplit(
		in.siblingPtr,
		newCount,
		in.Entries[half].Value.(*Node),
		in.level,
		in.HighKey,
	)

	// 复制后一半的条目到新节点
	copy(newNode.Entries, in.Entries[half+1:])

	// 更新当前节点的 sibling 和 highKey
	in.siblingPtr = &newNode.Node
	in.HighKey = splitKey
	in.count = half

	return newNode, splitKey
}

// BatchMigrate 批量迁移条目到当前 INode 节点
// 返回更新后的 migrateIdx 和错误（如果有）
func (in *INode) BatchMigrate(migrate []Entry, migrateIdx int, migrateNum int) (int, error) {
	if migrateIdx >= migrateNum {
		return migrateIdx, fmt.Errorf("migrateIdx out of range")
	}

	// 更新 leftmost_ptr
	in.leftmostPtr = migrate[migrateIdx].Value.(*Node)
	migrateIdx++

	// 计算需要复制的条目数
	copyNum := migrateNum - migrateIdx
	if migrateIdx+copyNum > len(migrate) {
		return migrateIdx, fmt.Errorf("copyNum exceeds migrate length")
	}

	// 批量复制条目到当前节点
	in.Entries = append(in.Entries[:in.count], migrate[migrateIdx:migrateIdx+copyNum]...)
	in.count += copyNum
	migrateIdx += copyNum

	return migrateIdx, nil
}

// BatchKvPair 将键值对批量填充到 INode 的 Entries 中
// 返回更新后的 idx, 是否达到 batchSize, 和错误（如果有）
func (in *INode) BatchKvPair(keys []interface{}, values []*Node, idx int, num int, batchSize int) (int, bool, error) {
	for in.count < batchSize && idx < num-1 {
		in.Entries = append(in.Entries, Entry{
			Key:   keys[idx],
			Value: values[idx],
		})
		in.count++
		idx++
	}

	if in.count == batchSize {
		if idx >= num {
			return idx, false, fmt.Errorf("idx out of range when setting HighKey")
		}
		// 达到批量大小，设置 high_key 并返回 true
		in.HighKey = keys[idx]
		return idx, true, nil
	}

	if idx >= num {
		return idx, false, fmt.Errorf("idx out of range when inserting last key-value pair")
	}

	// 插入最后一个键值对
	in.Entries = append(in.Entries, Entry{
		Key:   keys[idx],
		Value: values[idx],
	})
	in.count++
	idx++
	return idx, false, nil
}

// BatchBuffer 将缓冲区中的键值对批量填充到 INode 的 Entries 中
// 返回更新后的 bufIdx, 是否达到 batchSize, 和错误（如果有）
func (in *INode) BatchBuffer(buf []Entry, bufIdx int, bufNum int, batchSize int) (int, bool, error) {
	for in.count < batchSize && bufIdx < bufNum-1 {
		in.Entries = append(in.Entries, Entry{
			Key:   buf[bufIdx].Key,
			Value: buf[bufIdx].Value,
		})
		in.count++
		bufIdx++
	}

	if in.count == batchSize {
		if bufIdx >= bufNum {
			return bufIdx, false, fmt.Errorf("bufIdx out of range when setting HighKey")
		}
		// 达到批量大小，设置 high_key 并返回
		in.HighKey = buf[bufIdx].Key
		return bufIdx, true, nil
	}

	if bufIdx >= bufNum {
		return bufIdx, false, fmt.Errorf("bufIdx out of range when inserting last key-value pair")
	}

	// 插入最后一个键值对
	in.Entries = append(in.Entries, Entry{
		Key:   buf[bufIdx].Key,
		Value: buf[bufIdx].Value,
	})
	in.count++
	bufIdx++
	return bufIdx, false, nil
}

// BatchInsertLastLevelWithMigrationAndMovement 批量插入到叶子节点，包括迁移和缓冲区处理
// BatchInsertLastLevelWithMigrationAndMovement 批量插入到叶子节点，包括迁移和缓冲区处理
// 返回更新后的 migrateIdx, bufIdx 和错误（如果有）
func (in *INode) BatchInsertLastLevelWithMigrationAndMovement(
	migrate []Entry, migrateIdx int, migrateNum int,
	keys []interface{}, values []*Node, idx int, num int, batchSize int,
	buf []Entry, bufIdx int, bufNum int,
) (int, int, error) {
	fromStart := true

	// 如果还有迁移条目，优先处理
	if migrateIdx < migrateNum {
		fromStart = false
		var err error
		migrateIdx, err = in.BatchMigrate(migrate, migrateIdx, migrateNum)
		if err != nil {
			return migrateIdx, bufIdx, err
		}
	}

	// 如果还有键值对需要插入，并且当前节点未满
	if idx < num && in.count < batchSize {
		if fromStart {
			// 从头开始插入，更新 leftmost_ptr
			in.leftmostPtr = values[idx]
			idx++
		}
		fromStart = false

		if idx < num {
			// 批量插入键值对
			newIdx, reached, err := in.BatchKvPair(keys, values, idx, num, batchSize)
			if err != nil {
				return migrateIdx, bufIdx, err
			}
			idx = newIdx
			if reached {
				return migrateIdx, bufIdx, nil // 如果达到批量大小，直接返回
			}

			// 处理边界情况：插入完成但需要从缓冲区处理
			if idx == num && in.count == batchSize && bufNum != 0 {
				if bufIdx >= bufNum {
					return migrateIdx, bufIdx, fmt.Errorf("bufIdx out of range when setting HighKey")
				}
				in.HighKey = buf[bufIdx].Key
				return migrateIdx, bufIdx, nil
			}
		}
	}

	// 如果还有缓冲区条目需要插入，并且当前节点未满
	if bufIdx < bufNum && in.count < batchSize {
		if fromStart {
			// 从缓冲区开始插入，更新 leftmost_ptr
			in.leftmostPtr = buf[bufIdx].Value.(*Node)
			bufIdx++
		}
		// 批量插入缓冲区条目
		var err error
		bufIdx, _, err = in.BatchBuffer(buf, bufIdx, bufNum, batchSize)
		if err != nil {
			return migrateIdx, bufIdx, err
		}
	}

	return migrateIdx, bufIdx, nil
}

// BatchInsertLastLevelWithMovement 此版本主要处理批量插入，同时考虑起始点设置和缓冲区的批量插入
// BatchInsertLastLevelWithMovement 批量插入到叶子节点，考虑迁移和缓冲区
// 返回更新后的 idx, bufIdx 和错误（如果有）
func (in *INode) BatchInsertLastLevelWithMovement(
	keys []interface{}, values []*Node, idx int, num int, batchSize int, // 键值对
	buf []Entry, bufIdx int, bufNum int, // 缓冲区
) (int, int, error) {
	fromStart := true

	// Step 1: 插入键值对
	if idx < num {
		in.leftmostPtr = values[idx]
		idx++
		fromStart = false

		if idx < num {
			// 批量插入键值对
			newIdx, reached, err := in.BatchKvPair(keys, values, idx, num, batchSize)
			if err != nil {
				return idx, bufIdx, err
			}
			idx = newIdx
			if reached {
				return idx, bufIdx, nil
			}

			// 如果达到批量大小，设置 HighKey 并退出
			if idx == num && in.count == batchSize && bufNum != 0 {
				if bufIdx >= bufNum {
					return idx, bufIdx, fmt.Errorf("bufIdx out of range when setting HighKey")
				}
				in.HighKey = buf[bufIdx].Key
				return idx, bufIdx, nil
			}
		}
	}

	// Step 2: 从缓冲区插入条目
	if bufIdx < bufNum && in.count < batchSize {
		if fromStart {
			// 从缓冲区开始插入，更新 leftmost_ptr
			in.leftmostPtr = buf[bufIdx].Value.(*Node)
			bufIdx++
		}
		// 批量插入缓冲区条目
		var err error
		bufIdx, _, err = in.BatchBuffer(buf, bufIdx, bufNum, batchSize)
		if err != nil {
			return idx, bufIdx, err
		}
	}

	return idx, bufIdx, nil
}

// BatchInsertLastLevel 批量插入到叶子节点，包括迁移和缓冲区处理
// 返回新节点集合、新Num 和错误（如果有）
func (in *INode) BatchInsertLastLevel(keys []interface{}, values []*Node, num int, batchSize int) ([]*INode, error) {
	pos := in.FindLowerBound(keys[0])
	batchSizeCalc := int(float64(in.Cardinality) * FillFactor)
	inplace := (in.count + num) < in.Cardinality
	moveNum := 0
	idx := 0
	if pos < 0 {
		moveNum = in.count
	} else {
		moveNum = in.count - pos - 1
	}

	if inplace { // 正常插入
		in.moveNormalInsertion(pos, num, moveNum)
		if pos < 0 { // 更新最左指针
			in.leftmostPtr = values[0]
		} else {
			in.Entries[pos].Value = values[0]
		}

		for i, j := pos+1, 0; j < num; i, j = i+1, j+1 {
			in.Entries[i].Key = keys[j]
			in.Entries[i].Value = values[j]
		}
		in.count += num
		return nil, nil
	}
	// 需要数据迁移和可能的分裂
	prevHighKey := in.HighKey
	if pos < 0 {
		in.leftmostPtr = values[0]
	} else {
		in.Entries[pos].Value = values[0]
	}

	var migrate []Entry
	if batchSizeCalc < pos { // 插入到中间，需要迁移
		migrateNum := pos - batchSizeCalc
		migrate = make([]Entry, migrateNum)
		copy(migrate, in.Entries[batchSizeCalc:pos])

		buf := make([]Entry, moveNum)
		copy(buf, in.Entries[pos+1:pos+1+moveNum])
		in.count = batchSizeCalc

		totalNum := num + moveNum + migrateNum
		newNum, lastChunk := in.CalculateNodeNum(totalNum, batchSizeCalc)

		newNodes := make([]*INode, newNum)
		for i := range newNodes {
			newNodes[i] = NewINodeForInsertInBatch(in.level)
		}

		oldSibling := in.siblingPtr
		in.siblingPtr = &newNodes[0].Node
		for i := 0; i < newNum-1; i++ {
			newNodes[i].siblingPtr = &newNodes[i+1].Node
			// 调用重构后的 BatchInsertLastLevelWithMigrationAndMovement
			migrateIdx, bufIdx, err := newNodes[i].BatchInsertLastLevelWithMigrationAndMovement(
				migrate, 0, migrateNum,
				keys, values, idx, num, batchSizeCalc,
				buf, 0, moveNum,
			)
			if err != nil {
				return nil, err
			}
			idx = migrateIdx
			bufIdx = bufIdx
		}
		newNodes[newNum-1].siblingPtr = oldSibling
		_, _, err := newNodes[newNum-1].BatchInsertLastLevelWithMigrationAndMovement(
			migrate, 0, migrateNum,
			keys, values, idx, num, lastChunk,
			buf, 0, moveNum,
		)
		if err != nil {
			return nil, err
		}
		newNodes[newNum-1].HighKey = prevHighKey
		return newNodes, nil
	} else {
		moveIdx := 0
		// 处理非迁移的情况
		buf := make([]Entry, moveNum)
		copy(buf, in.Entries[pos+1:pos+1+moveNum])
		idx := 0
		for i := pos + 1; i < batchSizeCalc && idx < num; i, idx = i+1, idx+1 {
			in.Entries[i].Key = keys[idx]
			in.Entries[i].Value = values[idx]
		}
		in.count += idx - moveNum - 1
		for ; in.count < batchSizeCalc; in.count, moveIdx = in.count+1, moveIdx+1 {
			in.Entries[in.count].Key = buf[moveIdx].Key
			in.Entries[in.count].Value = buf[moveIdx].Value
		}
		var newHighKey interface{}
		if idx < num {
			newHighKey = keys[idx]
		} else {
			newHighKey = buf[moveIdx].Key
		}

		totalNum := num - idx + moveNum - moveIdx
		newNum, lastChunk := in.CalculateNodeNum(totalNum, batchSizeCalc)

		newNodes := make([]*INode, newNum)
		for i := range newNodes {
			newNodes[i] = NewINodeForInsertInBatch(in.level)
		}

		oldSibling := in.siblingPtr
		in.siblingPtr = &newNodes[0].Node
		for i := 0; i < newNum-1; i++ {
			newNodes[i].siblingPtr = &newNodes[i+1].Node
			// 调用重构后的 BatchInsertLastLevelWithMovement
			_, _, err := newNodes[i].BatchInsertLastLevelWithMovement(
				keys, values, idx, num, batchSizeCalc,
				buf, moveIdx, moveNum,
			)
			if err != nil {
				return nil, err
			}
		}
		newNodes[newNum-1].siblingPtr = oldSibling
		_, _, err := newNodes[newNum-1].BatchInsertLastLevelWithMovement(
			keys, values, idx, num, lastChunk,
			buf, moveIdx, moveNum,
		)
		if err != nil {
			return nil, err
		}
		newNodes[newNum-1].HighKey = newHighKey
		return newNodes, nil
	}
}

// CalculateNodeNum 计算需要的新节点数量和最后一个节点的条目数
func (in *INode) CalculateNodeNum(totalNum int, batchSize int) (newNum int, lastChunk int) {
	numerator := totalNum / (batchSize + 1)
	remains := totalNum % (batchSize + 1)

	if numerator == 0 { // 只需要一个新节点
		newNum = 1
		lastChunk = remains
		return
	}

	// 需要多个新节点
	if remains == 0 { // 恰好匹配
		newNum = numerator
		lastChunk = batchSize
	} else {
		if remains < in.Cardinality-batchSize { // 可以挤进最后一个新节点
			newNum = numerator
			lastChunk = batchSize + remains
		} else { // 需要额外的新节点
			newNum = numerator + 1
			lastChunk = remains
		}
	}
	return
}

func (in *INode) InsertForRoot(keys []interface{}, values []*Node, left *Node, num int) {
	in.leftmostPtr = left
	for i := 0; i < num; i++ {
		in.Entries = append(in.Entries, Entry{
			Key:   keys[i],
			Value: values[i],
		})
		in.count++
	}
}

func (in *INode) moveNormalInsertion(pos, num, moveNum int) {
	// 扩展 Entries 以保证 pos+num+1 的位置有效
	if pos+num+1 > len(in.Entries) {
		in.Entries = append(in.Entries, make([]Entry, pos+num+1-len(in.Entries))...)
	}

	// 计算目标位置及需要移动的元素数量
	targetPos := pos + num + 1
	sourcePos := pos + 1

	// 如果目标位置超出了当前 Entries 的大小，就需要扩展
	if targetPos+moveNum > len(in.Entries) {
		// 需要扩展 Entries 的大小
		in.Entries = append(in.Entries, make([]Entry, targetPos+moveNum-len(in.Entries))...)
	}

	// 执行 copy 操作
	copy(in.Entries[targetPos:], in.Entries[sourcePos:sourcePos+moveNum])
}

func (in *INode) RightmostPtr() *Node {
	if in.count == 0 {
		return nil
	}
	return in.Entries[in.count-1].Value.(*Node)
}

func (in *INode) Print() {
	fmt.Printf("LeftmostPtr: %v\n", in.leftmostPtr)
	for i, entry := range in.Entries[:in.count] {
		fmt.Printf("[%d] Key: %v, Value: %v\n", i, entry.Key, entry.Value)
	}
	fmt.Printf("HighKey: %v\n\n", in.HighKey)
}

func (in *INode) SanityCheck(prevHighKey interface{}, first bool) {
	// 检查键的顺序是否正确
	for i := 0; i < in.count-1; i++ {
		for j := i + 1; j < in.count; j++ {
			if in.Entries[i].Key.(int) > in.Entries[j].Key.(int) {
				fmt.Printf("INode: Key order is not preserved!!\n")
				fmt.Printf("[%d].Key: %v\t[%d].Key: %v at node %p\n", i, in.Entries[i].Key, j, in.Entries[j].Key, in)
			}
		}
	}

	// 检查每个键是否符合 highKey 和 prevHighKey 的约束
	for i := 0; i < in.count; i++ {
		if in.siblingPtr != nil && in.Entries[i].Key.(int) > in.HighKey.(int) {
			fmt.Printf("INode: %d (%v) is higher than high key %v at node %p\n", i, in.Entries[i].Key, in.HighKey, in)
		}
		if !first {
			if in.siblingPtr != nil && in.Entries[i].Key.(int) <= prevHighKey.(int) {
				fmt.Printf("INode: %d (%v) is smaller than previous high key %v\n", i, in.Entries[i].Key, prevHighKey)
				fmt.Printf("--------- Node Address: %p, Current HighKey: %v\n", in, in.HighKey)
			}
		}
	}
	// 如果有 sibling 节点，递归检查下一个节点
	if in.siblingPtr != nil {
		siblingPtr := in.siblingPtr
		siblingPtr.SanityCheck(in.HighKey, false)
	}
}

// BatchInsertWithMigrationAndMovement 批量插入到叶子节点，包括迁移和缓冲区处理
// 返回更新后的 migrateIdx, bufIdx 和错误（如果有）
func (in *INode) BatchInsertWithMigrationAndMovement(
	migrate []Entry, migrateIdx int, migrateNum int,
	keys []interface{}, values []*Node, idx int, num int,
	batchSize int, buf []Entry, bufIdx int, bufNum int,
) (int, int, error) {
	fromStart := true

	// 如果还有迁移条目，优先处理
	if migrateIdx < migrateNum {
		fromStart = false
		var err error
		migrateIdx, err = in.BatchMigrate(migrate, migrateIdx, migrateNum)
		if err != nil {
			return migrateIdx, bufIdx, err
		}
	}

	// 如果还有键值对需要插入，并且当前节点未满
	if idx < num && in.count < batchSize {
		if fromStart {
			in.leftmostPtr = values[idx]
			idx++
		}
		fromStart = false

		if idx < num {
			var reached bool
			var err error
			idx, reached, err = in.BatchKvPair(keys, values, idx, num, batchSize)
			if err != nil {
				return migrateIdx, bufIdx, err
			}
			if reached {
				return migrateIdx, bufIdx, nil // 如果达到批量大小，直接返回
			}
			if idx == num && in.count == batchSize && bufNum != 0 {
				if bufIdx >= bufNum {
					return migrateIdx, bufIdx, fmt.Errorf("bufIdx out of range when setting HighKey")
				}
				in.HighKey = buf[bufIdx].Key
			}
		}
	}

	// 如果还有缓冲区条目需要插入，并且当前节点未满
	if bufIdx < bufNum && in.count < batchSize {
		if fromStart {
			in.leftmostPtr = buf[bufIdx].Value.(*Node)
			bufIdx++
		}
		var _ bool
		var err error
		bufIdx, _, err = in.BatchBuffer(buf, bufIdx, bufNum, batchSize)
		if err != nil {
			return migrateIdx, bufIdx, err
		}
	}

	return migrateIdx, bufIdx, nil
}

// BatchInsertWithMovement 批量插入到叶子节点，考虑迁移和缓冲区
// 返回更新后的 idx, bufIdx 和错误（如果有）
func (in *INode) BatchInsertWithMovement(
	keys []interface{}, values []*Node, idx int, num int,
	batchSize int, buf []Entry, bufIdx int, bufNum int,
) (int, int, error) {
	fromStart := true

	// 批量插入键值对
	if idx < num {
		fromStart = false
		in.leftmostPtr = values[idx]
		idx++
		if idx < num {
			var reached bool
			var err error
			idx, reached, err = in.BatchKvPair(keys, values, idx, num, batchSize)
			if err != nil {
				return idx, bufIdx, err
			}
			if reached {
				return idx, bufIdx, nil
			}
			if idx == num && in.count == batchSize && bufNum != 0 {
				if bufIdx >= bufNum {
					return idx, bufIdx, fmt.Errorf("bufIdx out of range when setting HighKey")
				}
				in.HighKey = buf[bufIdx].Key
			}
		}
	}

	// 从缓冲区插入键值对
	if bufIdx < bufNum && in.count < batchSize {
		if fromStart {
			in.leftmostPtr = buf[bufIdx].Value.(*Node)
			bufIdx++
		}
		var _ bool
		var err error
		bufIdx, _, err = in.BatchBuffer(buf, bufIdx, bufNum, batchSize)
		if err != nil {
			return idx, bufIdx, err
		}
	}

	return idx, bufIdx, nil
}

// BatchInsert 批量插入到叶子节点，包括迁移和缓冲区处理
// 返回新节点集合和错误（如果有）
func (in *INode) BatchInsert(
	keys []interface{}, values []*Node, num int,
) ([]*INode, error) {
	pos := in.FindLowerBound(keys[0])
	batchSize := int(float64(in.Cardinality) * FillFactor)
	inPlace := (in.count + num) < in.Cardinality
	moveNum := 0
	idx := 0

	if pos < 0 {
		moveNum = in.count
	} else {
		moveNum = in.count - pos - 1
	}

	// Case 1: Insert in-place
	if inPlace {
		in.moveNormalInsertion(pos, num, moveNum)
		for i, j := pos+1, 0; j < num; i, j = i+1, j+1 {
			in.Entries[i] = Entry{Key: keys[j], Value: values[j]}
		}
		in.count += num
		in.HighKey = keys[num-1]
		return nil, nil
	}

	// Case 2: Split the node for insertion
	// need insert in the middle (migrated + new kvs + moved)
	if batchSize < pos {
		// Migrate entries
		migrateNum := pos - batchSize
		migrate := make([]Entry, migrateNum)
		copy(migrate, in.Entries[batchSize:batchSize+migrateNum])

		// Buffer entries to be moved
		bufEntries := make([]Entry, moveNum)
		copy(bufEntries, in.Entries[pos+1:pos+1+moveNum])

		// Adjust current node
		in.count = batchSize

		// Calculate new nodes needed
		totalNum := num + moveNum + migrateNum
		newNum, lastChunk := in.CalculateNodeNum(totalNum, batchSize)

		// Create new sibling nodes
		newNodes := make([]*INode, newNum)
		for i := 0; i < newNum; i++ {
			newNodes[i] = NewINodeForInsertInBatch(in.level)
		}

		// Adjust sibling pointers
		oldSibling := in.siblingPtr
		in.siblingPtr = &newNodes[0].Node

		// Insert data into sibling nodes
		migrateIdx, moveIdx := 0, 0
		prevHighKey := in.HighKey
		if migrateNum > 0 {
			in.HighKey = migrate[migrateIdx].Key
		}

		for i := 0; i < newNum-1; i++ {
			newNodes[i].siblingPtr = &newNodes[i+1].Node
			var err error
			migrateIdx, moveIdx, err = newNodes[i].BatchInsertWithMigrationAndMovement(
				migrate, migrateIdx, migrateNum,
				keys, values, idx, num,
				batchSize, bufEntries, moveIdx, moveNum,
			)
			if err != nil {
				return nil, err
			}
		}

		// Last node adjustments
		newNodes[newNum-1].siblingPtr = oldSibling
		newNodes[newNum-1].HighKey = prevHighKey
		_, _, err := newNodes[newNum-1].BatchInsertWithMigrationAndMovement(
			migrate, migrateIdx, migrateNum,
			keys, values, idx, num,
			lastChunk, bufEntries, moveIdx, moveNum,
		)
		if err != nil {
			return nil, err
		}

		return newNodes, nil
	}

	// Case 3: Insert into the middle without migration
	// need insert in the middle (new_kvs + moved)
	moveIdx := 0
	bufEntries := make([]Entry, moveNum)
	copy(bufEntries, in.Entries[pos+1:pos+1+moveNum])

	// Fill the current node
	for i := pos + 1; i < batchSize && idx < num; i, idx = i+1, idx+1 {

		in.Entries[i] = Entry{Key: keys[idx], Value: values[idx]}
	}

	in.count += (idx - moveNum)
	for in.count < batchSize && moveIdx < moveNum {
		if len(in.Entries) <= in.count {
			in.Entries = append(in.Entries, Entry{}) // Expanding the slice if needed
		}
		in.Entries[in.count] = bufEntries[moveIdx]
		in.count++
		moveIdx++
	}

	prevHighKey := in.HighKey
	if idx < num {
		in.HighKey = keys[idx]
	} else if moveIdx < moveNum {
		in.HighKey = bufEntries[moveIdx].Key
	}

	// Calculate new nodes needed
	totalNum := num - idx + moveNum - moveIdx
	newNum, lastChunk := in.CalculateNodeNum(totalNum, batchSize)

	// Create new sibling nodes
	newNodes := make([]*INode, newNum)
	for i := 0; i < newNum; i++ {
		newNodes[i] = NewINodeForInsertInBatch(in.level)
	}

	// Adjust sibling pointers
	oldSibling := in.siblingPtr
	in.siblingPtr = &newNodes[0].Node

	// Insert data into sibling nodes
	for i := 0; i < newNum-1; i++ {
		newNodes[i].siblingPtr = &newNodes[i+1].Node
		var err error
		idx, moveIdx, err = newNodes[i].BatchInsertWithMovement(
			keys, values, idx, num, batchSize, bufEntries, moveIdx, moveNum,
		)
		if err != nil {
			return nil, err
		}
	}

	// Last node adjustments
	newNodes[newNum-1].siblingPtr = oldSibling
	newNodes[newNum-1].HighKey = prevHighKey
	_, _, err := newNodes[newNum-1].BatchInsertWithMovement(
		keys, values, idx, num, lastChunk, bufEntries, moveIdx, moveNum,
	)
	if err != nil {
		return nil, err
	}

	return newNodes, nil
}

func (n *INode) GetRightmostPtr() NodeInterface {
	if len(n.Entries) > 0 {
		value, ok := n.Entries[len(n.Entries)-1].Value.(NodeInterface)
		if !ok {
			panic("Inode GetRightmostPtr should be NodeInterface")
		}
		return value
	}
	return nil
}
