package blinkhash

import (
	"fmt"
)

// INode 结构在 Go 中模仿 inode_t 的功能。
// 由于go中不存在泛型，需要指出的是entry中的value值必须是node类型的
type INode struct {
	Cardinality int
	Node                    // 嵌入 Node 结构以复用基本字段
	HighKey     interface{} // 最高键
	Entries     []Entry     // 条目切片
	Type        NodeType
}

func (in *INode) GetHighKey() interface{} {
	return in.HighKey
}
func (in *INode) SetHighKey(key interface{}) {
	in.HighKey = key
}

// NewINode 创建并初始化一个 INode 实例，适用于各种构造场景
func NewINode(level int, highKey interface{}, sibling, left NodeInterface) *INode {
	inode := &INode{
		Node: Node{
			level:       level,
			siblingPtr:  sibling,
			leftmostPtr: left,
		},
		Type:        INNERNode,
		Cardinality: INodeCardinality,
		HighKey:     highKey,
		Entries:     make([]Entry, INodeCardinality),
	}
	return inode
}

func NewINodeFromLeaves(node NodeInterface) *INode {
	inode := &INode{
		Node: Node{
			level:       node.GetLevel(),
			siblingPtr:  node.GetSiblingPtr(),
			leftmostPtr: node.GetLeftmostPtr(),
		},
		Type:        INNERNode,
		Cardinality: INodeCardinality,
		HighKey:     node.GetHighKey(),
		Entries:     node.GetEntries(),
	}
	return inode
}

// NewINodeSimple 传入 level 的简单构造函数
func NewINodeForInsertInBatch(level int) *INode {
	return &INode{
		Node: Node{
			level:       level,
			siblingPtr:  nil,
			leftmostPtr: nil,
		},
		Type:        INNERNode,
		Cardinality: INodeCardinality,
		HighKey:     nil,
		Entries:     make([]Entry, 0, INodeCardinality), // 初始化为空但有容量
	}
}

// NewINodeForHeightGrowth 用于树高度增加时的构造函数
func NewINodeForHeightGrowth(key interface{}, left, right, sibling NodeInterface, level int, highKey interface{}) *INode {
	inode := &INode{
		Node: Node{
			level:       level,
			siblingPtr:  sibling,
			leftmostPtr: left,
			count:       1, // 只有一个条目
		},
		Cardinality: INodeCardinality,
		Type:        INNERNode,
		HighKey:     highKey,
		Entries:     make([]Entry, 1, INodeCardinality), // 只有一个条目
	}
	// 初始化条目
	inode.Entries[0] = Entry{
		Key:   key,
		Value: right,
	}
	return inode
}

// NewINodeForSplit 用于节点分裂时的构造函数
func NewINodeForSplit(sibling NodeInterface, count int32, left NodeInterface, level int, highKey interface{}) *INode {
	return &INode{
		Node: Node{
			siblingPtr:  sibling,
			leftmostPtr: left,
			count:       count,
			level:       level,
		},
		Type:        INNERNode,
		Cardinality: INodeCardinality,
		HighKey:     highKey,
		Entries:     make([]Entry, count, INodeCardinality),
	}
}

// IsFull 检查节点是否已满
func (in *INode) IsFull() bool {
	return in.count == int32(in.Cardinality)
}

func (in *INode) GetNode() *Node {
	return &in.Node
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
	return int(in.count - 1)
}

// ScanNode 根据提供的键扫描并返回对应的节点
func (in *INode) ScanNode(key interface{}) NodeInterface {
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
		//inode的最大highKey都小于要插入的Key，那么就要去右侧sibling节点找了
		if highKeyInt < keyInt {
			// 使用类型断言来检查 siblingPtr 是否实际上是 *Node 类型
			if node, ok := in.siblingPtr.(NodeInterface); ok {
				return node
			}
			return nil // 或其他错误处理
		}
	}
	idx := in.FindLowerBound(key)
	if idx >= 0 && idx < int(in.count) {
		if node, ok := in.Entries[idx].Value.(NodeInterface); ok {
			return node
		}
	} else {
		if node, ok := in.leftmostPtr.(NodeInterface); ok {
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
	if pos < -1 || pos >= int(in.count) {
		panic(fmt.Sprintf("Insert: invalid position %d", pos))
	}
	if in.count >= int32(in.Cardinality) {
		return NeedSplit
	}

	//// 将后续元素向后移动一位
	in.Entries = append(in.Entries, Entry{}) // 扩展切片以防止越界
	copy(in.Entries[pos+2:], in.Entries[pos+1:])
	in.Entries[pos+1] = Entry{Key: key, Value: value}
	// 更新计数
	in.IncrementCount()

	//// Shift条目以腾出插入位置
	//copy(in.Entries[pos+2:], in.Entries[pos+1:int(in.count)])
	//in.Entries[pos+1] = Entry{Key: key, Value: value}
	//in.IncrementCount()
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
	in.IncrementCount()
	// 更新 HighKey
	if in.count > 0 {
		in.HighKey = in.Entries[in.count-1].Key
	}

	return nil
}

// Split 分裂当前 INode 节点，返回新的节点和分裂键
func (in *INode) Split() (INodeInterface, interface{}) {
	half := in.count / 2
	//half := len(in.Entries)
	splitKey := in.Entries[half].Key

	// 创建新节点，容量为剩余条目数
	newCount := in.count - half - 1
	if newCount < 0 {
		fmt.Println("newCount < 0 ,this should not happen!")
		panic("newCount < 0 ,this should not happen!")
	}
	newNode := NewINodeForSplit(
		in.siblingPtr,
		newCount,
		//内部节点的孩子，也可能是内部节点
		in.Entries[half].Value.(NodeInterface),
		in.level,
		in.HighKey,
	)

	// 复制后一半的条目到新节点
	copy(newNode.Entries, in.Entries[half+1:])

	// 更新当前节点的 sibling 和 highKey
	in.siblingPtr = newNode
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
	in.count += int32(copyNum)
	migrateIdx += copyNum

	return migrateIdx, nil
}

// BatchKvPair 将键值对批量填充到 INode 的 Entries 中
// 返回更新后的 idx, 是否达到 batchSize, 和错误（如果有）
func (in *INode) BatchKvPair(keys []interface{}, values []NodeInterface, idx int, num int, batchSize int) (int, bool, error) {
	for int(in.count) < batchSize && idx < num-1 {
		in.Entries = append(in.Entries, Entry{
			Key:   keys[idx],
			Value: values[idx],
		})
		in.IncrementCount()
		idx++
	}

	if int(in.count) == batchSize {
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
	in.IncrementCount()
	idx++
	return idx, false, nil
}

// BatchBuffer 将缓冲区中的键值对批量填充到 INode 的 Entries 中
// 返回更新后的 bufIdx, 是否达到 batchSize, 和错误（如果有）
func (in *INode) BatchBuffer(buf []Entry, bufIdx int, bufNum int, batchSize int) (int, bool) {
	// 如果缓冲区已经用完(或根本没有可插入条目)，就直接返回
	if bufIdx >= bufNum {
		// 并不是“越界错误”，而是表示没有更多要插的 entry
		return bufIdx, false
	}

	for int(in.count) < batchSize && bufIdx < bufNum-1 {
		in.Entries = append(in.Entries, Entry{
			Key:   buf[bufIdx].Key,
			Value: buf[bufIdx].Value,
		})
		in.IncrementCount()
		bufIdx++
	}

	if int(in.count) == batchSize {
		// 达到batchSize后，如果还有剩余buf，可以把 in.HighKey = buf[bufIdx].Key
		if bufIdx < bufNum {
			in.HighKey = buf[bufIdx].Key
		}
		return bufIdx, true
	}

	// 插入最后一个键值对
	in.Entries = append(in.Entries, Entry{
		Key:   buf[bufIdx].Key,
		Value: buf[bufIdx].Value,
	})
	in.IncrementCount()
	bufIdx++
	return bufIdx, false
}

// BatchInsertLastLevelWithMigrationAndMovement 批量插入到叶子节点，包括迁移和缓冲区处理
// 返回更新后的 migrateIdx, bufIdx 和错误（如果有）
func (in *INode) BatchInsertLastLevelWithMigrationAndMovement(
	migrate []Entry, migrateIdx int, migrateNum int,
	keys []interface{}, values []NodeInterface, idx int, num int, batchSize int,
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
	if idx < num && int(in.count) < batchSize {
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
			if idx == num && int(in.count) == batchSize && bufNum != 0 {
				if bufIdx >= bufNum {
					return migrateIdx, bufIdx, fmt.Errorf("bufIdx out of range when setting HighKey")
				}
				in.HighKey = buf[bufIdx].Key
				return migrateIdx, bufIdx, nil
			}
		}
	}

	// 如果还有缓冲区条目需要插入，并且当前节点未满
	if bufIdx < bufNum && int(in.count) < batchSize {
		if fromStart {
			// 从缓冲区开始插入，更新 leftmost_ptr
			in.leftmostPtr = buf[bufIdx].Value.(*Node)
			bufIdx++
		}
		// 批量插入缓冲区条目
		bufIdx, _ = in.BatchBuffer(buf, bufIdx, bufNum, batchSize)
	}

	return migrateIdx, bufIdx, nil
}

// BatchInsertLastLevelWithMovement 此版本主要处理批量插入，同时考虑起始点设置和缓冲区的批量插入
// BatchInsertLastLevelWithMovement 批量插入到叶子节点，考虑迁移和缓冲区
// 返回更新后的 idx, bufIdx 和错误（如果有）
func (in *INode) BatchInsertLastLevelWithMovement(
	keys []interface{}, values []NodeInterface, idx int, num int, batchSize int, // 键值对
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
			if idx == num && int(in.count) == batchSize && bufNum != 0 {
				if bufIdx >= bufNum {
					return idx, bufIdx, fmt.Errorf("bufIdx out of range when setting HighKey")
				}
				in.HighKey = buf[bufIdx].Key
				return idx, bufIdx, nil
			}
		}
	}

	// Step 2: 从缓冲区插入条目
	if bufIdx < bufNum && int(in.count) < batchSize {
		if fromStart {
			// 从缓冲区开始插入，更新 leftmost_ptr
			in.leftmostPtr = buf[bufIdx].Value.(LeafNodeInterface)
			bufIdx++
		}
		// 批量插入缓冲区条目
		in.BatchBuffer(buf, bufIdx, bufNum, batchSize)
	}
	return idx, bufIdx, nil
}

// BatchInsertLastLevel 批量插入到叶子节点，包括迁移和缓冲区处理
// 返回新节点集合、新Num 和错误（如果有）
func (in *INode) BatchInsertLastLevel(keys []interface{}, values []NodeInterface, num int, batchSize int) ([]INodeInterface, error) {
	pos := in.FindLowerBound(keys[0])
	batchSizeCalc := int(float64(in.Cardinality) * FillFactor)
	// 原版: bool inplace = ((cnt + num) < cardinality);
	inplace := (int(in.count) + num - 1) <= in.Cardinality

	moveNum := 0
	idx := 0
	if pos < 0 {
		// insert at leftmostPtr,因为所有 entry 都要往后挪,这个moveNum代表需要挪动的entry数量
		moveNum = int(in.count)
	} else {
		// insert in the middle，把后面 [pos+1 ..count-1] 往后挪 1。
		moveNum = int(in.count) - (pos + 1)
	}

	if inplace {
		// 如果仅有 1 个 BTreeNode（num=1），说明就是“1对1” 替换，不需要移动 Entry，也不需要计数+1
		if num == 1 {
			// 只需替换
			if pos < 0 {
				// 哈希节点在 leftmostPtr
				in.leftmostPtr = values[0]
			} else {
				// 哈希节点在 entry[pos]
				in.Entries[pos].Value = values[0]
			}
			// 不需要移动或者插入额外 entry, 也不增加 in.count
			return nil, nil
		} else {
			// === 若 num > 1，才需要移动并插入多条 Entry ===

			// 1) 移动后方 entry
			in.moveNormalInsertionForLastLevel(pos, num, moveNum)

			// 2) 替换 leftmostPtr 或 entry[pos]
			if pos < 0 {
				in.leftmostPtr = values[idx]
				idx++
			} else {
				in.Entries[pos].Value = values[idx]
				idx++
			}

			// 3) 插入其余 (num-1) 条 entry 到 [pos+1.. pos+num]
			for i := pos + 1; i < pos+num; i++ {
				in.Entries[i].Key = keys[idx]
				in.Entries[i].Value = values[idx]
				idx++
			}

			// c++ 原版： cnt += (num-1); net + (num-1)
			// 如果在你实现中“覆盖”也算1个slot，那么 net + (num-1).
			in.count += int32(num - 1)

			return nil, nil
		}
	} else {

		// need split / migration
		prevHighKey := in.HighKey

		// first, set leftmostPtr or entry[pos].value = values[0]
		if pos < 0 {
			in.leftmostPtr = values[idx]
			idx++
		} else {
			in.Entries[pos].Value = values[idx]
			idx++
		}

		// we go into 2 big branches in c++:
		// if (batchSize < pos) { ... } else { ... }
		if batchSizeCalc < pos {
			// case1: "插入到中间 (migrated + new kvs + moved)"
			/*
				如果插入位置 pos 大于一个阈值（例如 batchSizeCalc，或 batch_size），可能意味着你要插入的位置更偏右，
				从而会把当前结点先留一部分条目，然后有相当多的 old entry 以及新的 (key, value) 都去放入新的结点；
			*/
			migrateNum := pos - batchSizeCalc
			// allocate slice
			migrate := make([]Entry, migrateNum)
			copy(migrate, in.Entries[batchSizeCalc:pos])

			buf := make([]Entry, moveNum)
			copy(buf, in.Entries[pos+1:pos+1+moveNum])

			in.count = int32(batchSizeCalc)

			totalNum := num + moveNum + migrateNum
			newNum, lastChunk := in.CalculateNodeNum(totalNum, batchSizeCalc)

			newNodes := make([]INodeInterface, newNum)
			for i := 0; i < newNum; i++ {
				newNodes[i] = NewINodeForInsertInBatch(in.level)
			}

			oldSibling, ok := in.siblingPtr.(INodeInterface)
			if !ok {
				oldSibling = nil
			}
			in.siblingPtr = newNodes[0]

			migrateIdx := 0
			bufIdx := 0
			// c++: high_key = migrate[migrateIdx].key
			// maybe in.Go => in.HighKey = ...
			if migrateNum > 0 {
				in.HighKey = migrate[migrateIdx].Key
			}

			// fill each newNodes[i] except last one
			for i := 0; i < newNum-1; i++ {
				newNodes[i].SetSibling(newNodes[i+1])
				// call BatchInsertLastLevelWithMigrationAndMovement
				migIdx, bfIdx, err := newNodes[i].BatchInsertLastLevelWithMigrationAndMovement(
					migrate, migrateIdx, migrateNum,
					keys, values, idx, num, batchSizeCalc,
					buf, bufIdx, moveNum,
				)
				if err != nil {
					return nil, err
				}
				migrateIdx = migIdx
				bufIdx = bfIdx
			}

			newNodes[newNum-1].SetSibling(oldSibling)
			_, _, err := newNodes[newNum-1].BatchInsertLastLevelWithMigrationAndMovement(
				migrate, migrateIdx, migrateNum,
				keys, values, idx, num, lastChunk,
				buf, 0, moveNum,
			)
			if err != nil {
				return nil, err
			}
			newNodes[newNum-1].SetHighKey(prevHighKey)

			return newNodes, nil
		} else {
			// case2: "插入到中间 (new_kvs + moved)"
			/*
				如果 pos 不大于这个阈值，表示你要插入的位置不那么靠后，只需要把右边部分“moved”到新结点，也就是 Case2（new_kvs + moved）。
			*/
			moveIdx := 0
			//需要移动的部分，先存入buf
			buf := make([]Entry, moveNum)
			copy(buf, in.Entries[pos+1:pos+1+moveNum])

			// fill [pos+1.. batchSizeCalc) with as many from keys/values
			for i := pos + 1; i < batchSizeCalc && idx < num; i++ {
				in.Entries[i].Key = keys[idx]
				in.Entries[i].Value = values[idx]
				idx++
			}

			// c++ => cnt += (idx - move_num -1)
			in.count += int32(idx - moveNum - 1)

			for ; in.count < int32(batchSizeCalc) && moveIdx < moveNum; in.count, moveIdx = in.count+1, moveIdx+1 {
				in.Entries[in.count].Key = buf[moveIdx].Key
				in.Entries[in.count].Value = buf[moveIdx].Value
			}

			//var newHighKey interface{}
			//if idx < num {
			//	newHighKey = keys[idx]
			//} else {
			//	newHighKey = buf[moveIdx].Key
			//}
			// 先假设 newHighKey = prevHighKey (把父节点原来的 highKey 带过来)
			newHighKey := prevHighKey
			// 如果 idx < num, 说明后面 still have keys[idx.. num-1] 未插完
			//   => 取 keys[num-1] or keys[idx] 里更大的?
			//   你要看插入是升序还是？ 如果 keys 是排序好的, 取 keys[num-1] 即最大
			if idx < num {
				newHighKey = keys[idx]
			} else {
				newHighKey = buf[moveIdx].Key
			}

			totalNum := num - idx + moveNum - moveIdx
			newNum, lastChunk := in.CalculateNodeNum(totalNum, batchSizeCalc)

			newNodes := make([]INodeInterface, newNum)
			for i := range newNodes {
				newNodes[i] = NewINodeForInsertInBatch(in.level)
			}

			oldSibling, ok := in.siblingPtr.(INodeInterface)
			if !ok {
				oldSibling = nil
			}
			in.siblingPtr = newNodes[0]

			for i := 0; i < newNum-1; i++ {
				newNodes[i].SetSibling(newNodes[i+1])
				_, _, err := newNodes[i].BatchInsertLastLevelWithMovement(
					keys, values, idx, num, batchSizeCalc,
					buf, moveIdx, moveNum,
				)
				if err != nil {
					return nil, err
				}
			}
			newNodes[newNum-1].SetSibling(oldSibling)
			_, _, err := newNodes[newNum-1].BatchInsertLastLevelWithMovement(
				keys, values, idx, num, lastChunk,
				buf, moveIdx, moveNum,
			)
			if err != nil {
				return nil, err
			}
			in.HighKey = newHighKey
			newNodes[newNum-1].SetHighKey(prevHighKey)
			return newNodes, nil
		}
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

func (in *INode) InsertForRoot(keys []interface{}, values []NodeInterface, left NodeInterface, num int) {
	in.leftmostPtr = left
	for i := 1; i < num; i++ {
		in.Entries = append(in.Entries, Entry{
			Key:   keys[i],
			Value: values[i],
		})
		in.IncrementCount()
		// 更新 HighKey
		if in.HighKey == nil || compareIntKeys(keys[i], in.HighKey) > 0 {
			in.HighKey = keys[i]
		}
	}
}

// pos: 需要插入的位置，num: 需要插入的btreeNode数量，moveNum
func (in *INode) moveNormalInsertionForLastLevel(pos, num, moveNum int) {
	// 1) 计算我们最终需要下标达到多少：
	//    要腾出 [pos+1 .. pos+num] 的区域给新插入的 num 条目，
	//    并且需要把 [pos+1.. pos+1+moveNum) 向后挪 num 个位置，
	//    目标区间为 [pos+num+1 .. pos+num+1+moveNum)
	//
	//    因此我们可能要访问 in.Entries[pos+num + moveNum], 下标上限 pos+num+moveNum
	//    例如: if pos=1, num=2, moveNum=0 => 需要访问 [pos+2] = [3], 这就可能超 len(in.Entries).

	// 2) 先确保 slice 容量足够
	neededIndex := pos + num + moveNum - 1 // +1 取决于具体写法, 见下
	if neededIndex >= len(in.Entries) {
		// 计算需要补多少
		extra := (neededIndex + 1) - len(in.Entries)
		in.Entries = append(in.Entries, make([]Entry, extra)...)
	}

	// 3) 如果 moveNum>0，则执行后移 copy
	//    目标: [pos+num+1 .. pos+num+1+moveNum)
	//    源  : [pos+1       .. pos+1+moveNum)
	if moveNum > 0 {
		// 同理再做一次 slice 容量校验(针对 pos+num+1+moveNum)
		targetEnd := pos + num + moveNum
		if targetEnd > len(in.Entries) {
			extra := targetEnd - len(in.Entries)
			in.Entries = append(in.Entries, make([]Entry, extra)...)
		}
		copy(
			in.Entries[pos+num:pos+num+moveNum],
			in.Entries[pos+1:pos+1+moveNum],
		)
	}
}

func (in *INode) moveNormalInsertionForInnerNode(pos, num, moveNum int) {
	// ----------------------------------------------------------------
	// 1) 计算写访问的最大下标 upTo = (pos + num + moveNum)
	//
	//   在经典 c++ memmove做法:
	//   memmove(&entry[pos+num+1], &entry[pos+1], moveNum)
	//   => largest write index = (pos+ num+1) + moveNum -1 = pos+ num+ moveNum
	//
	//   当 pos=-1, num=1, moveNum=2 => upTo= -1+1+2=2 => 代表可能写到 entry[2].
	//   当 pos=0,  num=1, moveNum=1 => upTo= 0+1+1=2 => 可能写到 entry[2].
	// ----------------------------------------------------------------
	upTo := pos + num + moveNum
	if upTo > len(in.Entries) {
		need := upTo - len(in.Entries)
		// append 至少补 enough 长度
		in.Entries = append(in.Entries, make([]Entry, need)...)
	}

	if upTo >= len(in.Entries) {
		extra := (upTo + 1) - len(in.Entries)
		in.Entries = append(in.Entries, make([]Entry, extra)...)
	}
	// ----------------------------------------------------------------
	// 2) 若 moveNum>0, 做真正的 copy 向后挪。
	//
	//   c++: memmove(&entry[pos+ num+1], &entry[pos+1], moveNum)
	//   => 目标区间: [pos+num+1 .. pos+num+1+moveNum)
	//      源区间 : [pos+1       .. pos+1+moveNum)
	//
	//   若 pos=-1 => target= [num.. num+moveNum), source= [0.. moveNum)
	//   若 pos>=0 => target= [pos+ num+1.. pos+ num+1+moveNum), source=[pos+1.. pos+1+moveNum)
	// ----------------------------------------------------------------

	if moveNum > 0 {
		if pos < 0 {
			//  pos=-1 =>  把 [0.. moveNum-1] => [num .. num+moveNum-1]
			//  e.g. if moveNum=2,num=1 => 把 [0..1] => [1..2]
			copy(
				in.Entries[num:num+moveNum],
				in.Entries[0:moveNum],
			)
		} else {
			//  pos>=0 => 把 [pos+1.. pos+1+moveNum-1] => [pos+num.. pos+num+moveNum-1]
			copy(
				in.Entries[pos+1+num:pos+1+num+moveNum],
				in.Entries[pos+1:pos+1+moveNum],
			)
		}
	}
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
	for i := 0; i < int(in.count)-1; i++ {
		for j := i + 1; j < int(in.count); j++ {
			if in.Entries[i].Key.(int) > in.Entries[j].Key.(int) {
				fmt.Printf("INode: Key order is not preserved!!\n")
				fmt.Printf("[%d].Key: %v\t[%d].Key: %v at node %p\n", i, in.Entries[i].Key, j, in.Entries[j].Key, in)
			}
		}
	}

	// 检查每个键是否符合 highKey 和 prevHighKey 的约束
	for i := 0; i < int(in.count); i++ {
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
	keys []interface{}, values []NodeInterface, idx int, num int,
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
	if idx < num && int(in.count) < batchSize {
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
			if idx == num && int(in.count) == batchSize && bufNum != 0 {
				if bufIdx >= bufNum {
					return migrateIdx, bufIdx, fmt.Errorf("bufIdx out of range when setting HighKey")
				}
				in.HighKey = buf[bufIdx].Key
			}
		}
	}

	// 如果还有缓冲区条目需要插入，并且当前节点未满
	if bufIdx < bufNum && int(in.count) < batchSize {
		if fromStart {
			in.leftmostPtr = buf[bufIdx].Value.(*Node)
			bufIdx++
		}
		var _ bool
		bufIdx, _ = in.BatchBuffer(buf, bufIdx, bufNum, batchSize)
	}

	return migrateIdx, bufIdx, nil
}

// BatchInsertWithMovement 批量插入到叶子节点，考虑迁移和缓冲区
// 返回更新后的 idx, bufIdx 和错误（如果有）
func (in *INode) BatchInsertWithMovement(
	keys []interface{}, values []NodeInterface, idx int, num int,
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
			if idx == num && int(in.count) == batchSize && bufNum != 0 {
				if bufIdx >= bufNum {
					return idx, bufIdx, fmt.Errorf("bufIdx out of range when setting HighKey")
				}
				in.HighKey = buf[bufIdx].Key
			}
		}
	}

	// 从缓冲区插入键值对
	if bufIdx < bufNum && int(in.count) < batchSize {
		if fromStart {
			in.leftmostPtr = buf[bufIdx].Value.(*Node)
			bufIdx++
		}
		var _ bool
		bufIdx, _ = in.BatchBuffer(buf, bufIdx, bufNum, batchSize)
	}

	return idx, bufIdx, nil
}

// BatchInsert 批量插入到叶子节点，包括迁移和缓冲区处理
// 返回新节点集合和错误（如果有）
func (in *INode) BatchInsert(
	keys []interface{}, values []NodeInterface, num int,
) ([]INodeInterface, error) {
	// 1) 查找插入位置
	pos := in.FindLowerBound(keys[0])

	// 2) 计算阈值 batchSize (比如 batch_size = FillFactor * Cardinality)
	batchSize := int(float64(in.Cardinality) * FillFactor)

	// 3) 判断能否 in-place
	//    原版 c++ often do: (cnt+num) < cardinality
	inPlace := (int(in.count) + num) <= in.Cardinality

	idx := 0

	// 4) 计算 moveNum
	moveNum := 0
	if pos < 0 {
		// 替换 leftmostPtr + 还要在 [0..count-1] 向后挪 num
		moveNum = int(in.count)
	} else {
		// 替换 entry[pos], 还要把 [pos+1.. count-1] 向后挪 num
		moveNum = int(in.count) - (pos + 1)
	}

	// Case 1: Insert in-place
	if inPlace {
		in.moveNormalInsertionForInnerNode(pos, num, moveNum)
		//如果插入的位置，是在最左侧，那么整体右移，原来的leftmostPtr现在到了entries[0]
		// 再插入
		if pos < 0 {
			// pos=-1 => entries[0 .. num-1] 填上 (keys, values)
			for i := 0; i < num; i++ {
				in.Entries[i] = Entry{Key: keys[i], Value: values[i]}
			}
		} else {
			// pos>=0 => entry[pos+1.. pos+num] 填
			for i, j := pos+1, 0; j < num; i, j = i+1, j+1 {
				in.Entries[i] = Entry{Key: keys[j], Value: values[j]}
			}
		}
		in.count += int32(num)
		in.HighKey = in.Entries[in.count-1].Value.(INodeInterface).GetHighKey()
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
		in.count = int32(batchSize)

		// Calculate new nodes needed
		totalNum := num + moveNum + migrateNum
		newNum, lastChunk := in.CalculateNodeNum(totalNum, batchSize)

		// Create new sibling nodes
		newNodes := make([]INodeInterface, newNum)
		for i := 0; i < newNum; i++ {
			newNodes[i] = NewINodeForInsertInBatch(in.level)
		}

		// Adjust sibling pointers
		oldSibling, ok := in.siblingPtr.(INodeInterface)
		if !ok {
			oldSibling = nil
		}
		in.siblingPtr = newNodes[0]

		// Insert data into sibling nodes
		migrateIdx, moveIdx := 0, 0
		prevHighKey := in.HighKey
		if migrateNum > 0 {
			in.HighKey = migrate[migrateIdx].Key
		}

		for i := 0; i < newNum-1; i++ {
			newNodes[i].SetSibling(newNodes[i+1])
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
		newNodes[newNum-1].SetSibling(oldSibling)
		newNodes[newNum-1].SetHighKey(prevHighKey)
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

	in.count += int32(idx - moveNum)
	for int(in.count) < batchSize && moveIdx < moveNum {
		if len(in.Entries) <= int(in.count) {
			in.Entries = append(in.Entries, Entry{}) // Expanding the slice if needed
		}
		in.Entries[in.count] = bufEntries[moveIdx]
		in.IncrementCount()
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
	newNodes := make([]INodeInterface, newNum)
	for i := 0; i < newNum; i++ {
		newNodes[i] = NewINodeForInsertInBatch(in.level)
	}

	// Adjust sibling pointers
	oldSibling, ok := in.siblingPtr.(INodeInterface)
	if !ok {
		oldSibling = nil
	}
	in.siblingPtr = newNodes[0]

	// Insert data into sibling nodes
	for i := 0; i < newNum-1; i++ {
		newNodes[i].SetSibling(newNodes[i+1])
		var err error
		idx, moveIdx, err = newNodes[i].BatchInsertWithMovement(
			keys, values, idx, num, batchSize, bufEntries, moveIdx, moveNum,
		)
		if err != nil {
			return nil, err
		}
	}

	// Last node adjustments
	newNodes[newNum-1].SetSibling(oldSibling)
	newNodes[newNum-1].SetHighKey(prevHighKey)
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
func (n *INode) GetEntries() []Entry {
	return n.Entries
}
func (n *INode) GetType() NodeType {
	return INNERNode
}
func (n *INode) GetCardinality() int {
	return n.Cardinality
}
func (n *INode) SetSibling(sibling INodeInterface) {
	n.siblingPtr = sibling
}
