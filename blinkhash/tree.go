package blinkhash

import (
	"fmt"
	"sync"
	"unsafe"
)

type BTree struct {
	root   NodeInterface
	epoche *Epoche
	lock   sync.Mutex
}

func NewBTree() *BTree {
	return &BTree{
		root:   NewLNodeHash(0), // 假设默认根节点是一个哈希节点
		epoche: NewEpoche(256),  // 设置 Epoche 的初始容量或阈值
		lock:   sync.Mutex{},
	}
}

// Insert inserts a key-value pair into the B-tree.
func (bt *BTree) Insert(key, value interface{}, ti *ThreadInfo) {
	// Create an EpocheGuard and ensure Release is called at the end.
	epocheGuard := NewEpocheGuard(ti)
	defer epocheGuard.Release()
insertLoop: // 标签
	for {
		restart := false
		cur := bt.root
		stack := make([]INodeInterface, 0)

		// Attempt to acquire read lock on the root node.
		curVersion, needRestart := cur.TryReadLock()
		if needRestart {
			continue // Restart the insert process.
		}

		// Tree traversal to find the leaf node.
		for cur.GetLevel() != 0 {
			parent := NewINode(cur.GetLevel(), nil, cur.GetSiblingPtr(), cur.GetLeftmostPtr())
			//parent, ok := cur.(INodeInterface)
			//if !ok {
			//	panic("expected INodeInterface")
			//}
			child := parent.ScanNode(key)
			childVersion, needRestart := child.TryReadLock()
			if needRestart {
				restart = true
				break
			}

			// Check version consistency.
			curEndVersion, needRestart := cur.GetVersion()
			if needRestart || curVersion != curEndVersion {
				restart = true
				//为什么cpp中是goto，循环，但是在go中是break？
				break
			}

			if child != parent.GetSiblingPtr() {
				stack = append(stack, parent)
			}

			cur = child
			curVersion = childVersion
		}

		if restart {
			continue insertLoop // 跳转到最外层循环开始
		}

		leafNode, ok := cur.(LeafNodeInterface)
		if !ok {
			panic("expected *LNodeHash")
		}
		leafVersion := curVersion

		// Check if we need to traverse to the sibling leaf node.
		for leafNode.GetSiblingPtr() != nil && compareIntKeys(leafNode.GetHighKey(), key) < 0 {
			sibling, ok := leafNode.GetSiblingPtr().(*LNodeHash)
			if !ok {
				panic("expected *LNodeHash")
			}

			siblingVersion, needRestart := sibling.TryReadLock()
			if needRestart {
				restart = true
				break
			}

			leafEndVersion, needRestart := leafNode.GetVersion()
			if needRestart || leafVersion != leafEndVersion {
				restart = true
				break
			}

			leafNode = sibling
			leafVersion = siblingVersion
		}

		if restart {
			continue insertLoop // 跳转到最外层循环开始
		}

		//这里的Insert，应该是调用Insertable，而不是调用LNodeHash中的Insert
		//应该是根据leaf的类型来执行不同的Insert
		// Attempt to insert into the leaf node.
		ret := leafNode.Insert(key, value, leafVersion)
		if ret == NeedRestart { // Leaf node has been split during insertion.
			continue // Restart the insert process.
		} else if ret == InsertSuccess { // Insertion succeeded.
			return
		} else { // Leaf node split.
			splittableLeaf, splitKey := leafNode.Split(key, value, leafVersion)
			if splittableLeaf == nil { // 另一线程已分裂该叶子节点
				continue // 重启插入过程
			}

			newLeafNode, ok := splittableLeaf.(LeafNodeInterface)
			if !ok {
				panic("expected LeafNodeInterface new leaf node")
			}

			if len(stack) > 0 {
				stackIdx := len(stack) - 1
				oldParent := stack[stackIdx]
			parentRestart:
				for stackIdx >= 0 {
					oldParent = stack[stackIdx]
					originalNode := leafNode.GetNode()
					restartParent := false
					// Attempt to acquire write lock on the parent node.
					parentVersion, needRestart := oldParent.TryReadLock()
					if needRestart {
						restartParent = true
					}

					if restartParent {
						continue parentRestart // 跳转到最外层循环开始 // Restart the insert process.
					}

					/*while 未实现部分*/
					// 遍历父节点，直到找到一个合适的节点来插入新的分裂键
					for oldParent.GetSiblingPtr() != nil && compareIntKeys(oldParent.GetHighKey(), splitKey) < 0 {
						sibling := oldParent.GetSiblingPtr()
						siblingVersion, needRestart := sibling.TryReadLock()
						if needRestart {
							restartParent = true
							break // 跳出循环，准备重启
						}

						parentEndVersion, needRestart := oldParent.GetVersion()
						if needRestart || parentVersion != parentEndVersion {
							restartParent = true
							break // 版本不一致，准备重启
						}

						// 更新当前父节点为兄弟节点
						oldParent = sibling.(INodeInterface)
						parentVersion = siblingVersion
					}

					// 检查是否需要重启父节点处理过程
					if restartParent {
						continue parentRestart
					}
					node := oldParent.GetNode()
					success, needRestart := node.TryUpgradeWriteLock(oldParent.GetLock())
					if !success || needRestart {
						continue parentRestart
					}
					originalNode.WriteUnlock()

					if !oldParent.IsFull() { // Normal insert.
						oldParent.Insert(splitKey, newLeafNode.GetNode(), oldParent.GetLock())
						oldParent.WriteUnlock()
						return
					}

					// Internal node split.
					splittableNode, newSplitKey := oldParent.Split(key, value, 0)
					newParent := splittableNode.(INodeInterface)
					if compareIntKeys(splitKey, newSplitKey) <= 0 {
						oldParent.Insert(splitKey, newLeafNode.GetNode(), oldParent.GetLock())
					} else {
						newParent.Insert(splitKey, newLeafNode.GetNode(), newParent.GetLock())
					}

					if stackIdx > 0 {
						splitKey = newSplitKey
						stackIdx--
						oldParent = stack[stackIdx]
					} else { // set new root
						if oldParent == bt.root {
							newRoot := NewINodeForHeightGrowth(splitKey, oldParent, newParent, nil, oldParent.GetLevel()+1, newParent.GetHighKey())
							bt.root = newRoot
							oldParent.WriteUnlock()
						} else {
							bt.insertKey(newSplitKey, newParent, oldParent)
						}
						return
					}
				}
			} else {
				// Set new root node.
				if bt.root == leafNode { // Current node is root.
					newRoot := NewINodeForHeightGrowth(splitKey, leafNode, newLeafNode, nil, leafNode.GetLevel()+1, newLeafNode.GetHighKey())
					bt.root = newRoot
					leafNode.WriteUnlock()
				} else { // Another thread has already created a new root.
					bt.insertKey(splitKey, newLeafNode, leafNode)
				}
			}
		}
	}
}

// insertKey is called when the root has been split by another thread.
// It inserts a key and node pointers into the B-tree.
func (bt *BTree) insertKey(key interface{}, value NodeInterface, prev NodeInterface) {
	for {
		restart := false
		cur := bt.root
		// 尝试获取根节点的读锁
		curVersionStart, needRestart := cur.TryReadLock()
		if needRestart {
			continue
		}

		parent, ok := cur.(*INode)
		if !ok {
			panic("expected *INode")
		}

		// 遍历树，找到 level = prev.level + 1 的内部节点
		for parent.GetLevel() != prev.GetLevel()+1 {
			child := parent.ScanNode(key)
			childVersion, needRestart := child.TryReadLock()
			if needRestart {
				restart = true
				break
			}

			// 检查版本一致性
			curEndVersion, needRestart := cur.GetVersion()
			if needRestart || curVersionStart != curEndVersion {
				restart = true
				break
			}

			cur = child
			curVersionStart = childVersion
		}

		if restart {
			continue
		}

		// 查找需要插入的位置
		for parent.GetSiblingPtr() != nil && compareIntKeys(parent.HighKey, key) < 0 {
			sibling := parent.GetSiblingPtr()

			siblingVersionStart, needRestart := sibling.TryReadLock()
			if needRestart {
				restart = true
				break
			}

			parentEndVersion, needRestart := parent.GetVersion()
			if needRestart || curVersionStart != parentEndVersion {
				restart = true
				break
			}

			parent = sibling.(*INode)
			curVersionStart = siblingVersionStart
		}

		if restart {
			continue
		}

		// 尝试升级为写锁
		success, needRestart := parent.TryUpgradeWriteLock(curVersionStart)
		if needRestart || !success {
			continue
		}

		// 解锁 prev 节点
		prev.WriteUnlock()

		if !ok {
			panic("Except type *Node")
		}
		// 检查父节点是否已满
		if !parent.IsFull() {
			err := parent.Insert(key, value, parent.lock)
			if err != InsertSuccess {
				panic("parent.Insert failed!")
				return
			}
			parent.WriteUnlock()
			return
		} else {
			// 父节点分裂
			splittableNode, splitKey := parent.Split(key, value, 0)
			newParent, ok := splittableNode.(INodeInterface)
			if !ok {
				panic("newParentSplittableInterface cannot be INode")

			}
			if compareIntKeys(key, splitKey) <= 0 {
				err := parent.Insert(key, value, parent.lock)
				if err != InsertSuccess {
					panic("parent.Insert failed!")
					return
				}
			} else {
				err := newParent.Insert(key, value, newParent.GetLock())
				if err != InsertSuccess {
					panic("parent.Insert failed!")
					return
				}
			}

			if parent == bt.root {
				// 创建新的根节点
				newRoot := NewINodeForHeightGrowth(splitKey, parent, newParent, nil, parent.level+1, parent.HighKey)
				bt.root = newRoot
				parent.WriteUnlock()
			} else {
				node := cur
				// 递归插入到更高层
				bt.insertKey(splitKey, newParent, node)
			}
		}
	}
}

func (bt *BTree) Lookup(key interface{}, ti *ThreadInfo) interface{} {
	eg := NewEpocheGuardReadonly(ti)
	defer eg.Release()

restart:
	cur := bt.root
	curVersion, needRestart := cur.TryReadLock()
	if needRestart {
		goto restart
	}

	// traversal
	for cur.GetLevel() != 0 {
		parent, ok := cur.(*INode)
		if !ok {
			panic("expected *INode")
		}
		child := parent.ScanNode(key)
		childVersion, needRestart := child.TryReadLock()
		if needRestart {
			goto restart
		}

		parentEndVersion, needRestart := parent.GetVersion()
		if needRestart || (curVersion != parentEndVersion) {
			goto restart
		}

		cur = child
		curVersion = childVersion
	}

	// found leaf
	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	leafVersion := curVersion

	// move right if necessary
	for leaf.GetSiblingPtr() != nil && compareIntKeys(leaf.GetHighKey(), key) < 0 {
		sibling := leaf.GetSiblingPtr()

		siblingVersion, needRestart := sibling.TryReadLock()
		if needRestart {
			goto restart
		}

		leafEndVersion, needRestart := leaf.GetVersion()
		if needRestart || (leafVersion != leafEndVersion) {
			goto restart
		}

		lf, ok := sibling.(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
		leaf = lf
		leafVersion = siblingVersion
	}
	needRestart = false
	val, needRestart := leaf.Find(key) // 封装leaf.find
	if needRestart {
		goto restart
	}

	leafEndVersion, needRestart := leaf.GetVersion()
	if needRestart || (leafVersion != leafEndVersion) {
		goto restart
	}
	if val != nil {
		return val
	}
	return nil
}

func (bt *BTree) Remove(key interface{}, ti *ThreadInfo) bool {
	eg := NewEpocheGuard(ti)
	defer eg.Release()

restart:
	cur := bt.root
	curVersion, needRestart := cur.TryReadLock()
	if needRestart {
		goto restart
	}

	// traversal
	for cur.GetLevel() != 0 {
		parent, ok := cur.(*INode)
		if !ok {
			panic("expected *INode")
		}
		child := parent.ScanNode(key)
		childVersion, needRestart := child.TryReadLock()
		if needRestart {
			goto restart
		}

		parentEndVersion, needRestart := parent.GetVersion()
		if needRestart || (curVersion != parentEndVersion) {
			goto restart
		}
		cur = child
		curVersion = childVersion
	}

	// found leaf
	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	leafVersion := curVersion

	// move right if necessary
	for leaf.GetSiblingPtr() != nil && compareIntKeys(leaf.GetHighKey(), key) < 0 {
		sibling := leaf.GetSiblingPtr()

		siblingVersion, needRestart := sibling.TryReadLock()
		if needRestart {
			goto restart
		}
		leafEndVersion, needRestart := leaf.GetVersion()
		if needRestart || (leafVersion != leafEndVersion) {
			goto restart
		}

		lf, ok := sibling.(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
		leaf = lf
		leafVersion = siblingVersion
	}

	ret := leaf.Remove(key, leafVersion) // 封装leaf.remove
	if ret == NeedRestart {
		goto restart
	} else if ret == RemoveSuccess {
		return true
	} else {
		return false
	}
}

func (bt *BTree) Update(key, value interface{}, ti *ThreadInfo) bool {
	eg := NewEpocheGuardReadonly(ti)
	defer eg.Release()
restart:
	cur := bt.root
	curVersion, needRestart := cur.TryReadLock()
	if needRestart {
		goto restart
	}

	// traversal
	for cur.GetLevel() != 0 {
		parent, ok := cur.(*INode)
		if !ok {
			panic("expected *INode")
		}

		child := parent.ScanNode(key)
		childVersion, needRestart := child.TryReadLock()
		if needRestart {
			goto restart
		}

		parentEndVersion, needRestart := parent.GetVersion()
		if needRestart || curVersion != parentEndVersion {
			goto restart
		}

		cur = child
		curVersion = childVersion
	}

	// found leaf
	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	leafVersion := curVersion

	// move right if necessary
	for leaf.GetSiblingPtr() != nil && compareIntKeys(leaf.GetHighKey(), key) < 0 {
		sibling := leaf.GetSiblingPtr()
		siblingVersion, needRestart := sibling.TryReadLock()
		if needRestart {
			goto restart
		}

		leafEndVersion, needRestart := leaf.GetVersion()
		if needRestart || leafVersion != leafEndVersion {
			goto restart
		}

		lf, ok := sibling.(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
		leaf = lf
		leafVersion = siblingVersion
	}

	ret := leaf.Update(key, value, leafVersion) // leaf.update的封装调用
	if ret == NeedRestart {
		goto restart
	} else if ret == UpdateSuccess {
		return true
	}
	return false
}

// BatchInsert 实现batch_insert逻辑
func (bt *BTree) BatchInsert(key []interface{}, value []NodeInterface, num int, prev NodeInterface, ti *ThreadInfo) {
	eg := NewEpocheGuard(ti)
	defer eg.Release()

restart:
	cur := bt.root
	curVersion, needRestart := cur.TryReadLock()
	if needRestart {
		goto restart
	}

	for cur.GetLevel() != prev.GetLevel()+1 {
		parent, ok := cur.(*INode)
		if !ok {
			panic("expected *INode")
		}
		child := parent.ScanNode(key[0])
		childVersion, needRestart := child.TryReadLock()
		if needRestart {
			goto restart
		}

		curEndVersion, needRestart := cur.GetVersion()
		if needRestart || (curVersion != curEndVersion) {
			goto restart
		}

		cur = child
		curVersion = childVersion
	}

	// found parent of prev node
	for cur.GetSiblingPtr() != nil && compareIntKeys(cur.GetHighKey(), key[0]) < 0 {
		sibling := cur.GetSiblingPtr()
		siblingVersion, needRestart := sibling.TryReadLock()
		if needRestart {
			goto restart
		}

		curEndVersion, needRestart := cur.GetVersion()
		if needRestart || (curVersion != curEndVersion) {
			goto restart
		}

		cur = sibling
		curVersion = siblingVersion
	}

	success, needRestart := cur.(*INode).TryUpgradeWriteLock(curVersion)
	if needRestart || !success {
		goto restart
	}

	// prev是叶子还是内部节点?
	if prev.GetLevel() == 0 {
		// leaf node
		value[0].WriteUnlock()
		// prev是叶子节点，调用convert_unlock_obsolete需要叶子节点定义
		leaf, ok := prev.(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
		leaf.WriteUnlockObsolete()
		ti.Epoche.MarkNodeForDeletion(prev, ti)
	} else {
		prev.WriteUnlock()
	}

	parent, ok := cur.(*INode)
	if !ok {
		panic("expected *INode")
	}

	new_num := 0
	var new_nodes []*INode
	if parent.GetLevel() == 1 {
		new_nodes, _ = parent.BatchInsertLastLevel(key, value, num, 0)
	} else {
		new_nodes, _ = parent.BatchInsert(key, value, num)
	}
	// BatchInsertLastLevel 和 BatchInsertInternal需要在INode中实现，类似C++逻辑

	if new_nodes == nil {
		parent.WriteUnlock()
		return
	}

	split_key := make([]interface{}, new_num)
	split_key[0] = parent.HighKey
	for i := 1; i < new_num; i++ {
		split_key[i] = new_nodes[i-1].HighKey
	}

	if parent != bt.root {
		// update non-root parent
		bt.BatchInsert(split_key, nodeInterfaceSlice(new_nodes), new_num, parent, ti)
	} else {
		// create new root
		// 需要递归roots逻辑
		for parent.Cardinality < new_num {
			new_roots, _new_num := bt.NewRootForAdjustment(split_key, nodeInterfaceSlice(new_nodes), new_num)
			new_nodes = new_roots
			new_num = _new_num
		}

		new_root := NewINodeForInsertInBatch(new_nodes[0].GetLevel() + 1)
		new_root.InsertForRoot(split_key, nodeInterfaceSlice(new_nodes), &parent.Node, new_num)
		bt.root = new_root
		parent.WriteUnlock()
	}
}

func (bt *BTree) NewRootForAdjustment(key []interface{}, value []NodeInterface, num int) ([]*INode, int) {
	// 在C++中FILL_FACTOR使用宏定义，这里您可自行定义FILL_FACTOR常量
	// 假设FILL_FACTOR为0.7或其他值，根据实际情况定义
	batch_size := int(float64(value[0].GetCount()) * FillFactor)

	var new_num int
	if num%batch_size == 0 {
		new_num = num / batch_size
	} else {
		new_num = num/batch_size + 1
	}

	new_roots := make([]*INode, new_num)
	//idx := 0
	for i := 0; i < new_num; i++ {
		// 假设level+1
		level := value[0].GetLevel() + 1
		new_roots[i] = NewINodeForInsertInBatch(level)
		// 在C++中是 new_roots[i]->batch_insert(key, value, idx, num, batch_size);
		// 在Go中需要INode实现batch_insert或batch_insert_last_level
		// 根据之前逻辑实现:
		//new_roots[i].BatchInsertForRoot(key, value, &idx, num, batch_size)
		if i < new_num-1 {
			new_roots[i].siblingPtr = new_roots[i+1]
		}
	}
	return new_roots, new_num
}

// nodeInterfaceSlice 将[]*INode转换为[]NodeInterface
func nodeInterfaceSlice(nodes []*INode) []NodeInterface {
	res := make([]NodeInterface, len(nodes))
	for i, n := range nodes {
		res[i] = n
	}
	return res
}
func nodeInterfaceSliceForBTreeNode(nodes []*LNodeBTree) []NodeInterface {
	res := make([]NodeInterface, len(nodes))
	for i, n := range nodes {
		res[i] = n
	}
	return res
}

func (bt *BTree) RangeLookup(min_key interface{}, rng int, buf []interface{}, ti *ThreadInfo) int {
	eg := NewEpocheGuard(ti)
	defer eg.Release()

restart:
	cur := bt.root
	curVersion, needRestart := cur.TryReadLock()
	if needRestart {
		goto restart
	}

	// traversal
	for cur.GetLevel() != 0 {
		parent, ok := cur.(*INode)
		if !ok {
			panic("expected *INode")
		}
		child := parent.ScanNode(min_key)
		childVersion, needRestart := child.TryReadLock()
		if needRestart {
			goto restart
		}

		curEndVersion, needRestart := cur.GetVersion()
		if needRestart || (curVersion != curEndVersion) {
			goto restart
		}

		cur = child
		curVersion = childVersion
	}

	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	leafVersion := curVersion

	count := 0
	continued := false
	for count < rng {
		// move right if necessary
		for leaf.GetSiblingPtr() != nil && compareIntKeys(leaf.GetHighKey(), min_key) < 0 {
			sibling := leaf.GetSiblingPtr()
			siblingVersion, needRestart := sibling.TryReadLock()
			if needRestart {
				goto restart
			}

			leafEndVersion, needRestart := leaf.GetVersion()
			if needRestart || (leafVersion != leafEndVersion) {
				goto restart
			}

			lf, ok := sibling.(LeafNodeInterface)
			if !ok {
				panic("expected LeafNodeInterface")
			}
			leaf = lf
			leafVersion = siblingVersion
		}

		ret := leaf.RangeLookUp(min_key, &buf, count, rng, continued)
		// ret的逻辑：
		// -1需要重启
		// -2需要convert
		// 其他为已插入个数
		if ret == NeedRestart {
			goto restart
		} else if ret == -2 {
			bt.convert(leaf, leafVersion, ti)
			goto restart
		}
		continued = true

		sibling := leaf.GetSiblingPtr()

		leafEndVersion, needRestart := leaf.GetVersion()
		if needRestart || (leafVersion != leafEndVersion) {
			goto restart
		}

		if ret == rng {
			return ret
		}

		if sibling == nil {
			break
		}

		siblingVersion, needRestart := sibling.TryReadLock()
		if needRestart {
			goto restart
		}
		lf, ok := sibling.(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
		leaf = lf
		leafVersion = siblingVersion
		count = ret
	}

	return count
}

// convert 对叶子节点进行转换，与C++一致
func (bt *BTree) convert(leaf LeafNodeInterface, leafVersion uint64, ti *ThreadInfo) bool {
	hashNode, ok := leaf.(*LNodeHash)
	if !ok {
		panic("Need leaf to be LNodeHashs")
	}
	bTreeNodes, num, err := hashNode.Convert(leafVersion)
	if err != nil {
		panic(err)
	}
	if bTreeNodes == nil {
		return false
	}
	split_key := make([]interface{}, num)
	split_key[0] = bTreeNodes[0].GetHighKey()
	for i := 1; i < num; i++ {
		split_key[i] = bTreeNodes[i-1].GetHighKey()
	}

	bt.BatchInsert(split_key, nodeInterfaceSliceForBTreeNode(bTreeNodes), num, leaf.(NodeInterface), ti)
	return true
}

func (bt *BTree) ConvertAll(ti *ThreadInfo) {
	eg := NewEpocheGuard(ti)
	defer eg.Release()

	cur := bt.root
	for cur.GetLevel() != 0 {
		cur = cur.GetLeftmostPtr()
	}

	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	curVersion, _ := leaf.GetVersion()
	// 在C++中没有明确的重启逻辑，此处暂不做重启处理

	for {
		if leaf.GetType() == BTreeNode {
			if leaf.GetNode() == Empty {
				return
			}
			lf, ok := leaf.GetSiblingPtr().(LeafNodeInterface)
			if !ok {
				panic("expected LeafNodeInterface")
			}
			leaf = lf
			continue
		}

		ret := bt.convert(leaf, curVersion, ti)
		if !ret {
			// blink_printf("Something wrong!! -- converting leaf %llx failed\n", leaf);
			fmt.Printf("Something wrong!! -- converting leaf %p failed\n", leaf)
		}
		sibling := leaf.GetSiblingPtr()
		if sibling == nil {
			break
		}
		lf, ok := sibling.(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
		leaf = lf
	}
}

// Print 打印B树的内部节点和叶子节点。
func (bt *BTree) Print() {
	bt.PrintInternal()
	bt.PrintLeaf()
}

// PrintInternal 遍历并打印所有内部节点。
func (bt *BTree) PrintInternal() {
	cur, ok := bt.root.(*INode)
	if !ok {
		panic("expected *INode")
	}
	internal := cur
	level := 0
	cnt := 1
	for cur.GetLevel() != 0 {
		fmt.Printf("level %d\n", level)
		internal = cur
		for {
			fmt.Printf("I%d(%v): ", cnt, cur)
			cur.Print()
			cnt++
			if cur.GetSiblingPtr() == nil {
				break
			}
			cur, ok = cur.GetSiblingPtr().(*INode)
			if !ok {
				panic("expected *INode")
			}
		}
		level++
		cur = internal.GetLeftmostPtr().(*INode)
	}
}

// PrintLeaf 遍历并打印所有叶子节点。
func (bt *BTree) PrintLeaf() {
	cur := bt.root
	for cur.GetLevel() != 0 {
		cur = cur.GetLeftmostPtr()
	}
	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	cnt := 1
	for {
		fmt.Printf("L%d(%v): ", cnt, leaf)
		leaf.Print()
		cnt++
		if leaf.GetSiblingPtr() == nil {
			break
		}
		leaf, ok = leaf.GetSiblingPtr().(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
	}
}

// SanityCheck 执行B树的完整性检查。
func (bt *BTree) SanityCheck() {
	cur := bt.root
	for cur.GetLevel() != 0 {
		p, ok := cur.(*INode)
		if !ok {
			panic("expected *INode")
		}
		p.SanityCheck(p.GetHighKey(), true)
		cur = p.GetLeftmostPtr()
	}

	l, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	l.SanityCheck(l.GetHighKey(), true)
}

// FindAnyway 在B树中查找指定键，并打印相关节点信息。
func (bt *BTree) FindAnyway(key interface{}) interface{} {
	cur := bt.root
	for cur.GetLevel() != 0 {
		cur = cur.GetLeftmostPtr()
	}

	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	var before LeafNodeInterface

	for {
		ret, found := leaf.Find(key)
		if found {
			if before != nil {
				fmt.Printf("before node(%v)\n", before)
				before.Print()
			}
			fmt.Printf("current node(%v)\n", leaf)
			leaf.Print()
			return ret
		}
		before = leaf
		if leaf.GetSiblingPtr() == nil {
			break
		}
		leaf, ok = leaf.GetSiblingPtr().(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
	}

	return nil
}

// Utilization 计算并打印B树的利用率。
func (bt *BTree) Utilization() float64 {
	cur := bt.root
	node := cur
	for cur.GetLevel() != 0 {
		var total uint64 = 0
		var count uint64 = 0
		for node != nil {
			internal, ok := node.(*INode)
			if !ok {
				panic("expected *INode")
			}
			total += uint64(internal.Cardinality)
			count += uint64(internal.GetCount())
			node = internal.GetSiblingPtr()
		}
		fmt.Printf("inode lv %d: %.2f %%\n", cur.GetLevel()-1, float64(count)/float64(total)*100.0)
		cur = cur.GetLeftmostPtr()
		node = cur
	}

	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	leaf_cnt := 0
	util := 0.0
	for {
		leaf_cnt++
		util += leaf.Utilization()

		if leaf.GetSiblingPtr() == nil {
			break
		}
		leaf, ok = leaf.GetSiblingPtr().(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
	}
	fmt.Printf("leaf %.2f %%\n", util/float64(leaf_cnt)*100.0)
	return util / float64(leaf_cnt) * 100.0
}

// RightmostUtilization 计算并返回最右侧叶子节点的利用率。
func (bt *BTree) RightmostUtilization() float64 {
	cur := bt.root
	for cur.GetLevel() != 0 {
		internal, ok := cur.(*INode)
		if !ok {
			panic("expected *INode")
		}
		cur = internal.GetRightmostPtr()
	}

	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}

	return leaf.Utilization()
}

// Footprint 计算B树的内存占用情况。
func (bt *BTree) Footprint(metrics *FootprintMetrics) {
	cur := bt.root
	leftmostNode := cur
	for cur.GetLevel() != 0 {
		leftmostNode = cur
		for cur != nil {
			metrics.Meta += uint64(unsafe.Sizeof(cur)) + sizeofKey() - uint64(unsafe.Sizeof((*NodeInterface)(nil)))

			internal, ok := cur.(*INode)
			if !ok {
				panic("expected *INode")
			}

			cnt := internal.GetCount()
			invalidNum := internal.Cardinality - cnt

			metrics.StructuralDataOccupied += uint64(unsafe.Sizeof(Entry{}))*uint64(cnt) + uint64(unsafe.Sizeof((*NodeInterface)(nil)))
			metrics.StructuralDataUnoccupied += uint64(unsafe.Sizeof(Entry{})) * uint64(invalidNum)

			cur = internal.GetSiblingPtr()
		}
		internalLeftmost, ok := leftmostNode.(*INode)
		if !ok {
			panic("expected *INode")
		}
		cur = internalLeftmost.GetLeftmostPtr()
		leftmostNode = cur
	}

	leaf, ok := cur.(LeafNodeInterface)
	if !ok {
		panic("expected LeafNodeInterface")
	}
	for {
		metrics.Meta += uint64(unsafe.Sizeof(leaf))
		leaf.Footprint(metrics)

		if leaf.GetSiblingPtr() == nil {
			break
		}
		leaf, ok = leaf.GetSiblingPtr().(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
	}
}

func (bt *BTree) height() int {
	return bt.root.GetLevel()
}

func (bt *BTree) getTreadInfo() *ThreadInfo {
	return NewThreadInfo(bt.epoche)
}

func (bt *BTree) GetEpoche() *Epoche {
	return bt.epoche

}
func (bt *BTree) GetHeight() int {
	return bt.root.GetLevel()

}
func (bt *BTree) GetThreadInfo() *ThreadInfo {
	info := ThreadInfo{
		Epoche: bt.epoche,
	}
	return &info
}

// sizeofKey 返回Key的大小（示例）。
func sizeofKey() uint64 {
	return uint64(unsafe.Sizeof(int(0))) // 假设Key是int类型
}

func sizeofUint8() uint8 {
	return uint8(unsafe.Sizeof(int(0))) // 假设Key是int类型
}
