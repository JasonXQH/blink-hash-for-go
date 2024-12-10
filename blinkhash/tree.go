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
		root:   NewLNodeHash(1), // 假设默认根节点是一个哈希节点
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
		stack := make([]*INode, 0)

		// Attempt to acquire read lock on the root node.
		curVersion, needRestart := cur.TryReadLock()
		if needRestart {
			continue // Restart the insert process.
		}

		// Tree traversal to find the leaf node.
		for cur.GetLevel() != 0 {
			parent, ok := cur.(*INode)
			if !ok {
				panic("expected *INode")
			}

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
						oldParent = sibling.(*INode)
						parentVersion = siblingVersion
					}

					// 检查是否需要重启父节点处理过程
					if restartParent {
						continue parentRestart
					}
					success, needRestart := oldParent.TryUpgradeWriteLock(oldParent.GetLock())
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
					newParent, newSplitKey := oldParent.Split()
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
							newRoot := NewINodeForHeightGrowth(splitKey, oldParent, newParent, nil, oldParent.GetLevel()+1, newParent.HighKey)
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
			newParent, splitKey := parent.Split()
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
				err := newParent.Insert(key, value, newParent.lock)
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
	bt.lock.Lock()
	defer bt.lock.Unlock()
	// 查找逻辑
	return nil
}

func (bt *BTree) Remove(key interface{}, ti *ThreadInfo) bool {
	bt.lock.Lock()
	defer bt.lock.Unlock()
	// 删除逻辑
	return false
}

func (bt *BTree) Update(key, value interface{}, ti *ThreadInfo) bool {
	bt.lock.Lock()
	defer bt.lock.Unlock()
	// 更新逻辑
	return false
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

			metrics.StructuralDataOccupied += uint64(unsafe.Sizeof(Entry{})*uint64(cnt)) + uint64(unsafe.Sizeof((*NodeInterface)(nil)))
			metrics.StructuralDataUnoccupied += uint64(unsafe.Sizeof(Entry{}) * uint64(invalidNum))

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
		nodeType := leaf.GetType()
		switch nodeType {
		case BTREE_NODE:
			lnodeBTree, ok := leaf.(*LNodeBTree)
			if !ok {
				panic("expected *LNodeBTree")
			}
			cnt := lnodeBTree.GetCount()
			invalidNum := lnodeBTree.Cardinality - cnt

			metrics.KeyDataOccupied += uint64(unsafe.Sizeof(Item{}) * uint64(cnt))
			metrics.KeyDataUnoccupied += uint64(unsafe.Sizeof(Item{}) * uint64(invalidNum))
		case HASH_NODE:
			lnodeHash, ok := leaf.(*LNodeHash)
			if !ok {
				panic("expected *LNodeHash")
			}
			lnodeHash.Footprint(metrics)
		}

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

// sizeofKey 返回Key的大小（示例）。
func sizeofKey() uint64 {
	return uint64(unsafe.Sizeof(int(0))) // 假设Key是int类型
}
