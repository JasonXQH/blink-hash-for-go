package blinkhash

import (
	"fmt"
	"reflect"
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
		// Ensure to release the root lock if restarting
		defer func() {
			if restart {
				cur.WriteUnlock()
			}
		}()
		// Tree traversal to find the leaf node.
		for cur.GetLevel() != 0 {
			parent, ok := cur.(INodeInterface)
			if !ok {
				panic("Need INodeInterface")
			}
			child := parent.ScanNode(key)
			if child == nil {
				panic("ScanNode returned nil")
			}
			childVersion, needRestart := child.TryReadLock()

			if needRestart {
				parent.WriteUnlock()
				restart = true
				break
			}

			// Check version consistency.
			curEndVersion, needRestart := cur.GetVersion()
			if needRestart || curVersion != curEndVersion {
				parent.WriteUnlock() // Release parent lock
				child.WriteUnlock()  // Release child lock
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
			cur.WriteUnlock()
			continue insertLoop // 跳转到最外层循环开始
		}

		leafNode, ok := cur.(LeafNodeInterface)
		if !ok {
			panic("expected LeafNodeInterface")
		}
		leafVersion := curVersion

		// Check if we need to traverse to the sibling leaf node.
		for leafNode.GetSiblingPtr() != nil && compareIntKeys(leafNode.GetHighKey(), key) < 0 {
			sibling, ok := leafNode.GetSiblingPtr().(LeafNodeInterface)
			if !ok {
				panic("expected *LNodeHash")
			}

			siblingVersion, needRestart := sibling.TryReadLock()
			if needRestart {
				leafNode.WriteUnlock() // Release leafNode lock
				sibling.WriteUnlock()
				restart = true
				break
			}

			leafEndVersion, needRestart := leafNode.GetVersion()
			if needRestart || leafVersion != leafEndVersion {
				sibling.WriteUnlock()  // Release sibling lock
				leafNode.WriteUnlock() // Release leafNode lock
				restart = true
				break
			}

			leafNode = sibling
			leafVersion = siblingVersion
		}

		if restart {
			leafNode.WriteUnlock()
			continue insertLoop // 跳转到最外层循环开始
		}

		//这里的Insert，应该是调用Insertable，而不是调用LNodeHash中的Insert
		//应该是根据leaf的类型来执行不同的Insert
		// Attempt to insert into the leaf node.
		ret := leafNode.Insert(key, value, leafVersion)
		if ret == NeedRestart { // Leaf node has been split during insertion.
			leafNode.WriteUnlock() // Release leafNode lock
			continue               // Restart the insert process.
		} else if ret == InsertSuccess { // Insertion succeeded.
			//TODO: 刷新parent的HighKey
			// 1) 先检查是否新插入的 key > leafNode.HighKey
			if compareIntKeys(key, leafNode.GetHighKey()) > 0 {
				leafNode.SetHighKey(key)
			}
			leafNode.WriteUnlock()

			// 2) 从栈顶往上遍历父节点，看是否需要更新 HighKey
			//    注意要对父节点做相应的加锁更新，避免并发问题
			for i := len(stack) - 1; i >= 0; i-- {
				parent := stack[i]
				parentVersion, needRestart := parent.TryReadLock()
				if needRestart {
					// 如果需要重启，可以 break 或者先行处理
					// 这里简化写法：先不做复杂的 restart，直接尝试一下
					continue
				}
				// 升级为写锁
				ok, needRestart := parent.TryUpgradeWriteLock(parentVersion)
				if !ok || needRestart {
					parent.WriteUnlock()
					// 同理，这里也可以再 break / continue 进行更复杂的重启处理
					continue
				}

				if compareIntKeys(key, parent.GetHighKey()) > 0 {
					parent.SetHighKey(key)
				}

				// 解锁
				parent.WriteUnlock()
			}
			return
		} else { // Leaf node split.
			splittableLeaf, splitKey := leafNode.Split(key, value, leafVersion)
			if splittableLeaf == nil { // 另一线程已分裂该叶子节点
				leafNode.WriteUnlock() // Release leafNode lock
				continue               // 重启插入过程
			}

			newNode, ok := splittableLeaf.(NodeInterface)
			if !ok {
				panic("expected LeafNodeInterface new leaf node")
			}

			if len(stack) > 0 {
				stackIdx := len(stack) - 1
				oldParent := stack[stackIdx]
			parentRestart:
				for stackIdx >= 0 {
					oldParent = stack[stackIdx]
					originalNode := leafNode
					restartParent := false
					// Attempt to acquire write lock on the parent node.
					parentVersion, needRestart := oldParent.TryReadLock()
					if needRestart {
						oldParent.WriteUnlock() // Release oldParent's read lock
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
							oldParent.WriteUnlock()
							restartParent = true
							break // 跳出循环，准备重启
						}

						parentEndVersion, needRestart := oldParent.GetVersion()
						if needRestart || parentVersion != parentEndVersion {
							oldParent.WriteUnlock()
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
					success, needRestart := oldParent.TryUpgradeWriteLock(oldParent.GetLock())
					if !success || needRestart {
						oldParent.WriteUnlock()
						continue parentRestart
					}
					if originalNode.GetLevel() != 0 {
						originalNode.WriteUnlock()
					} else {
						originalNode.(LeafNodeInterface).WriteUnlock()
					}
					//else {
					//	originalNode.(LeafNodeInterface).WriteUnlock()
					//}

					if !oldParent.IsFull() { // Normal insert.
						//重点关注这个splitKey
						oldParent.Insert(splitKey, newNode, oldParent.GetLock())
						// ——> 在这里判断若 splitKey > oldParent.HighKey，就更新
						if compareIntKeys(newNode.GetHighKey(), oldParent.GetHighKey()) > 0 {
							oldParent.SetHighKey(newNode.GetHighKey())
						}
						//bt.PrintTree()
						oldParent.WriteUnlock()
						return
					}

					// Internal node split.
					newParent, newSplitKey := oldParent.Split()
					if compareIntKeys(splitKey, newSplitKey) <= 0 {
						oldParent.Insert(splitKey, newNode, oldParent.GetLock())
						// 若 splitKey > oldParent.HighKey，更新 oldParent
						if compareIntKeys(newNode.GetHighKey(), oldParent.GetHighKey()) > 0 {
							oldParent.SetHighKey(newNode.GetHighKey())
						}
					} else {
						newParent.Insert(splitKey, newNode, newParent.GetLock())
						// 若 splitKey > newParent.HighKey，更新 newParent
						if compareIntKeys(newNode.GetHighKey(), newParent.GetHighKey()) > 0 {
							newParent.SetHighKey(newNode.GetHighKey())
						}
					}

					oldParent.WriteUnlock()
					if stackIdx > 0 {
						splitKey = newSplitKey
						stackIdx--
						oldParent = stack[stackIdx]
						newNode = newParent
					} else { // set new root
						if oldParent == bt.root {
							newRoot := NewINodeForHeightGrowth(oldParent.GetHighKey(), oldParent, newParent, nil, oldParent.GetLevel()+1, newParent.GetHighKey())
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
					newRoot := NewINodeForHeightGrowth(splitKey, leafNode, newNode, nil, leafNode.GetLevel()+1, newNode.GetHighKey())
					bt.root = newRoot
				} else { // Another thread has already created a new root.
					bt.insertKey(splitKey, newNode, leafNode)
				}
				leafNode.WriteUnlock() // Ensure to release leafNode lock
				return
			}

		}
	}
}

// insertKey is called when the root has been split by another thread.
// It inserts a key and node pointers into the B-tree.
func (bt *BTree) insertKey(key interface{}, value NodeInterface, prev NodeInterface) {

insertLoop:
	for {
		cur := bt.root
		// 尝试获取根节点的读锁
		curVersionStart, needRestart := cur.TryReadLock()
		if needRestart {
			// root锁没拿到 -> 重试
			continue insertLoop
		}

		parentIF, ok := cur.(INodeInterface)
		if !ok {
			// 解锁后panic
			cur.WriteUnlock()
			panic("expected INodeInterface")
		}
		// 遍历树，找到 level = prev.level + 1 的内部节点
		for parentIF.GetLevel() != prev.GetLevel()+1 {
			child := parentIF.ScanNode(key)
			if child == nil {
				// 解锁 -> panic
				parentIF.WriteUnlock()
				cur.WriteUnlock()
				panic("ScanNode returned nil")
			}
			childVersion, cNeedRestart := child.TryReadLock()
			if cNeedRestart {
				// 解锁 -> 重试
				parentIF.WriteUnlock()
				cur.WriteUnlock()
				continue insertLoop
			}
			// 版本一致性检查
			curEndVersion, verNeedRestart := cur.GetVersion()
			if verNeedRestart || (curVersionStart != curEndVersion) {
				child.WriteUnlock()
				parentIF.WriteUnlock()
				cur.WriteUnlock()
				continue insertLoop
			}
			// 下探：先解锁parent & cur
			parentIF.WriteUnlock()
			cur.WriteUnlock()

			// 下探
			cur = child
			curVersionStart = childVersion

			// 更新 parentIF
			pIF, ok := cur.(INodeInterface)
			if !ok {
				cur.WriteUnlock()
				panic("expected INodeInterface in insertKey down path")
			}
			parentIF = pIF
		}
		// 查找需要插入的位置
		for parentIF.GetSiblingPtr() != nil && compareIntKeys(parentIF.GetHighKey(), key) < 0 {
			sibling := parentIF.GetSiblingPtr()
			siblingVersionStart, sNeedRestart := sibling.TryReadLock()
			if sNeedRestart {
				// 解锁 -> 重试
				parentIF.WriteUnlock()
				cur.WriteUnlock()
				continue insertLoop
			}

			parentEndVersion, pNeedRestart := parentIF.GetVersion()
			if pNeedRestart || curVersionStart != parentEndVersion {
				sibling.WriteUnlock()
				parentIF.WriteUnlock()
				cur.WriteUnlock()
				continue insertLoop
			}
			// 下一个兄弟
			parentIF.WriteUnlock()
			cur.WriteUnlock()

			parentIF = sibling.(INodeInterface)
			cur = sibling
			curVersionStart = siblingVersionStart
		}

		// 尝试升级为写锁
		success, needRestart := parentIF.TryUpgradeWriteLock(curVersionStart)
		if needRestart || !success {
			// 升级失败 -> 解锁 -> 重试
			parentIF.WriteUnlock()
			continue
		}

		// 解锁 prev 节点
		prev.WriteUnlock()

		// 检查父节点是否已满
		if !parentIF.IsFull() {
			err := parentIF.Insert(key, value, parentIF.GetLock())
			if err != InsertSuccess {
				parentIF.WriteUnlock()
				panic("parent.Insert failed!")
				return
			}
			parentIF.WriteUnlock()
			return
		} else {
			// 父节点分裂
			newParent, splitKey := parentIF.Split()
			if !ok {
				panic("newParentSplittableInterface cannot be INode")

			}
			if compareIntKeys(key, splitKey) <= 0 {
				err := parentIF.Insert(key, value, parentIF.GetLock())
				if err != InsertSuccess {
					parentIF.WriteUnlock()
					panic("parent.Insert failed!")
				}
			} else {
				err := newParent.Insert(key, value, newParent.GetLock())
				if err != InsertSuccess {
					parentIF.WriteUnlock()
					panic("parent.Insert failed!")
				}
			}

			if parentIF == bt.root {
				// 创建新的根节点.newParent成为了INodeInterface
				newRoot := NewINodeForHeightGrowth(splitKey, parentIF, newParent, nil, parentIF.GetLevel()+1, parentIF.GetHighKey())
				bt.root = newRoot
				parentIF.WriteUnlock()
			} else {
				// 递归插到更高层
				parentIF.WriteUnlock()
				bt.insertKey(splitKey, newParent, cur)
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
		parent, ok := cur.(INodeInterface)
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
		parent, ok := cur.(INodeInterface)
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
		parent, ok := cur.(INodeInterface)
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
func (bt *BTree) BatchInsert(keys []interface{}, values []NodeInterface, num int, prev NodeInterface, ti *ThreadInfo) {
	eg := NewEpocheGuard(ti)
	defer eg.Release()

batchLoop:
	for {
		// 1) 尝试对 root 节点加读锁
		cur := bt.root
		curVersion, needRestart := cur.TryReadLock()
		if needRestart {
			// 没拿到 root 锁就重启
			continue batchLoop
		}

		// 2) 下探到 `prev` 的父层级 => level = prev.GetLevel() + 1
		for cur.GetLevel() != (prev.GetLevel() + 1) {
			parent, ok := cur.(INodeInterface)
			if !ok {
				// 出现异常类型 -> 解锁后panic
				cur.WriteUnlock()
				panic("expected INodeInterface for parent node")
			}
			child := parent.ScanNode(keys[0])
			if child == nil {
				// 解锁 parent 和 cur
				parent.WriteUnlock()
				cur.WriteUnlock()
				panic("ScanNode returned nil")
			}

			childVersion, cNeedRestart := child.TryReadLock()
			if cNeedRestart {
				// 解锁 parent & cur -> 重启
				parent.WriteUnlock()
				cur.WriteUnlock()
				continue batchLoop
			}

			curEndVersion, cNeedRestart2 := cur.GetVersion()
			if cNeedRestart2 || (curVersion != curEndVersion) {
				// 解锁 child & parent & cur -> 重启
				child.WriteUnlock()
				parent.WriteUnlock()
				cur.WriteUnlock()
				continue batchLoop
			}
			// 解锁 parent
			parent.WriteUnlock()
			cur.WriteUnlock()
			// 下探
			cur = child
			curVersion = childVersion

		}

		// 此时 cur.GetLevel() == prev.GetLevel() + 1
		// 3) 移动到与keys[0] 更匹配的兄弟节点 (若有)
		for cur.GetSiblingPtr() != nil && compareIntKeys(cur.GetHighKey(), keys[0]) < 0 {
			sibling := cur.GetSiblingPtr()
			siblingVersion, sNeedRestart := sibling.TryReadLock()
			if sNeedRestart {
				// 解锁 cur -> 重启
				cur.WriteUnlock()
				continue batchLoop
			}

			curEndVersion, cNeedRestart2 := cur.GetVersion()
			if cNeedRestart2 || (curVersion != curEndVersion) {
				sibling.WriteUnlock()
				cur.WriteUnlock()
				continue batchLoop
			}

			// 下一个兄弟
			cur.WriteUnlock()
			cur = sibling
			curVersion = siblingVersion
		}

		// 4) 尝试升级为写锁
		parentNode, ok := cur.(INodeInterface)
		if !ok {
			cur.WriteUnlock()
			panic("expected INodeInterface")
		}
		success, upNeedRestart := parentNode.TryUpgradeWriteLock(curVersion)
		if upNeedRestart || !success {
			cur.WriteUnlock()
			continue batchLoop
		}

		// 5) 解锁 prev
		if prev.GetLevel() == 0 {
			// 如果 prev 是叶子节点, 需要 obsolete
			values[0].WriteUnlock() // 解锁 new leaf (?)
			leaf, ok := prev.(LeafNodeInterface)
			if !ok {
				parentNode.WriteUnlock()
				panic("expected LeafNodeInterface for prev")
			}
			leaf.WriteUnlockObsolete()
			ti.Epoche.MarkNodeForDeletion(prev, ti)
		} else {
			prev.WriteUnlock()
		}

		// 6) 进入批量插入逻辑
		parent, ok := cur.(INodeInterface)
		if !ok {
			parentNode.WriteUnlock()
			panic("expected INodeInterface for parent node")
		}

		var newNodes []INodeInterface
		var err error
		// 根据 parent 层级决定要调用哪个批量插入方法
		if parent.GetLevel() == 1 {
			// 最后一层父节点 -> batch insert last level
			newNodes, err = parent.BatchInsertLastLevel(keys, values, num, 0)
		} else {
			// 内部节点
			newNodes, err = parent.BatchInsert(keys, values, num)
		}
		if err != nil {
			// 根据需要处理错误
			parent.WriteUnlock()
			panic(err)
		}

		// 如果没产生新的 node，就直接返回
		if newNodes == nil || len(newNodes) == 0 {
			parent.WriteUnlock()
			return
		}

		newNum := len(newNodes)
		// 生成 splitKey
		splitKey := make([]interface{}, newNum)
		splitKey[0] = parent.GetHighKey()
		for i := 1; i < newNum; i++ {
			splitKey[i] = newNodes[i-1].GetHighKey()
		}

		if parent != bt.root {
			// 非root, 递归插到更高层
			parent.WriteUnlock()
			bt.BatchInsert(splitKey, nodeInterfaceForINodeInterface(newNodes), newNum, parent, ti)
		} else {
			// 若就是 root, 可能需要多层调整
			// 参考C++逻辑: NewRootForAdjustment
			for parent.GetCardinality() < newNum {
				newRoots, newSize := bt.NewRootForAdjustment(splitKey, nodeInterfaceForINodeInterface(newNodes), newNum)
				newNodes = newRoots
				newNum = newSize
			}

			// 生成新根
			newRoot := NewINodeForInsertInBatch(newNodes[0].GetLevel() + 1)
			newRoot.InsertForRoot(splitKey, nodeInterfaceForINodeInterface(newNodes), parent, newNum)
			bt.root = newRoot
			parent.WriteUnlock()
		}
		return
	}
}

func (bt *BTree) NewRootForAdjustment(key []interface{}, value []NodeInterface, num int) ([]INodeInterface, int) {
	// 在C++中FILL_FACTOR使用宏定义，这里您可自行定义FILL_FACTOR常量
	// 假设FILL_FACTOR为0.7或其他值，根据实际情况定义
	batch_size := int(float64(value[0].GetCount()) * FillFactor)

	var new_num int
	if num%batch_size == 0 {
		new_num = num / batch_size
	} else {
		new_num = num/batch_size + 1
	}

	new_roots := make([]INodeInterface, new_num)
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
			new_roots[i].SetSibling(new_roots[i+1])
		}
	}
	return new_roots, new_num
}

func nodeInterfaceForINodeInterface(nodes []INodeInterface) []NodeInterface {
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

// RangeLookup performs a range lookup starting from minKey, collecting up to rng items.
// Returns the collected results in a slice.
func (bt *BTree) RangeLookup(minKey interface{}, rng int, ti *ThreadInfo) []interface{} {
	eg := NewEpocheGuard(ti)
	defer eg.Release()
	//const maxAttempts = 100 // 你可以根据需求适当调大
	//attempts := 0
rangeLoop:
	for {
		//attempts++
		//if attempts > maxAttempts {
		//	// 达到最大重试次数，防止无限循环
		//	fmt.Printf("RangeLookup: too many attempts (%d). Abort this query.\n", attempts)
		//	// 这里你可以选择返回空 slice、或返回nil、或抛出异常
		//	return nil
		//}
		results := make([]interface{}, 0, rng) // 用来收集本次查询的结果
		cur := bt.root
		curVersion, needRestart := cur.TryReadLock()
		if needRestart {
			continue rangeLoop
		}

		// 1) 从根下探到叶子
		for cur.GetLevel() != 0 {
			parent, ok := cur.(INodeInterface)
			if !ok {
				cur.WriteUnlock()
				panic(fmt.Sprintf("Need INodeInterface, got: %T (%v)", cur, reflect.TypeOf(cur)))
			}
			child := parent.ScanNode(minKey)
			if child == nil {
				parent.WriteUnlock()
				cur.WriteUnlock()
				panic("ScanNode returned nil")
			}
			childVersion, childNeedRestart := child.TryReadLock()
			if childNeedRestart {
				//fmt.Println("need loop, childVersion: ", childVersion)
				parent.WriteUnlock()
				cur.WriteUnlock()
				continue rangeLoop
			}

			// Check version consistency
			curEndVersion, curNeedRestart := cur.GetVersion()
			if curNeedRestart || (curVersion != curEndVersion) {
				child.WriteUnlock()
				parent.WriteUnlock()
				cur.WriteUnlock()
				//fmt.Println("need loop, curEndVersion: ", curEndVersion)
				continue rangeLoop
			}
			// 释放父节点
			parent.WriteUnlock()
			cur.WriteUnlock()
			// 下探到 child
			cur = child
			curVersion = childVersion
		}

		// 2) 此时 cur 为叶节点
		leaf, ok := cur.(LeafNodeInterface)
		if !ok {
			cur.WriteUnlock()
			panic(fmt.Sprintf("expected LeafNodeInterface, got %T", cur))
		}
		leafVersion := curVersion
		continued := false

		// 3) 不断在当前或兄弟节点中收集，直到 results >= rng
		for len(results) < rng {
			// a) 如果当前叶节点的HighKey < minKey，则说明要去兄弟节点
			for leaf.GetSiblingPtr() != nil && compareIntKeys(leaf.GetHighKey(), minKey) < 0 {
				sibling := leaf.GetSiblingPtr()
				siblingVersion, sibRestart := sibling.TryReadLock()
				if sibRestart {
					leaf.WriteUnlock()
					//fmt.Println("need loop, siblingVersion: ", siblingVersion)
					continue rangeLoop
				}

				leafEndVersion, leafNeedRestart := leaf.GetVersion()
				if leafNeedRestart || (leafVersion != leafEndVersion) {
					//fmt.Println("need loop, leafNeedRestart: ", leafNeedRestart)
					sibling.WriteUnlock()
					leaf.WriteUnlock()
					continue rangeLoop
				}
				// 切换到 sibling
				leaf.WriteUnlock()
				lf, ok := sibling.(LeafNodeInterface)
				if !ok {
					sibling.WriteUnlock()
					panic("expected LeafNodeInterface")
				}
				leaf = lf
				leafVersion = siblingVersion
			}

			// b) 调用叶节点的 RangeLookUp
			//    注意这里的 continued 可以根据需要来设置
			//    初次进入可以传 continued=false，若需要多次在同节点内收集则可传true
			collected, retCode, _ := leaf.RangeLookUp(minKey, rng-len(results), continued, leafVersion)
			if retCode == NeedRestart {
				leaf.WriteUnlock()
				continue rangeLoop
			} else if retCode == NeedConvert {
				//fmt.Println("需要将LNodeHash转换为LNodeBtree,LeafNode:")
				//printNode(leaf, "", false)
				//bt.PrintTree()
				bt.convert(leaf, leafVersion, ti)
				//fmt.Println("转换完成，打印Tree")
				//bt.PrintTree()
				leaf.WriteUnlock()
				continue rangeLoop
			}
			continued = true
			// 追加收集结果
			results = append(results, collected...)

			// 如果已经收集够了 rng
			if len(results) >= rng {
				leaf.WriteUnlock()
				return results
			}

			// c) 检查兄弟节点
			sibling := leaf.GetSiblingPtr()
			leafEndVersion, needRestart := leaf.GetVersion()
			if needRestart || (leafVersion != leafEndVersion) {
				// 版本变化 -> 解锁 -> 重启
				leaf.WriteUnlock()
				continue rangeLoop
			}

			if sibling == nil {
				// 到达最右叶子,结束
				leaf.WriteUnlock()
				return results
			}

			// 切换到 sibling
			siblingVersion, sibRestart := sibling.TryReadLock()
			if sibRestart {
				leaf.WriteUnlock()
				//fmt.Println("need loop, siblingVersion: ", siblingVersion)
				continue rangeLoop
			}
			leafEndVersion, leafNeedRestart := leaf.GetVersion()
			if leafNeedRestart || (leafVersion != leafEndVersion) {
				sibling.WriteUnlock()
				leaf.WriteUnlock()
				//fmt.Println("need loop, leafNeedRestart: ", leafNeedRestart)
				continue rangeLoop
			}

			leaf.WriteUnlock()

			lf, ok := sibling.(LeafNodeInterface)
			if !ok {
				sibling.WriteUnlock()
				panic("expected LeafNodeInterface")
			}
			leaf = lf
			leafVersion = siblingVersion
		}

		// 如果循环退出时 results>=rng 或无兄弟 => 结束
		leaf.WriteUnlock()
		return results
	}
}

// convert 对叶子节点进行转换，与C++一致
func (bt *BTree) convert(leaf LeafNodeInterface, leafVersion uint64, ti *ThreadInfo) bool {
	hashNode, ok := leaf.(*LNodeHash)
	if !ok {
		panic("Need leaf to be LNodeHashs")
	}
	bTreeNodes, num, err := hashNode.Convert(leafVersion)
	if err != nil {
		return false
	}
	if bTreeNodes == nil {
		return false
	}
	split_key := make([]interface{}, num)
	split_key[0] = bTreeNodes[0].GetHighKey()
	for i := 1; i < num; i++ {
		split_key[i] = bTreeNodes[i-1].GetHighKey()
	}

	// 检查 prev 是否为根节点
	if leaf == bt.root {
		// 创建一个新的内部根节点
		newRoot := NewINodeForInsertInBatch(bTreeNodes[0].GetLevel() + 1)
		newRoot.InsertForRoot(split_key, nodeInterfaceSliceForBTreeNode(bTreeNodes), bTreeNodes[0], num)
		bt.root = newRoot
		// 释放旧根节点的锁并标记为待删除
		bTreeNodes[0].WriteUnlock()
		hashNode.WriteUnlockObsolete()
		ti.Epoche.MarkNodeForDeletion(leaf, ti)
		return true
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
			invalidNum := internal.Cardinality - int(cnt)

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

func (bt *BTree) PrintTree() {
	bt.lock.Lock()
	defer bt.lock.Unlock()
	fmt.Println("BTree Structure:")
	printNode(bt.root, "", true)
}

func printNode(n NodeInterface, prefix string, isTail bool) {
	if n == nil {
		// 打印空指针的情况
		fmt.Printf("%s%s <NIL>\n", prefix, leafConnector(isTail))
		return
	}

	// 打印节点的基本信息
	nodeType := n.GetType()
	var nodeDesc string
	switch nodeType {
	case INNERNode: // 假设 INodeType 表示内部节点类型
		nodeDesc = fmt.Sprintf("INode(level=%d, highKey=%v, count=%d)", n.GetLevel(), n.GetHighKey(), n.GetCount())
	case BTreeNode:
		nodeDesc = fmt.Sprintf("LNodeBTree(level=%d, highKey=%v, count=%d)", n.GetLevel(), n.GetHighKey(), n.GetCount())
	case HashNode:
		nodeDesc = fmt.Sprintf("LNodeHash(level=%d, highKey=%v, count=%d)", n.GetLevel(), n.GetHighKey(), n.GetCount())
	default:
		nodeDesc = fmt.Sprintf("UnknownNodeType(level=%d, highKey=%v, count=%d)", n.GetLevel(), n.GetHighKey(), n.GetCount())
	}

	fmt.Printf("%s%s %s\n", prefix, leafConnector(isTail), nodeDesc)

	// 根据节点类型递归打印其子节点或叶子信息
	switch nodeType {
	case INNERNode:
		in := n.(*INode)

		// （1）先打印 leftmostPtr 指向的子节点（如果有）
		//     它代表最左侧子树，不属于 in.Entries[] 数组
		if in.leftmostPtr != nil {
			// isLastChild = 当 count=0 时, leftmostPtr 就是唯一的子节点
			isLastChild := (in.count == 0)
			newPrefix := prefix + nextLevelPrefix(isTail)

			// 给它起个名字，比如 "Leftmost child" 或者直接打印
			fmt.Printf("%s%s LeftmostChildPtr\n",
				newPrefix, leafConnector(!isLastChild))

			printNode(in.leftmostPtr.(NodeInterface), newPrefix, isLastChild)
		}

		// （2）遍历 [0..in.count-1] 的 entries
		//     对每个 entry，先打印 “Key”，再打印 entry[i].Value
		for i := 0; i < int(in.count); i++ {
			isLastEntry := i == int(in.count)-1
			newPrefix := prefix + nextLevelPrefix(isTail)

			// 打印这个条目对应的键
			// 如果想把“Key:”对齐或缩进美观，也可以在这里调格式
			fmt.Printf("%s    Key: %v\n", newPrefix, in.Entries[i].Key)
			// 再打印它指向的子节点
			// 这里假设 entry.Value 一定是 NodeInterface
			childNode := in.Entries[i].Value.(NodeInterface)
			printNode(childNode, newPrefix, isLastEntry)
		}

	case BTreeNode:
		// LNodeBTree：有若干 Entries，没有子节点
		//ln := n.(*LNodeBTree)
		//for i, entry := range ln.Entries {
		//	isLast := (i == len(ln.Entries)-1)
		//	newPrefix := prefix + nextLevelPrefix(isTail)
		//	fmt.Printf("%s%s Entry: %v\n", newPrefix, leafConnector(isLast), entry.Key)
		//}

	case HashNode:
		// LNodeHash：有Buckets，每个Bucket中有entries
		//ln := n.(*LNodeHash)
		//for bIndex, bucket := range ln.Buckets {
		//	isBucketLast := (bIndex == len(ln.Buckets)-1)
		//	newPrefix := prefix + nextLevelPrefix(isTail)
		//	fmt.Printf("%s%s Bucket #%d [state=%v]\n", newPrefix, leafConnector(isBucketLast), bIndex, bucket.state)
		//	// 打印 Bucket 内的 entries
		//	for eIndex, entry := range bucket.entries {
		//		isEntryLast := (eIndex == len(bucket.entries)-1)
		//		entryPrefix := newPrefix + nextLevelPrefix(isBucketLast)
		//		fmt.Printf("%s%s Entry: %v\n", entryPrefix, leafConnector(isEntryLast), entry.Key)
		//	}
		//}

	default:
		// 其他未识别类型，不做特殊处理
	}
}

// 以下是辅助函数

func leafConnector(isTail bool) string {
	if isTail {
		return "└──"
	} else {
		return "├──"
	}
}

func nextLevelPrefix(isTail bool) string {
	if isTail {
		return "    "
	} else {
		return "│   "
	}
}
