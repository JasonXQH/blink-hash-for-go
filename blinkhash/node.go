package blinkhash

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

// Node 定义了 node_t 的 Go 版本
type Node struct {
	lock        uint64
	siblingPtr  NodeInterface
	leftmostPtr NodeInterface
	count       int32
	level       int
}

func (n *Node) GetSiblingPtr() NodeInterface { return n.siblingPtr }

func (n *Node) GetLeftmostPtr() NodeInterface { return n.leftmostPtr }

func (n *Node) GetType() NodeType {
	return BASENode
}

func (n *Node) GetCount() int32 {
	return n.count
}

func (n *Node) GetLevel() int {
	return n.level
}

func (n *Node) GetLock() uint64 {
	return n.lock
}

func (n *Node) GetHighKey() interface{} { return nil }

// Print 函数打印 Node 信息
func (n *Node) Print() {
	// 打印 Node 的基本信息
	fmt.Printf("Node Information:\n")
	fmt.Printf("Lock: %d\n", n.lock)
	fmt.Printf("Count: %d\n", n.count)
	fmt.Printf("Level: %d\n", n.level)

	// 打印 siblingPtr 和 leftmostPtr 信息（假设它们是 NodeInterface 类型）
	if n.siblingPtr != nil {
		fmt.Println("Sibling Pointer: (non-nil)")
	} else {
		fmt.Println("Sibling Pointer: nil")
	}

	if n.leftmostPtr != nil {
		fmt.Println("Leftmost Pointer: (non-nil)")
	} else {
		fmt.Println("Leftmost Pointer: nil")
	}
}

func (n *Node) SanityCheck(prevHighKey interface{}, first bool) {
	//打印SanityCheck信息
	fmt.Println("Node执行SanityCheck")
}

// NewNode 创建并初始化 Node 结构体实例
func NewNode(level int) *Node {
	return &Node{
		level: level,
	}
}

// NewNodeWithSiblings 创建一个新的 Node 实例并初始化相关的指针和计数器
func NewNodeWithSiblings(sibling, left NodeInterface, count int32, level int) *Node {
	return &Node{
		siblingPtr:  sibling,
		leftmostPtr: left,
		count:       count,
		level:       level,
	}
}

// UpdateMeta 更新 Node 的元数据
func (n *Node) UpdateMeta(siblingPtr NodeInterface, level int) {
	atomic.StoreUint64(&n.lock, 0) // 重置锁为未锁定
	n.siblingPtr = siblingPtr
	n.leftmostPtr = nil
	n.count = 0
	n.level = level
}

// IsLocked 检查版本是否被锁定
func (n *Node) IsLocked(version uint64) bool {
	return (version & 0b10) == 0b10
}

// IsObsolete 检查版本是否过时
func (n *Node) IsObsolete(version uint64) bool {
	return (version & 1) == 1
}

// GetVersion 获取当前版本，检查是否需要重启
func (n *Node) GetVersion() (uint64, bool) {
	version := atomic.LoadUint64(&n.lock)
	needRestart := n.IsLocked(version) || n.IsObsolete(version)
	return version, needRestart
}

// TryReadLock 尝试进行读锁定
func (n *Node) TryReadLock() (uint64, bool) {
	version := atomic.LoadUint64(&n.lock)
	needRestart := n.IsLocked(version) || n.IsObsolete(version)
	return version, needRestart
}

// WriteLock 尝试获取写锁
func (n *Node) WriteLock() {
	for {
		version := atomic.LoadUint64(&n.lock)
		if atomic.CompareAndSwapUint64(&n.lock, version, version+0b10) {
			return
		}
	}
}

// TryWriteLock 尝试获取写锁，如果成功返回 true
func (n *Node) TryWriteLock() bool {
	version := atomic.LoadUint64(&n.lock)
	if n.IsLocked(version) || n.IsObsolete(version) {
		runtime.Gosched() // 让出时间片，相当于 _mm_pause()
		return false
	}
	return atomic.CompareAndSwapUint64(&n.lock, version, version+0b10)
}

// WriteUnlock 释放写锁
func (n *Node) WriteUnlock() {
	// 检查当前节点是否上锁，如果没有锁定，则直接返回
	version := atomic.LoadUint64(&n.lock)
	if !n.IsLocked(version) {
		return // 如果没有锁定，就不执行解锁操作
	}
	// 如果已上锁，释放写锁
	atomic.AddUint64(&n.lock, 0b10)
}

// WriteUnlockObsolete 将节点标记为过时并释放写锁
// WriteUnlockObsolete 将节点标记为过时并释放写锁
func (n *Node) WriteUnlockObsolete() {
	for {
		old := atomic.LoadUint64(&n.lock)
		new := old - 0b11
		if atomic.CompareAndSwapUint64(&n.lock, old, new) {
			return
		}
		// 如果 CompareAndSwap 失败，重试
	}
}
func (n *Node) GetEntries() []Entry {
	return nil
}

// TryUpgradeWriteLock 尝试升级写锁，如果版本不匹配或不能锁定则设置需要重启标志
func (n *Node) TryUpgradeWriteLock(version uint64) (bool, bool) {
	needRestart := false
	currentVersion := atomic.LoadUint64(&n.lock)
	if version != currentVersion {
		needRestart = true
		return false, needRestart
	}

	// 尝试进行原子更新，增加 0b10 表示获取写锁
	if !atomic.CompareAndSwapUint64(&n.lock, version, version+0b10) {
		runtime.Gosched() // 让出时间片，相当于 _mm_pause()
		needRestart = true
	}
	return !needRestart, needRestart
}
func (n *Node) IncrementCount() {
	atomic.AddInt32(&n.count, 1)
}

func (n *Node) DecrementCount() {
	atomic.AddInt32(&n.count, -1)
}
