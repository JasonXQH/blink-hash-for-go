package blinkhash

import (
	"runtime"
	"sync/atomic"
)

// Node 定义了 node_t 的 Go 版本
type Node struct {
	lock        uint64
	siblingPtr  *Node
	leftmostPtr *Node
	count       int
	level       int
}

// NewNode 创建并初始化 Node 结构体实例
func NewNode(level int) *Node {
	return &Node{
		level: level,
	}
}

// NewNodeWithSiblings 创建一个新的 Node 实例并初始化相关的指针和计数器
func NewNodeWithSiblings(sibling, left *Node, count, level int) *Node {
	return &Node{
		siblingPtr:  sibling,
		leftmostPtr: left,
		count:       count,
		level:       level,
	}
}

// UpdateMeta 更新 Node 的元数据
func (n *Node) UpdateMeta(siblingPtr *Node, level int) {
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

func (n *Node) GetCnt() int {
	return n.count
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
	return atomic.CompareAndSwapUint64(&n.lock, version, version+0b10)
}

// WriteUnlock 释放写锁
func (n *Node) WriteUnlock() {
	atomic.AddUint64(&n.lock, 0b10)
}

// WriteUnlockObsolete 将节点标记为过时并释放写锁
func (n *Node) WriteUnlockObsolete() {
	atomic.AddUint64(&n.lock, 0b11)
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
