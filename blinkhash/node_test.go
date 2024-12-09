package blinkhash

import (
	"sync"
	"testing"
)

// TestNodeCreation 测试 Node 的创建和初始化功能
func TestNodeCreation(t *testing.T) {
	node := NewNode(1)
	if node.level != 1 {
		t.Errorf("Expected level 1, got %d", node.level)
	}

	nodeWithSiblings := NewNodeWithSiblings(node, node, 10, 2)
	if nodeWithSiblings.level != 2 || nodeWithSiblings.count != 10 {
		t.Errorf("Node initialization with siblings failed")
	}
}

// TestLockingMechanisms 测试节点的锁定机制
func TestLockingMechanisms(t *testing.T) {
	node := NewNode(1)

	// 测试写锁
	node.WriteLock()
	if !node.IsLocked(node.lock) {
		t.Errorf("Failed to acquire write lock")
	}
	node.WriteUnlock()
	if node.IsLocked(node.lock) {
		t.Errorf("Failed to release write lock")
	}

	// 测试尝试写锁
	if !node.TryWriteLock() {
		t.Errorf("Failed to acquire write lock via TryWriteLock")
	}
	node.WriteUnlock()

	// 测试升级写锁
	version, _ := node.TryReadLock()
	success, _ := node.TryUpgradeWriteLock(version)
	if !success {
		t.Errorf("Failed to upgrade read lock to write lock")
	}
	node.WriteUnlock()
}

// TestConcurrency 测试 Node 结构在并发环境下的表现
func TestConcurrency(t *testing.T) {
	node := NewNode(0)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node.WriteLock()
			node.WriteUnlock()
		}()
	}
	wg.Wait()
}

// TestObsoleteFlag 测试节点的过时标记
func TestObsoleteFlag(t *testing.T) {
	node := NewNode(1)
	node.WriteLock()
	node.WriteUnlockObsolete()
	if !node.IsObsolete(node.lock) {
		t.Errorf("Failed to mark node as obsolete")
	}
}

// BenchmarkNodeLocking 测试 Node 锁定的性能
func BenchmarkNodeLocking(b *testing.B) {
	node := NewNode(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			node.WriteLock()
			node.WriteUnlock()
		}
	})
}

func TestBatchBuffer(t *testing.T) {
	// 模拟缓冲区数据
	buf := []Entry{
		{Key: "key1", Value: &Node{}},
		{Key: "key2", Value: &Node{}},
		{Key: "key3", Value: &Node{}},
		{Key: "key4", Value: &Node{}},
	}
	bufIdx := 0
	bufNum := len(buf)
	batchSize := 2

	// 创建一个 INode
	inode := NewINode(1, nil, nil, nil)

	// 执行 BatchBuffer
	inode.BatchBuffer(buf, bufIdx, bufNum, batchSize)

	// 验证结果
	if inode.count != batchSize {
		t.Errorf("Expected count to be %d, got %d", batchSize, inode.count)
	}
	if inode.HighKey != "key3" {
		t.Errorf("Expected HighKey to be 'key3', got %v", inode.HighKey)
	}
	if bufIdx != 2 {
		t.Errorf("Expected bufIdx to be 2, got %d", bufIdx)
	}
}
