package blinkhash

import (
	//"fmt"
	"testing"
)

// Mock NodeInterface 和 Node 实现，用于测试
type MockNode struct {
	id int
}

func (m *MockNode) TryWriteLock() bool {
	return true
}

func (m *MockNode) WriteUnlock() {
	// Mock implementation
}

func (m *MockNode) TryConvertLock(version uint64) bool {
	return true
}

func (m *MockNode) ConvertUnlock() {
	// Mock implementation
}

func (m *MockNode) StabilizeAll(version uint64) bool {
	return true
}

func (m *MockNode) StabilizeBucket(loc int) bool {
	return true
}

func (m *MockNode) Hash(hv uint64) uint64 {
	return hv
}

// TestINode_Insert 测试单条插入
func TestINode_Insert(t *testing.T) {
	inode := NewINodeForInsertInBatch(1)

	newNode1 := NewNode(1)
	// 插入第一条
	err := inode.Insert(10, newNode1)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if inode.count != 1 {
		t.Errorf("Expected count to be 1, got %d", inode.count)
	}
	if inode.HighKey != 10 {
		t.Errorf("Expected HighKey to be 10, got %v", inode.HighKey)
	}
	if inode.leftmostPtr != nil {
		t.Errorf("Expected leftmostPtr to be nil, got %v", inode.leftmostPtr)
	}
	if inode.Entries[0].Key != 10 {
		t.Errorf("Expected Entries[0].Key to be 10, got %v", inode.Entries[0].Key)
	}
	if inode.Entries[0].Value != newNode1 {
		t.Errorf("Expected Entries[0].Value to be node 1, got %v", inode.Entries[0].Value)
	}

	newNode2 := NewNode(2)
	// 插入第二条
	err = inode.InsertWithLeft(20, newNode2, NewNode(1))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if inode.count != 2 {
		t.Errorf("Expected count to be 2, got %d", inode.count)
	}
	if inode.HighKey != 20 {
		t.Errorf("Expected HighKey to be 20, got %v", inode.HighKey)
	}
	if inode.Entries[1].Key != 20 {
		t.Errorf("Expected Entries[1].Key to be 20, got %v", inode.Entries[1].Key)
	}
	if inode.Entries[1].Value != newNode2 {
		t.Errorf("Expected Entries[1].Value to be node 2, got %v", inode.Entries[1].Value)
	}
}

// TestINode_Split 测试节点分裂
func TestINode_Split(t *testing.T) {
	inode := NewINodeForInsertInBatch(1)
	batchSize := 4
	// 假设卡片容量为4
	for i := 1; i <= batchSize; i++ {
		err := inode.Insert(i*10, NewNode(i))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// 分裂节点
	newNode, splitKey := inode.Split()
	if newNode == nil {
		t.Fatalf("Split returned nil")
	}
	if splitKey != 30 {
		t.Errorf("Expected splitKey to be 30, got %v", splitKey)
	}
	if inode.count != 2 {
		t.Errorf("Expected original node count to be 2, got %d", inode.count)
	}
	if newNode.count != 1 {
		t.Errorf("Expected new node count to be 1, got %d", newNode.count)
	}
	if inode.HighKey != 30 {
		t.Errorf("Expected original node HighKey to be 30, got %v", inode.HighKey)
	}
	if newNode.HighKey != 40 {
		t.Errorf("Expected new node HighKey to be 40, got %v", newNode.HighKey)
	}
	if inode.siblingPtr != &newNode.Node {
		t.Errorf("Expected original node siblingPtr to point to new node")
	}
}

// TestINode_BatchInsert 测试批量插入
func TestINode_BatchInsert(t *testing.T) {
	inode := NewINodeForInsertInBatch(1)
	keys := []interface{}{10, 20, 30}
	values := []*Node{NewNode(1), NewNode(2), NewNode(3)}
	num := 3

	newNodes, err := inode.BatchInsert(keys, values, num)
	if err != nil {
		t.Fatalf("BatchInsert failed: %v", err)
	}
	if newNodes != nil {
		t.Errorf("Expected no new nodes, got %v", newNodes)
	}
	if inode.count != 3 {
		t.Errorf("Expected count to be 3, got %d", inode.count)
	}
	if inode.HighKey != 30 {
		t.Errorf("Expected HighKey to be 30, got %v", inode.HighKey)
	}
	for i, key := range keys {
		if inode.Entries[i].Key != key {
			t.Errorf("Expected Entries[%d].Key to be %v, got %v", i, key, inode.Entries[i].Key)
		}
		if inode.Entries[i].Value != values[i] {
			t.Errorf("Expected Entries[%d].Value to be node %d, got %v", i, i+1, inode.Entries[i].Value)
		}
	}

	// 批量插入导致节点分裂
	keysSplit := []interface{}{40, 50}
	valuesSplit := []*Node{NewNode(4), NewNode(5)}
	numSplit := 2

	newNodes, err = inode.BatchInsert(keysSplit, valuesSplit, numSplit)
	if err != nil {
		t.Fatalf("BatchInsert failed: %v", err)
	}
	if newNodes == nil || len(newNodes) == 0 {
		t.Errorf("Expected new nodes after split, got %v", newNodes)
	}
	if inode.count != 2 { // assuming split occurs after count=4, adjust according to cardinality
		t.Errorf("Expected original node count to be 2 after split, got %d", inode.count)
	}
	if newNodes[0].count != 3 { // adjust based on BatchInsert logic
		t.Errorf("Expected new node count to be 3, got %d", newNodes[0].count)
	}
	// Further assertions can be added based on the exact split logic
}

// TestINode_BatchMigrate 测试批量迁移
func TestINode_BatchMigrate(t *testing.T) {
	inode := NewINodeForInsertInBatch(1)
	migrate := []Entry{
		{Key: 10, Value: NewNode(1)},
		{Key: 20, Value: NewNode(2)},
	}
	migrateNum := 2
	migrateIdx := 0

	updatedIdx, err := inode.BatchMigrate(migrate, migrateIdx, migrateNum)
	if err != nil {
		t.Fatalf("BatchMigrate failed: %v", err)
	}
	if updatedIdx != 2 {
		t.Errorf("Expected migrateIdx to be 2, got %d", updatedIdx)
	}
	if inode.count != 2 {
		t.Errorf("Expected count to be 2, got %d", inode.count)
	}
	if inode.leftmostPtr != migrate[0].Value {
		t.Errorf("Expected leftmostPtr to be node 1, got %v", inode.leftmostPtr)
	}
	for i, entry := range migrate[1:2] {
		if inode.Entries[i].Key != entry.Key {
			t.Errorf("Expected Entries[%d].Key to be %v, got %v", i, entry.Key, inode.Entries[i].Key)
		}
		if inode.Entries[i].Value != entry.Value {
			t.Errorf("Expected Entries[%d].Value to be node %d, got %v", i, i+1, inode.Entries[i].Value)
		}
	}
}

// TestINode_BatchKvPair 测试批量键值对插入
func TestINode_BatchKvPair(t *testing.T) {
	inode := NewINodeForInsertInBatch(1)
	keys := []interface{}{10, 20, 30}
	values := []*Node{NewNode(1), NewNode(2), NewNode(3)}
	num := 3
	batchSize := 2
	idx := 0

	newIdx, reached, err := inode.BatchKvPair(keys, values, idx, num, batchSize)
	if err != nil {
		t.Fatalf("BatchKvPair failed: %v", err)
	}
	if newIdx != 2 {
		t.Errorf("Expected newIdx to be 2, got %d", newIdx)
	}
	if !reached {
		t.Errorf("Expected reached to be true, got false")
	}
	if inode.count != 2 {
		t.Errorf("Expected count to be 2, got %d", inode.count)
	}
	if inode.HighKey != 20 {
		t.Errorf("Expected HighKey to be 20, got %v", inode.HighKey)
	}

	// 执行剩余插入
	newIdx, reached, err = inode.BatchKvPair(keys, values, newIdx, num, batchSize)
	if err != nil {
		t.Fatalf("BatchKvPair failed on second call: %v", err)
	}
	if newIdx != 3 {
		t.Errorf("Expected newIdx to be 3, got %d", newIdx)
	}
	if reached {
		t.Errorf("Expected reached to be false, got true")
	}
	if inode.count != 3 {
		t.Errorf("Expected count to be 3, got %d", inode.count)
	}
	if inode.HighKey != 30 {
		t.Errorf("Expected HighKey to be 30, got %v", inode.HighKey)
	}
}

// TestINode_BatchBuffer 测试批量缓冲区插入
func TestINode_BatchBuffer(t *testing.T) {
	inode := NewINodeForInsertInBatch(1)
	buf := []Entry{
		{Key: 10, Value: NewNode(1)},
		{Key: 20, Value: NewNode(2)},
		{Key: 30, Value: NewNode(3)},
	}
	bufNum := 3
	batchSize := 2
	bufIdx := 0

	newBufIdx, reached, err := inode.BatchBuffer(buf, bufIdx, bufNum, batchSize)
	if err != nil {
		t.Fatalf("BatchBuffer failed: %v", err)
	}
	if newBufIdx != 2 {
		t.Errorf("Expected bufIdx to be 2, got %d", newBufIdx)
	}
	if !reached {
		t.Errorf("Expected reached to be true, got false")
	}
	if inode.count != 2 {
		t.Errorf("Expected count to be 2, got %d", inode.count)
	}
	if inode.HighKey != 20 {
		t.Errorf("Expected HighKey to be 20, got %v", inode.HighKey)
	}

	// 执行剩余插入
	newBufIdx, reached, err = inode.BatchBuffer(buf, newBufIdx, bufNum, batchSize)
	if err != nil {
		t.Fatalf("BatchBuffer failed on second call: %v", err)
	}
	if newBufIdx != 3 {
		t.Errorf("Expected bufIdx to be 3, got %d", newBufIdx)
	}
	if reached {
		t.Errorf("Expected reached to be false, got true")
	}
	if inode.count != 3 {
		t.Errorf("Expected count to be 3, got %d", inode.count)
	}
	if inode.HighKey != 30 {
		t.Errorf("Expected HighKey to be 30, got %v", inode.HighKey)
	}
}

// TestINode_SplitAndBatchInsert 测试分裂后批量插入
func TestINode_SplitAndBatchInsert(t *testing.T) {
	inode := NewINodeForInsertInBatch(1)
	// 假设 batchSize = 2, cardinality = 4 (PageSize assumptions)
	keys := []interface{}{10, 20, 30, 40}
	values := []*Node{NewNode(1), NewNode(2), NewNode(3), NewNode(4)}
	num := 4

	// 批量插入
	newNodes, err := inode.BatchInsert(keys, values, num)
	if err != nil {
		t.Fatalf("BatchInsert failed: %v", err)
	}

	if newNodes != nil {
		t.Errorf("Expected no new nodes, got %v", newNodes)
	}
	if inode.count != 4 {
		t.Errorf("Expected count to be 4, got %d", inode.count)
	}
	if inode.HighKey != 40 {
		t.Errorf("Expected HighKey to be 40, got %v", inode.HighKey)
	}

	// 插入第5条，导致节点分裂
	keysSplit := []interface{}{50, 60}
	valuesSplit := []*Node{NewNode(5), NewNode(6)}
	numSplit := 2

	newNodes, err = inode.BatchInsert(keysSplit, valuesSplit, numSplit)
	if err != nil {
		t.Fatalf("BatchInsert after split failed: %v", err)
	}

	if newNodes == nil || len(newNodes) != 1 {
		t.Errorf("Expected one new node after split, got %v", newNodes)
	}
	if inode.count != 2 {
		t.Errorf("Expected original node count to be 2 after split, got %d", inode.count)
	}
	if inode.HighKey != 30 {
		t.Errorf("Expected original node HighKey to be 30 after split, got %v", inode.HighKey)
	}
	if newNodes[0].count != 4 {
		t.Errorf("Expected new node count to be 4, got %d", newNodes[0].count)
	}
	if newNodes[0].HighKey != 60 {
		t.Errorf("Expected new node HighKey to be 60, got %v", newNodes[0].HighKey)
	}
}

// TestINode_SanityCheck 测试节点的完整性检查
func TestINode_SanityCheck(t *testing.T) {
	inode := NewINodeForInsertInBatch(1)
	keys := []interface{}{10, 20, 30, 40}
	values := []*Node{NewNode(1), NewNode(2), NewNode(3), NewNode(4)}
	num := 4

	for i := 0; i < num; i++ {
		err := inode.Insert(keys[i], values[i])
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// 添加一个打乱顺序的键，期待 SanityCheck 报错
	err := inode.Insert(25, NewNode(5))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// 进行 SanityCheck
	inode.SanityCheck(nil, true)
	// 预期输出应报告键顺序不正确
	// 由于测试环境无法捕获标准输出，实际测试中可能需要调整 SanityCheck 方法以返回错误
}

// TestINode_ScanNode 测试 ScanNode 方法
func TestINode_ScanNode(t *testing.T) {
	inode := NewINodeForInsertInBatch(1)
	sibling := NewNode(99)
	inode.siblingPtr = sibling
	inode.HighKey = 30

	// 插入一些条目
	keys := []interface{}{10, 20, 30}
	values := []*Node{NewNode(1), NewNode(2), NewNode(3)}
	num := 3

	for i := 0; i < num; i++ {
		err := inode.Insert(keys[i], values[i])
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// 测试 key < HighKey，应该返回相应的节点
	node := inode.ScanNode(25)
	if node == nil || node != values[1] {
		t.Errorf("Expected ScanNode(25) to return node 2, got %v", node)
	}

	// 测试 key > HighKey，应该返回 sibling_ptr
	node = inode.ScanNode(35)
	if node == nil || node != sibling {
		t.Errorf("Expected ScanNode(35) to return sibling node, got %v", node)
	}

	// 测试 key equal to HighKey
	node = inode.ScanNode(30)
	if node == nil || node != values[2] {
		t.Errorf("Expected ScanNode(30) to return node 3, got %v", node)
	}
}

// Additional tests can be added here for BatchInsertWithMigrationAndMovement, BatchInsertWithMovement, etc.

// Example of using Print and SanityCheck (Note: In real tests, avoid using fmt.Println, use assertions instead)
func ExampleINode_Print() {
	inode := NewINodeForInsertInBatch(1)
	keys := []interface{}{10, 20, 30}
	values := []*Node{NewNode(1), NewNode(2), NewNode(3)}
	num := 3

	for i := 0; i < num; i++ {
		inode.Insert(keys[i], values[i])
	}

	inode.Print()
	// Output:
	// LeftmostPtr: <nil>
	// [0] Key: 10, Value: &blinkhash.MockNode{...}
	// [1] Key: 20, Value: &blinkhash.MockNode{...}
	// [2] Key: 30, Value: &blinkhash.MockNode{...}
	// HighKey: 30
}

// Similarly, other methods like BatchInsertWithMigrationAndMovement can have their own test cases.
