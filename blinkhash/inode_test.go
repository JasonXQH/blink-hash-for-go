package blinkhash

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestINode_Split(t *testing.T) {
	inode := &INode{
		Node: Node{
			level: 1,
			count: 4,
		},
		Entries: []Entry{
			{Key: 1, Value: &Node{}},
			{Key: 3, Value: &Node{}},
			{Key: 5, Value: &Node{}},
			{Key: 7, Value: &Node{}},
		},
	}

	fmt.Println("Before Insert:", inode.Entries)
	inode.Insert(4, &Node{}, &Node{})
	fmt.Println("After Insert:", inode.Entries)

	var splitKey interface{}
	newNode, splitKey := inode.Split()
	fmt.Println("Split Key:", splitKey)
	fmt.Println("Current Node:", inode.Entries)
	fmt.Println("New Node:", newNode.Entries)
}

func TestBatchMigrate_UpdatesLeftmostPtrCorrectly(t *testing.T) {
	// Setup initial INode and Entries
	initialNode := Node{}
	entryNode := Node{}
	entries := []Entry{
		{Key: 1, Value: entryNode},
		{Key: 2, Value: Node{}},
	}
	// Create an INode instance
	inode := INode{
		Node:    initialNode,
		Entries: make([]Entry, 0),
	}
	// Set migrateIdx to 0
	migrateIdx := 0
	// Perform BatchMigrate
	inode.BatchMigrate(entries, &migrateIdx, len(entries))
	// Assert that leftmostPtr is updated correctly
	assert.Equal(t, entryNode, inode.leftmostPtr, "leftmostPtr should be updated to the first entry's value")
}

func TestINode_BatchKvPair(t *testing.T) {
	// 示例键值对
	keys := []interface{}{"key1", "key2", "key3", "key4"}
	values := []*Node{&Node{}, &Node{}, &Node{}, &Node{}}
	idx := 0
	num := len(keys)
	batchSize := 2

	// 创建一个 INode
	inode := NewINode(1, nil, nil, nil)

	// 批量填充键值对
	for {
		shouldSplit := inode.BatchKvPair(keys, values, &idx, num, batchSize)
		fmt.Printf("INode after batch: %+v\n", inode)

		if shouldSplit {
			fmt.Println("Reached batch size, splitting...")
			// 假设新节点处理逻辑
			inode = NewINode(1, nil, nil, nil)
		}

		if idx >= num {
			break
		}
	}
}

func TestBatchInsertLastLevelWithBuffer(t *testing.T) {
	// 创建一个 INode
	inode := &INode{
		Node: Node{
			level:       1,
			count:       0,
			leftmostPtr: nil,
		},
		Entries: make([]Entry, 0),
	}

	// 示例数据
	migrate := []Entry{
		{Key: 1, Value: &Node{}},
		{Key: 2, Value: &Node{}},
	}
	migrateIdx := 0
	migrateNum := len(migrate)

	keys := []interface{}{3, 4, 5}
	values := []*Node{&Node{}, &Node{}, &Node{}}
	idx := 0
	num := len(keys)
	batchSize := 4

	buf := []Entry{
		{Key: 6, Value: &Node{}},
		{Key: 7, Value: &Node{}},
	}
	bufIdx := 0
	bufNum := len(buf)

	// 执行批量插入
	inode.BatchInsertLastLevelWithMigrationAndMovement(migrate, &migrateIdx, migrateNum, keys, values, &idx, num, batchSize, buf, &bufIdx, bufNum)

	// 输出结果
	fmt.Println("Entries:", inode.Entries)
	fmt.Println("HighKey:", inode.HighKey)
	fmt.Println("LeftmostPtr:", inode.leftmostPtr)
}
func TestINode_BatchInsertLastLevelWithMovement(t *testing.T) {
	// 创建一个 INode
	inode := &INode{
		Node: Node{
			level:       1,
			count:       0,
			leftmostPtr: nil,
		},
		Entries: make([]Entry, 0),
	}

	// 示例数据
	keys := []interface{}{3, 4, 5}
	values := []*Node{&Node{}, &Node{}, &Node{}}
	idx := 0
	num := len(keys)
	batchSize := 4

	buf := []Entry{
		{Key: 6, Value: &Node{}},
		{Key: 7, Value: &Node{}},
	}
	bufIdx := 0
	bufNum := len(buf)

	// 执行批量插入
	inode.BatchInsertLastLevelWithMovement(keys, values, &idx, num, batchSize, buf, &bufIdx, bufNum)

	// 输出结果
	fmt.Println("Entries:", inode.Entries)
	fmt.Println("HighKey:", inode.HighKey)
	fmt.Println("LeftmostPtr:", inode.leftmostPtr)
}

func TestINode_CalculateNodeNum(t *testing.T) {
	var totalNum = 120
	var numerator = 10
	var remains = 5
	var lastChunk, newNum int
	batchSize := 10

	inode := &INode{
		Cardinality: 15,
	}
	inode.CalculateNodeNum(totalNum, &numerator, &remains, &lastChunk, &newNum, batchSize)

	fmt.Println("Numerator:", numerator)
	fmt.Println("Remains:", remains)
	fmt.Println("LastChunk:", lastChunk)
	fmt.Println("NewNum:", newNum)
}

func TestINode_BatchInsertWithMigration(t *testing.T) {
	inode := &INode{
		Entries: make([]Entry, 0),
		Node: Node{
			level: 1,
			count: 0,
		},
	}

	migrate := []Entry{
		{Key: 1, Value: &Node{}},
		{Key: 2, Value: &Node{}},
	}
	migrateIdx := 0
	migrateNum := len(migrate)

	keys := []interface{}{3, 4, 5}
	values := []*Node{{}, {}, {}}
	idx := 0
	num := len(keys)

	batchSize := 4

	buf := []Entry{
		{Key: 6, Value: &Node{}},
		{Key: 7, Value: &Node{}},
	}
	bufIdx := 0
	bufNum := len(buf)

	// 调用 BatchInsertWithMigration
	inode.BatchInsertWithMigrationAndMoveMent(
		migrate, &migrateIdx, migrateNum,
		keys, values, &idx, num,
		batchSize, buf, &bufIdx, bufNum,
	)

	// 输出结果
	fmt.Printf("Entries: %+v\n", inode.Entries)
	fmt.Printf("Count: %d\n", inode.count)
	fmt.Printf("HighKey: %+v\n", inode.HighKey)
}

func TestINode_BatchInsertLastLevel(t *testing.T) {
	keys := []interface{}{10, 20, 30}
	values := make([]*Node, 3)
	for i := range values {
		values[i] = &Node{level: 1}
	}

	inode := NewINode(1, nil, nil, nil)
	inode.count = 0 // Starting with an empty node for simplicity

	// Assuming an FILL_FACTOR of 0.9 for this example and a high enough cardinality
	newNum := new(int)
	expectedNum := 0 // No new nodes should be created
	*newNum = expectedNum

	resultNodes, err := inode.BatchInsertLastLevel(keys, values, len(keys), newNum)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(resultNodes) != expectedNum {
		t.Errorf("Expected %d new nodes, got %d", expectedNum, len(resultNodes))
	}
	if inode.count != len(keys) {
		t.Errorf("Expected count %d, got %d", len(keys), inode.count)
	}

	// Verify keys and values
	for i, key := range keys {
		if inode.Entries[i].Key != key || inode.Entries[i].Value != values[i] {
			t.Errorf("Entry %d did not match expected. Got key %v and value %v", i, inode.Entries[i].Key, inode.Entries[i].Value)
		}
	}
}
