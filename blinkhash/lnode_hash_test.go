package blinkhash

import "testing"

// TestLNodeHash_StabilizeBucket 测试 LNodeHash 的 StabilizeBucket 方法
func TestLNodeHash_StabilizeBucket(t *testing.T) {
	// 仅在启用了 LINKED 和 FINGERPRINT 时进行测试
	if !LINKED || !FINGERPRINT {
		t.Skip("Skipping StabilizeBucket test because LINKED or FINGERPRINT is disabled")
	}

	// 创建两个 LNodeHash 节点，模拟兄弟关系
	leftNode := &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       2,
		},
		Type:        HashNode,
		Cardinality: 5,
		HighKey:     10,
		Buckets:     make([]Bucket, 5),
	}

	rightNode := &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       2,
		},
		Type:        HashNode,
		Cardinality: 5,
		HighKey:     20,
		Buckets:     make([]Bucket, 5),
	}

	// 设置兄弟关系
	leftNode.siblingPtr = rightNode
	rightNode.LeftSiblingPtr = leftNode

	// 初始化桶的指纹和条目
	for i := 0; i < leftNode.Cardinality; i++ {
		leftNode.Buckets[i].fingerprints = make([]uint8, 10)
		leftNode.Buckets[i].entries = make([]Entry, 10)
		rightNode.Buckets[i].fingerprints = make([]uint8, 10)
		rightNode.Buckets[i].entries = make([]Entry, 10)
	}

	// 设置左节点的某个桶为 LINKED_LEFT
	loc := 2
	leftNode.Buckets[loc].state = LINKED_LEFT

	// 设置左节点桶中的条目
	leftNode.Buckets[loc].fingerprints[0] = 1
	leftNode.Buckets[loc].entries[0] = Entry{Key: 5, Value: "leftValue1"}

	// 设置右节点的相应桶为 LINKED_RIGHT
	rightNode.Buckets[loc].state = LINKED_RIGHT

	// 设置右节点桶中的条目
	rightNode.Buckets[loc].fingerprints[1] = 1
	rightNode.Buckets[loc].entries[1] = Entry{Key: 15, Value: "rightValue1"}

	// 创建一个当前节点，设置其高键和指针
	currentNode := &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  rightNode,
			leftmostPtr: leftNode,
			count:       1,
			level:       2,
		},
		Type:        HashNode,
		Cardinality: 5,
		HighKey:     12,
		Buckets:     make([]Bucket, 5),
	}

	// 初始化当前节点的桶
	for i := 0; i < currentNode.Cardinality; i++ {
		currentNode.Buckets[i].fingerprints = make([]uint8, 10)
		currentNode.Buckets[i].entries = make([]Entry, 10)
	}

	// 设置当前节点桶的状态为 LINKED_LEFT
	currentNode.Buckets[loc].state = LINKED_LEFT

	// 运行 StabilizeBucket
	success := currentNode.StabilizeBucket(loc)
	if !success {
		t.Errorf("Expected StabilizeBucket to succeed, but it failed")
	}

	// 验证数据迁移
	if currentNode.Buckets[loc].entries[0].Key != 5 || currentNode.Buckets[loc].entries[0].Value != "leftValue1" {
		t.Errorf("Data migration failed for current node's bucket")
	}

	if leftNode.Buckets[loc].fingerprints[0] != 0 || leftNode.Buckets[loc].entries[0].Key != nil {
		t.Errorf("Data was not cleared from left node's bucket after migration")
	}

	// 验证状态更新
	if currentNode.Buckets[loc].state != STABLE || leftNode.Buckets[loc].state != STABLE {
		t.Errorf("Bucket states were not updated to STABLE after migration")
	}
}
