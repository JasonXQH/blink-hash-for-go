package blinkhash

import (
	"fmt"
	"testing"
)

// 测试函数：测试 Insert 和 Split
func TestLNodeBTree_Insert(t *testing.T) {
	// 创建一个 LNodeBTree 节点，层级为 2
	lnBTree := NewLNodeBTreeWithLevel(2)
	lnBTree.lock = 12345
	lnBTree.Cardinality = 5
	lnBTree.count = 3
	lnBTree.HighKey = 10
	lnBTree.Entries = append(lnBTree.Entries, Entry{Key: 1, Value: "value1"})
	lnBTree.Entries = append(lnBTree.Entries, Entry{Key: 3, Value: "value3"})
	lnBTree.Entries = append(lnBTree.Entries, Entry{Key: 5, Value: "value5"})

	// 创建另一个 LNodeBTree 节点，层级为 2
	lnBTree2 := NewLNodeBTreeWithLevel(2)
	lnBTree2.lock = 12346
	lnBTree.Cardinality = 5
	lnBTree2.count = 3
	lnBTree2.HighKey = 12
	lnBTree2.Entries = append(lnBTree2.Entries, Entry{Key: 7, Value: "value7"})
	lnBTree2.Entries = append(lnBTree2.Entries, Entry{Key: 8, Value: "value8"})
	lnBTree2.Entries = append(lnBTree2.Entries, Entry{Key: 9, Value: "value9"})

	// 执行插入操作：插入键值对 (4, "value4")
	result := lnBTree.Insert(4, "value4", 12345)
	if result != INSERT_SUCCESS {
		t.Errorf("Expected Insert to return INSERT_SUCCESS, got %s", getStatusName(result))
	}

	// 打印节点信息以验证插入结果
	fmt.Println("After first insert:")
	lnBTree.Print()

	// 继续插入直到需要分裂：插入键值对 (6, "value6")
	result = lnBTree.Insert(6, "value6", 12349)
	result = lnBTree.Insert(7, "value7", 12353)
	if result != NEED_SPLIT { // 期望插入 6 后需要分裂，返回值为 1
		t.Errorf("Expected Insert to return  NEED_SPLIT, got %s ", getStatusName(result))
	}

	// 执行分裂操作，插入键值对 (10, "value6")
	newLeaf, splitKey := lnBTree.Split(6, "value6", 12353)

	// 验证分裂结果
	if newLeaf == nil {
		t.Errorf("Expected Split to return new leaf, got nil")
	}
	expectedSplitKey := 3
	if splitKey != expectedSplitKey {
		t.Errorf("Expected splitKey to be %d, got %v", expectedSplitKey, splitKey)
	}

	// 打印分裂后的节点信息
	fmt.Println("\nAfter split:")
	lnBTree.Print()
	if newBtreeNodeLeaf, ok := newLeaf.(*LNodeBTree); ok {
		newBtreeNodeLeaf.Print()
	}
}
