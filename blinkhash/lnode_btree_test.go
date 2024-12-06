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
	if result != InsertSuccess {
		t.Errorf("Expected Insert to return INSERT_SUCCESS, got %s", getStatusName(result))
	}

	// 打印节点信息以验证插入结果
	fmt.Println("After first insert:")
	lnBTree.Print()

	// 继续插入直到需要分裂：插入键值对 (6, "value6")
	result = lnBTree.Insert(6, "value6", 12349)
	result = lnBTree.Insert(7, "value7", 12353)
	if result != NeedSplit { // 期望插入 6 后需要分裂，返回值为 1
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

// 测试函数：测试 Insert、Split、Update、Remove、Find、RangeLookUp 和 Utilization
func TestLNodeBTree_AllMethods(t *testing.T) {
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
	lnBTree2.Cardinality = 5
	lnBTree2.count = 3
	lnBTree2.HighKey = 12
	lnBTree2.Entries = append(lnBTree2.Entries, Entry{Key: 7, Value: "value7"})
	lnBTree2.Entries = append(lnBTree2.Entries, Entry{Key: 8, Value: "value8"})
	lnBTree2.Entries = append(lnBTree2.Entries, Entry{Key: 9, Value: "value9"})

	// 创建一个 LNodeHash 节点，设置兄弟节点为 nil，计数为 3，层级为 2
	lnHash := NewLNodeHashWithSibling(nil, 3, 2)
	lnHash.lock = 54321
	lnHash.HighKey = 15
	// 初始化 Buckets（根据实际需求进行初始化，这里仅添加示例数据）
	lnHash.Buckets = append(lnHash.Buckets, Bucket{
		lock:         1,
		state:        1,
		fingerprints: []uint8{0x1},
		entries:      []Entry{{Key: "a", Value: "A"}},
	})
	lnHash.Buckets = append(lnHash.Buckets, Bucket{
		lock:         2,
		state:        2,
		fingerprints: []uint8{0x2},
		entries:      []Entry{{Key: "b", Value: "B"}},
	})
	lnHash.Buckets = append(lnHash.Buckets, Bucket{
		lock:         3,
		state:        3,
		fingerprints: []uint8{0x3},
		entries:      []Entry{{Key: "c", Value: "C"}},
	})
	lnHash.count = 3

	// 设置兄弟节点指针
	lnBTree.siblingPtr = lnBTree2
	lnBTree2.siblingPtr = lnHash

	// 创建 Tree 并设置根节点
	//tree := &Tree{root: lnBTree}

	// 子测试：Insert 和 Split
	t.Run("InsertAndSplit", func(t *testing.T) {
		// 执行插入操作：插入键值对 (4, "value4")
		result := lnBTree.Insert(4, "value4", 12345)
		if result != InsertSuccess {
			t.Errorf("Expected Insert to return InsertSuccess, got %s", getStatusName(result))
		}

		// 打印节点信息以验证插入结果
		fmt.Println("After first insert:")
		lnBTree.Print()

		// 执行插入操作，导致需要分裂：插入键值对 (6, "value6")
		result = lnBTree.Insert(6, "value6", 12349)
		result = lnBTree.Insert(7, "value7", 12353)
		if result != NeedSplit {
			t.Errorf("Expected Insert to return NeedSplit, got %s", getStatusName(result))
		}

		// 执行分裂操作，插入键值对 (10, "value10")
		newLeaf, splitKey := lnBTree.Split(10, "value10", 12353)

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
		} else {
			t.Errorf("Expected newLeaf to be of type *LNodeBTree, got %T", newLeaf)
		}
	})

	// 子测试：Update
	t.Run("Update", func(t *testing.T) {
		// 更新一个存在的键：键 3 -> "value3_updated"
		updateResult := lnBTree.Update(3, "value3_updated", 12346)
		if updateResult != UpdateSuccess {
			t.Errorf("Expected Update to return UpdateSuccess, got %s", getStatusName(updateResult))
		}

		// 验证更新结果
		value, found := lnBTree.Find(3)
		if !found {
			t.Errorf("Expected to find key 3 after update, but it was not found")
		} else if value != "value3_updated" {
			t.Errorf("Expected value for key 3 to be 'value3_updated', got '%v'", value)
		}

		// 更新一个不存在的键：键 100 -> "value100"
		updateResult = lnBTree.Update(100, "value100", 12347)
		if updateResult != UpdateFailure {
			t.Errorf("Expected Update to return UpdateFailure for non-existing key, got %s", getStatusName(updateResult))
		}
	})

	// 子测试：Remove
	t.Run("Remove", func(t *testing.T) {
		// 删除一个存在的键：键 1
		removeResult := lnBTree.Remove(1, 12348)
		if removeResult != RemoveSuccess {
			t.Errorf("Expected Remove to return RemoveSuccess, got %s", getStatusName(removeResult))
		}

		// 验证删除结果
		_, found := lnBTree.Find(1)
		if found {
			t.Errorf("Expected key 1 to be removed, but it was found")
		}

		// 删除一个不存在的键：键 100
		removeResult = lnBTree.Remove(100, 12349)
		if removeResult != KeyNotFound {
			t.Errorf("Expected Remove to return KeyNotFound for non-existing key, got %s", getStatusName(removeResult))
		}
	})

	// 子测试：Find
	t.Run("Find", func(t *testing.T) {
		// 查找一个存在的键：键 3
		value, found := lnBTree.Find(3)
		lnBTree.Insert(1, "value1", 12365)
		lnBTree.Insert(4, "value4", 12369)
		lnBTree.Insert(5, "value5", 12373)
		if !found {
			t.Errorf("Expected to find key 3, but it was not found")
		} else if value != "value3_updated" {
			t.Errorf("Expected value for key 3 to be 'value3_updated', got '%v'", value)
		}

		// 查找一个不存在的键：键 100
		value, found = lnBTree.Find(100)
		if found {
			t.Errorf("Expected not to find key 100, but it was found with value '%v'", value)
		}
	})

	// 子测试：RangeLookUp
	t.Run("RangeLookUp", func(t *testing.T) {
		var buffer []interface{}
		// 非连续查找：从键 3 开始，获取 2 个值
		rangeResult := lnBTree.RangeLookUp(3, &buffer, 0, 2, false)
		if rangeResult != 2 {
			t.Errorf("Expected RangeLookUp to return 2, got %d", rangeResult)
		}
		expectedValues := []interface{}{"value4", "value5"}
		for i, val := range buffer {
			if val != expectedValues[i] {
				t.Errorf("Expected buffer[%d] to be '%v', got '%v'", i, expectedValues[i], val)
			}
		}

		// 连续查找：获取前 2 个值
		buffer = []interface{}{}
		rangeResult = lnBTree.RangeLookUp(0, &buffer, 0, 2, true)
		if rangeResult != 2 {
			t.Errorf("Expected RangeLookUp to return 2, got %d", rangeResult)
		}
		expectedValues = []interface{}{"value1", "value3_updated"}
		for i, val := range buffer {
			if val != expectedValues[i] {
				t.Errorf("Expected buffer[%d] to be '%v', got '%v'", i, expectedValues[i], val)
			}
		}

		// 超出范围查找：请求 10 个值，但只有 2 个
		buffer = []interface{}{}
		rangeResult = lnBTree.RangeLookUp(3, &buffer, 0, 10, false)
		if rangeResult != 2 {
			t.Errorf("Expected RangeLookUp to return 2 (available), got %d", rangeResult)
		}
		expectedValues = []interface{}{"value4", "value5"}
		for i, val := range buffer {
			if val != expectedValues[i] {
				t.Errorf("Expected buffer[%d] to be '%v', got '%v'", i, expectedValues[i], val)
			}
		}
	})

	// 子测试：Utilization
	t.Run("Utilization", func(t *testing.T) {
		utilization := lnBTree.Utilization()
		expectedUtilization := float64(4) / float64(5) // 目前有 2 个条目（3 和 4）
		if utilization != expectedUtilization {
			t.Errorf("Expected Utilization to be %f, got %f", expectedUtilization, utilization)
		}
	})

	// 子测试：SanityCheck
	t.Run("SanityCheck", func(t *testing.T) {
		// 由于 SanityCheck 方法会递归检查所有节点，我们可以简单调用它来确保不会 panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("SanityCheck panicked: %v", r)
			}
		}()
		lnBTree.SanityCheck(nil, true)
		lnBTree2.SanityCheck(lnBTree.HighKey, false)
		lnHash.SanityCheck(lnBTree2.HighKey, false)
	})
}
