package blinkhash

import (
	"fmt"
	"testing"
)

// TestLNodeHash_Insert 测试 LNodeHash 的 Insert 方法
func TestLNodeHash_Insert(t *testing.T) {
	// 创建一个 LNodeHash 节点，层级为 2
	lnHash := &LNodeHash{
		Node: Node{
			lock:        12348,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       2,
		},
		Type:        HashNode,
		Cardinality: 5,                 // 示例值，根据实际需求调整
		HighKey:     10,                // 示例高键
		Buckets:     make([]Bucket, 5), // 创建 5 个桶
	}

	// 初始化每个桶的指纹和条目
	for i := 0; i < lnHash.Cardinality; i++ {
		lnHash.Buckets[i].fingerprints = make([]uint8, 10) // 假设每个桶有 10 个条目
		lnHash.Buckets[i].entries = make([]Entry, 10)
		lnHash.Buckets[i].lock = 12348
		// 初始化条目为 nil
		for j := 0; j < 10; j++ {
			lnHash.Buckets[i].entries[j].Key = nil
			lnHash.Buckets[i].entries[j].Value = nil
		}
	}

	// 执行插入操作：插入键值对 (1, "value1")
	result := lnHash.Insert(1, "value1", 12348)
	if result != InsertSuccess {
		t.Errorf("Expected Insert to return InsertSuccess, got %d", result)
	}

	// 验证插入结果
	found := false
	for _, bucket := range lnHash.Buckets {
		for _, entry := range bucket.entries {
			if entry.Key == 1 && entry.Value == "value1" {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Errorf("Failed to insert (1, \"value1\")")
	}

	// 执行插入操作：插入键值对 (2, "value2")

	result = lnHash.Insert(2, "value2", 12348)
	if result != InsertSuccess {
		t.Errorf("Expected Insert to return InsertSuccess, got %d", result)
	}

	// 验证插入结果
	found = false
	for _, bucket := range lnHash.Buckets {
		for _, entry := range bucket.entries {
			if entry.Key == 2 && entry.Value == "value2" {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Errorf("Failed to insert (2, \"value2\")")
	}
	//
	//// 执行插入操作，直到需要分裂：插入多个键值对
	//keys := []int{3, 4, 5, 6}
	//values := []string{"value3", "value4", "value5", "value6"}
	//for i, key := range keys {
	//	result = lnHash.Insert(key, values[i], 12347+uint64(i))
	//	// 根据当前桶的容量，调整预期结果
	//	if lnHash.count < lnHash.Cardinality*10 { // 假设每个桶有 10 个槽位
	//		if result != InsertSuccess {
	//			t.Errorf("Expected Insert to return InsertSuccess, got %d", result)
	//		}
	//	} else {
	//		if result != NeedSplit {
	//			t.Errorf("Expected Insert to return NeedSplit, got %d", result)
	//		}
	//	}
	//}

	// 打印桶信息以验证插入结果
	fmt.Println("After inserts:")
	lnHash.Print()
}

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

// TestSplitWithoutFingerprintAndLinked
// 测试在 FINGERPRINT = false 和 LINKED = false 下执行 Split 函数
func TestSplitWithoutFingerprintAndLinked(t *testing.T) {
	if LINKED || FINGERPRINT {
		panic("此测试仅可在LINKED和FINGERPRINT均为FALSE的情况下进行")
	}
	// 构造一个需要split的LNodeHash节点
	lnHash := &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       4,
			level:       2,
		},
		Type:        HashNode,
		Cardinality: 4, // 根据需要的大小调整
		HighKey:     50,
		Buckets:     make([]Bucket, 4),
	}

	// 初始化Bucket
	for i := 0; i < lnHash.Cardinality; i++ {
		lnHash.Buckets[i].entries = make([]Entry, EntryNum)
		for j := 0; j < EntryNum; j++ {
			lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
		}
	}

	// 向节点中插入足够多的键值对来引发Split
	// 假设EntryNum较小，插入若干条数据
	insertCount := lnHash.Cardinality * EntryNum
	for i := 0; i < insertCount; i++ {
		// 假设Insert不需要fingerprint逻辑
		_ = lnHash.Insert(i, fmt.Sprintf("value%d", i), 0)
	}

	// 现在执行Split
	newNode, splitKey := lnHash.Split(insertCount+1, fmt.Sprintf("value%d", insertCount+1), lnHash.lock)
	if newNode == nil {
		t.Errorf("Expected split to succeed, got nil")
	}

	if splitKey == nil {
		t.Errorf("Expected a splitKey to be set, got nil")
	}

	// 检查分裂结果
	if lnHash.siblingPtr != newNode.(*LNodeHash) {
		t.Errorf("Expected newNode to be right sibling of lnHash")
	}

	// 检查新插入的key
	found := false
	if newHashNode, ok := newNode.(*LNodeHash); ok {
		if newHashNode.LeftSiblingPtr != lnHash {
			t.Errorf("Expected lnHash to be left sibling of newNode")
		}
		for _, bucket := range newHashNode.Buckets {
			for _, entry := range bucket.entries {
				if entry.Key == insertCount+1 && entry.Value == fmt.Sprintf("value%d", insertCount+1) {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
	}

	if !found {
		t.Errorf("Expected to find key %d in newNode, but not found", insertCount+1)
	}
}

// TestSplitWithFingerprintAndLinked
// 测试在 FINGERPRINT = true 和 LINKED = true 下执行 Split 函数
func TestSplitWithFingerprintAndLinked(t *testing.T) {
	if !LINKED || !FINGERPRINT {
		panic("此测试仅可以在LINKED和FINGERPRINT均为TRUE的情况下进行")
	}

	// 构造一个需要split的LNodeHash节点
	lnHash := &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       2,
		},
		Type:        HashNode,
		Cardinality: 4, // 根据需要的大小调整
		HighKey:     100,
		Buckets:     make([]Bucket, 4),
	}

	// 初始化Bucket（有fingerprint数组）
	for i := 0; i < lnHash.Cardinality; i++ {
		lnHash.Buckets[i].fingerprints = make([]uint8, EntryNum)
		lnHash.Buckets[i].entries = make([]Entry, EntryNum)
		for j := 0; j < EntryNum; j++ {
			lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
			lnHash.Buckets[i].fingerprints[j] = EmptyFingerprint
		}
	}

	// 插入数据以引发Split，同时模拟fingerprint插入
	insertCount := lnHash.Cardinality * EntryNum
	for i := 0; i < insertCount; i++ {
		result := lnHash.Insert(i, fmt.Sprintf("val%d", i), 0)
		if result != InsertSuccess && result != NeedSplit {
			t.Errorf("Insert returned unexpected result: %d", result)
		}
	}

	// 现在执行Split，检查在有FINGERPRINT和LINKED的逻辑下是否正确分裂
	newNode, splitKey := lnHash.Split(insertCount+10, "splitValue", lnHash.lock) // 尝试插入一个比高键大的值
	if newNode == nil {
		t.Errorf("Expected split to succeed under FINGERPRINT && LINKED, got nil")
	}

	if splitKey == nil {
		t.Errorf("Expected a splitKey to be set, got nil")
	}

	// 检查分裂结果和fingerprint
	if lnHash.siblingPtr != newNode.(*LNodeHash) {
		t.Errorf("Expected newNode to be right sibling of lnHash")
	}
	if newHashNode, ok := newNode.(*LNodeHash); ok {
		if newHashNode.LeftSiblingPtr != lnHash {
			t.Errorf("Expected lnHash to be left sibling of newNode")
		}
		// 验证新插入的key的fingerprint是否正确插入到右节点（如果它比splitKey大）
		found := false
		for _, bucket := range newHashNode.Buckets {
			for i, entry := range bucket.entries {
				if entry.Key == insertCount+10 && entry.Value == "splitValue" && bucket.fingerprints[i] != EmptyFingerprint {
					found = true
					break
				}
			}
			if found {
				break
			}
		}

		if !found {
			t.Errorf("Expected to find key %d in newNode with a valid fingerprint, but not found", insertCount+10)
		}
	}
}

// TestLNodeHash_Update 测试 LNodeHash 的 Update 方法
func TestLNodeHash_Update(t *testing.T) {
	// 创建一个 LNodeHash 节点
	// 对于测试 Update，这里不要求特定的LINKED、FINGERPRINT配置，
	// 可以先测试在 LINKED = false, FINGERPRINT = false 的情况下。
	if LINKED || FINGERPRINT {
		t.Skip("Skipping Update test because we want to test it under LINKED = false and FINGERPRINT = false")
	}

	lnHash := &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       2,
		},
		Type:        HashNode,
		Cardinality: 4,
		HighKey:     50,
		Buckets:     make([]Bucket, 4),
	}

	// 初始化Bucket
	for i := 0; i < lnHash.Cardinality; i++ {
		lnHash.Buckets[i].entries = make([]Entry, EntryNum)
		for j := 0; j < EntryNum; j++ {
			lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
		}
	}

	// 插入一些键值对
	keys := []int{10, 20, 30, 40}
	values := []string{"val10", "val20", "val30", "val40"}
	version := lnHash.lock // 假设lock代表版本或通过GetVersion()获得初始版本

	for i, k := range keys {
		result := lnHash.Insert(k, values[i], version)
		if result != InsertSuccess {
			t.Errorf("Expected Insert to return InsertSuccess for key %d, got %d", k, result)
		}
	}

	// 现在更新其中一些键的值
	updateKeys := []int{20, 40}
	newValues := []string{"val20_new", "val40_new"}

	for i, uk := range updateKeys {
		ret := lnHash.Update(uk, newValues[i], version)
		if ret != 0 {
			t.Errorf("Expected Update to return 0 (success) for key %d, got %d", uk, ret)
		}
	}

	// 验证更新是否生效
	for i, uk := range updateKeys {
		found := false
		var foundVal interface{}
		for _, bucket := range lnHash.Buckets {
			for _, entry := range bucket.entries {
				if entry.Key == uk {
					found = true
					foundVal = entry.Value
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			t.Errorf("Expected to find updated key %d, but not found", uk)
		} else {
			if foundVal != newValues[i] {
				t.Errorf("Expected value for key %d to be %s, got %v", uk, newValues[i], foundVal)
			}
		}
	}

	// 测试一个不存在的key更新应返回1（表示未找到）
	nonExistKey := 999
	ret := lnHash.Update(nonExistKey, "someValue", version)
	if ret != UpdateFailure {
		t.Errorf("Expected Update to return UpdateFailure for non-existent key %d, got %d", nonExistKey, ret)
	}

	// 测试在版本不匹配或需要重启的情况（这里可能需要模拟GetVersion返回的needRestart为true来测试-1返回值）
	// 如果GetVersion逻辑不易修改，可暂时跳过此部分。
	// 如果实现GetVersion可控逻辑，则在此模拟needRestart为true，期望Update返回-1.
	// 例如：
	// mockNeedRestart = true
	// ret = lnHash.Update(10, "newValShouldFail", version)
	// if ret != -1 {
	//     t.Errorf("Expected Update to return -1 when needRestart is true, got %d", ret)
	// }
	// mockNeedRestart = false

	fmt.Println("TestLNodeHash_Update completed")
}

func TestLNodeHash_Remove(t *testing.T) {
	// 对Remove进行测试，可以在LINKED和FINGERPRINT为false的基础上测试基础功能
	if LINKED || FINGERPRINT {
		t.Skip("Skipping Remove test because we want to test under LINKED = false and FINGERPRINT = false")
	}

	lnHash := &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       2,
		},
		Type:        HashNode,
		Cardinality: 4,
		HighKey:     50,
		Buckets:     make([]Bucket, 4),
	}

	// 初始化Bucket
	for i := 0; i < lnHash.Cardinality; i++ {
		lnHash.Buckets[i].entries = make([]Entry, EntryNum)
		for j := 0; j < EntryNum; j++ {
			lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
		}
	}

	version := lnHash.lock // 假设lock代表当前版本号，或使用lnHash.GetVersion()获取初始版本

	// 插入一些数据
	keys := []int{10, 20, 30, 40, 50}
	values := []string{"v10", "v20", "v30", "v40", "v50"}

	for i, k := range keys {
		res := lnHash.Insert(k, values[i], version)
		if res != InsertSuccess {
			t.Errorf("Insert failed for key %d, expected InsertSuccess got %d", k, res)
		}
	}

	// 删除已存在的key
	delKey := 30
	ret := lnHash.Remove(delKey, version)
	if ret != 0 {
		t.Errorf("Expected Remove to return 0 (success) for key %d, got %d", delKey, ret)
	}

	// 检查key是否已被删除
	found := false
	for _, bucket := range lnHash.Buckets {
		for _, entry := range bucket.entries {
			if entry.Key == delKey {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if found {
		t.Errorf("Key %d was not removed successfully", delKey)
	}

	// 删除不存在的key，应返回1
	nonExistKey := 999
	ret = lnHash.Remove(nonExistKey, version)
	if ret != 1 {
		t.Errorf("Expected Remove to return 1 (key not found) for non-existent key %d, got %d", nonExistKey, ret)
	}

	// 测试需要重启（needRestart）情况:
	// 若您的实现中GetVersion或TryLock可以模拟重启或版本不匹配情况，可以进行此类测试。
	// 例如:
	// mockNeedRestart = true
	// ret = lnHash.Remove(10, version)
	// if ret != -1 {
	//     t.Errorf("Expected Remove to return -1 when needRestart is true, got %d", ret)
	// }
	// mockNeedRestart = false

	// 删除边界值key，如最小或最大
	delKey = 10
	ret = lnHash.Remove(delKey, version)
	if ret != 0 {
		t.Errorf("Expected Remove to return 0 (success) for key %d, got %d", delKey, ret)
	}

	fmt.Println("TestLNodeHash_Remove completed")
}

// TestLNodeHash_Find 测试 LNodeHash 的 Find 方法
func TestLNodeHash_Find(t *testing.T) {
	// 子测试1：LINKED = false, FINGERPRINT = false
	if LINKED || FINGERPRINT {
		t.Skip("Skipping Remove test because we want to test under LINKED = false and FINGERPRINT = false")
	}

	t.Run("Find without LINKED and FINGERPRINT", func(t *testing.T) {
		// 设置全局标志
		// 构造一个 LNodeHash 节点
		lnHash := &LNodeHash{
			Node: Node{
				lock:        0,
				siblingPtr:  nil,
				leftmostPtr: nil,
				count:       0,
				level:       2,
			},
			Type:        HashNode,
			Cardinality: 4,
			HighKey:     50,
			Buckets:     make([]Bucket, 4),
		}

		// 初始化Bucket
		for i := 0; i < lnHash.Cardinality; i++ {
			lnHash.Buckets[i].entries = make([]Entry, EntryNum)
			for j := 0; j < EntryNum; j++ {
				lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
			}
		}

		// 插入一些数据
		keys := []int{10, 20, 30, 40}
		values := []string{"v10", "v20", "v30", "v40"}
		version := lnHash.lock // 假设lock代表当前版本号，或使用lnHash.GetVersion()获取初始版本

		for i, k := range keys {
			res := lnHash.Insert(k, values[i], version)
			if res != InsertSuccess {
				t.Errorf("Insert failed for key %d, expected InsertSuccess got %d", k, res)
			}
		}

		// 测试查找已存在的键
		for i, k := range keys {
			val := lnHash.Find(k)
			if val == nil {
				t.Errorf("Expected to find key %d, but got nil", k)
			} else if val != values[i] {
				t.Errorf("Expected value %s for key %d, got %v", values[i], k, val)
			}
		}

		// 测试查找不存在的键
		nonExistKey := 999
		val := lnHash.Find(nonExistKey)
		if val != nil {
			t.Errorf("Expected nil for non-existent key %d, got %v", nonExistKey, val)
		}
	})

	//子测试2：LINKED = true, FINGERPRINT = true
	t.Run("Find with LINKED and FINGERPRINT", func(t *testing.T) {
		// 判断全局标志
		if !LINKED || !FINGERPRINT {
			t.Skip("Skipping StabilizeBucket test because LINKED or FINGERPRINT is disabled")
		}
		// 构造一个 LNodeHash 节点
		lnHash := &LNodeHash{
			Node: Node{
				lock:        0,
				siblingPtr:  nil,
				leftmostPtr: nil,
				count:       0,
				level:       2,
			},
			Type:        HashNode,
			Cardinality: 4,
			HighKey:     50,
			Buckets:     make([]Bucket, 4),
		}

		// 初始化Bucket（有fingerprint数组）
		for i := 0; i < lnHash.Cardinality; i++ {
			lnHash.Buckets[i].fingerprints = make([]uint8, EntryNum)
			lnHash.Buckets[i].entries = make([]Entry, EntryNum)
			for j := 0; j < EntryNum; j++ {
				lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
				lnHash.Buckets[i].fingerprints[j] = EmptyFingerprint
			}
		}

		// 插入一些数据
		keys := []int{10, 20, 30, 40}
		values := []string{"v10", "v20", "v30", "v40"}
		version := uint64(1) // 假设版本为1

		for i, k := range keys {
			res := lnHash.Insert(k, values[i], version)
			if res != InsertSuccess {
				t.Errorf("Insert failed for key %d, expected InsertSuccess got %d", k, res)
			}
		}

		// 测试查找已存在的键
		for i, k := range keys {
			val := lnHash.Find(k)
			if val == nil {
				t.Errorf("Expected to find key %d, but got nil", k)
			} else if val != values[i] {
				t.Errorf("Expected value %s for key %d, got %v", values[i], k, val)
			}
		}

		// 测试查找不存在的键
		nonExistKey := 999
		val := lnHash.Find(nonExistKey)
		if val != nil {
			t.Errorf("Expected nil for non-existent key %d, got %v", nonExistKey, val)
		}
	})
}

// TestLNodeHash_RangeLookUp 测试 LNodeHash 的 RangeLookUp 方法
func TestLNodeHash_RangeLookUp(t *testing.T) {
	// 子测试1：LINKED = false, FINGERPRINT = false
	if LINKED || FINGERPRINT {
		t.Skip("Skipping Remove test because we want to test under LINKED = false and FINGERPRINT = false")
	}

	t.Run("RangeLookUp without LINKED and FINGERPRINT", func(t *testing.T) {
		// 设置全局标志
		// 构造一个 LNodeHash 节点
		lnHash := &LNodeHash{
			Node: Node{
				lock:        0,
				siblingPtr:  nil,
				leftmostPtr: nil,
				count:       0,
				level:       2,
			},
			Type:        HashNode,
			Cardinality: 4,
			HighKey:     50,
			Buckets:     make([]Bucket, 4),
		}

		// 初始化Bucket
		for i := 0; i < lnHash.Cardinality; i++ {
			lnHash.Buckets[i].entries = make([]Entry, EntryNum)
			for j := 0; j < EntryNum; j++ {
				lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
			}
		}

		// 插入一些数据
		keys := []int{10, 20, 30, 40}
		values := []string{"v10", "v20", "v30", "v40"}
		version := lnHash.lock

		for i, k := range keys {
			res := lnHash.Insert(k, values[i], version)
			if res != InsertSuccess {
				t.Errorf("Insert failed for key %d, expected InsertSuccess got %d", k, res)
			}
		}

		// 执行范围查找
		var buf []interface{}
		count := 0
		searchRange := 3
		continued := false

		resultCount := lnHash.RangeLookUp(0, &buf, count, searchRange, continued)
		expectedCount := 3

		if resultCount != expectedCount {
			t.Errorf("Expected RangeLookUp to return count %d, got %d", expectedCount, resultCount)
		}

		// 验证返回的值是否按顺序排序
		expectedValues := []string{"v10", "v20", "v30"}
		for i, v := range expectedValues {
			if buf[i] != v {
				t.Errorf("Expected buf[%d] to be %s, got %v", i, v, buf[i])
			}
		}
	})

	// 子测试2：LINKED = true, FINGERPRINT = true
	t.Run("RangeLookUp with LINKED and FINGERPRINT", func(t *testing.T) {
		// 设置全局标志
		if !LINKED || !FINGERPRINT {
			t.Skip("Skipping StabilizeBucket test because LINKED or FINGERPRINT is disabled")
		}
		// 构造一个 LNodeHash 节点
		lnHash := &LNodeHash{
			Node: Node{
				lock:        0,
				siblingPtr:  nil,
				leftmostPtr: nil,
				count:       0,
				level:       2,
			},
			Type:        HashNode,
			Cardinality: 4,
			HighKey:     50,
			Buckets:     make([]Bucket, 4),
		}

		// 初始化Bucket（有fingerprint数组）
		for i := 0; i < lnHash.Cardinality; i++ {
			lnHash.Buckets[i].fingerprints = make([]uint8, EntryNum)
			lnHash.Buckets[i].entries = make([]Entry, EntryNum)
			for j := 0; j < EntryNum; j++ {
				lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
				lnHash.Buckets[i].fingerprints[j] = EmptyFingerprint
			}
		}

		// 插入一些数据
		keys := []int{10, 20, 30, 40}
		values := []string{"v10", "v20", "v30", "v40"}
		version := uint64(1) // 假设版本为1

		for i, k := range keys {
			res := lnHash.Insert(k, values[i], version)
			if res != InsertSuccess {
				t.Errorf("Insert failed for key %d, expected InsertSuccess got %d", k, res)
			}
		}

		// 执行范围查找
		var buf []interface{}
		count := 0
		searchRange := 3
		continued := false

		resultCount := lnHash.RangeLookUp(0, &buf, count, searchRange, continued)
		expectedCount := 3

		if resultCount != expectedCount {
			t.Errorf("Expected RangeLookUp to return count %d, got %d", expectedCount, resultCount)
		}

		// 验证返回的值是否按顺序排序
		expectedValues := []string{"v10", "v20", "v30"}
		for i, v := range expectedValues {
			if buf[i] != v {
				t.Errorf("Expected buf[%d] to be %s, got %v", i, v, buf[i])
			}
		}
	})
}

// TestLNodeHash_Convert 测试 LNodeHash 的 Convert 方法
func TestLNodeHash_Convert(t *testing.T) {
	// 子测试1：LINKED = false, FINGERPRINT = false
	t.Run("Convert without LINKED and FINGERPRINT", func(t *testing.T) {
		// 判断全局标志
		if LINKED || FINGERPRINT {
			t.Skip("Skipping Remove test because we want to test under LINKED = false and FINGERPRINT = false")
		}

		lnHash := &LNodeHash{
			Node: Node{
				lock:        0,
				siblingPtr:  nil,
				leftmostPtr: nil,
				count:       4,
				level:       2,
			},
			Type:        HashNode,
			Cardinality: 4, // 根据需要的大小调整
			HighKey:     128,
			Buckets:     make([]Bucket, 4),
		}

		// 初始化Bucket
		for i := 0; i < lnHash.Cardinality; i++ {
			lnHash.Buckets[i].entries = make([]Entry, EntryNum)
			for j := 0; j < EntryNum; j++ {
				lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
			}
		}

		// 向节点中插入足够多的键值对来引发Split
		// 假设EntryNum较小，插入若干条数据
		insertCount := lnHash.Cardinality * EntryNum
		for i := 0; i < insertCount; i++ {
			// 假设Insert不需要fingerprint逻辑
			_ = lnHash.Insert(i, fmt.Sprintf("value%d", i), 0)
		}

		// 执行转换
		leaves, num, err := lnHash.Convert(lnHash.lock)
		if err != nil {
			t.Fatalf("Convert failed: %v", err)
		}
		FillSize := int(FillFactor * float64(lnHash.Cardinality))

		// 验证返回的叶节点数量
		expectedNum := (insertCount + FillSize - 1) / FillSize
		if num != expectedNum {
			t.Errorf("Expected %d leaves, got %d", expectedNum, num)
		}

		// 验证叶节点之间的兄弟指针
		for i := 0; i < num-1; i++ {
			if leaves[i].siblingPtr != leaves[i+1] {
				t.Errorf("Expected leaf[%d].siblingPtr to point to leaf[%d], got %v", i, i+1, leaves[i].siblingPtr)
			}
		}
		if num > 0 && leaves[num-1].siblingPtr != lnHash.siblingPtr {
			t.Errorf("Expected last leaf's SiblingPtr to point to original sibling, got %v", leaves[num-1].siblingPtr)
		}

		// 验证高键
		if num > 0 && leaves[num-1].HighKey != lnHash.HighKey {
			t.Errorf("Expected last leaf's HighKey to be %v, got %v", lnHash.HighKey, leaves[num-1].HighKey)
		}

		fmt.Println("Convert without LINKED and FINGERPRINT completed")
	})

	// 子测试2：LINKED = true, FINGERPRINT = true
	t.Run("Convert with LINKED and FINGERPRINT", func(t *testing.T) {
		// 判断全局标志
		if !LINKED || !FINGERPRINT {
			t.Skip("Skipping StabilizeBucket test because LINKED or FINGERPRINT is disabled")
		}
		// 构造一个需要split的LNodeHash节点
		lnHash := &LNodeHash{
			Node: Node{
				lock:        0,
				siblingPtr:  nil,
				leftmostPtr: nil,
				count:       4,
				level:       2,
			},
			Type:        HashNode,
			Cardinality: 4, // 根据需要的大小调整
			HighKey:     128,
			Buckets:     make([]Bucket, 4),
		}

		// 初始化Bucket
		for i := 0; i < lnHash.Cardinality; i++ {
			lnHash.Buckets[i].entries = make([]Entry, EntryNum)
			for j := 0; j < EntryNum; j++ {
				lnHash.Buckets[i].entries[j] = Entry{Key: nil, Value: nil}
			}
		}

		// 向节点中插入足够多的键值对来引发Split
		// 假设EntryNum较小，插入若干条数据
		insertCount := lnHash.Cardinality * EntryNum
		for i := 0; i < insertCount; i++ {
			// 假设Insert不需要fingerprint逻辑
			_ = lnHash.Insert(i, fmt.Sprintf("value%d", i), 0)
		}

		// 执行转换
		leaves, num, err := lnHash.Convert(lnHash.lock)
		if err != nil {
			t.Fatalf("Convert failed: %v", err)
		}
		FillSize := int(FillFactor * float64(lnHash.Cardinality))
		// 验证返回的叶节点数量
		expectedNum := (insertCount + FillSize - 1) / FillSize
		if num != expectedNum {
			t.Errorf("Expected %d leaves, got %d", expectedNum, num)
		}

		// 验证叶节点之间的兄弟指针
		for i := 0; i < num-1; i++ {
			if leaves[i].siblingPtr != leaves[i+1] {
				t.Errorf("Expected leaf[%d].SiblingPtr to point to leaf[%d], got %v", i, i+1, leaves[i].siblingPtr)
			}
		}
		if num > 0 && leaves[num-1].siblingPtr != lnHash.siblingPtr {
			t.Errorf("Expected last leaf's SiblingPtr to point to original sibling, got %v", leaves[num-1].siblingPtr)
		}

		// 验证高键
		if num > 0 && leaves[num-1].HighKey != lnHash.HighKey {
			t.Errorf("Expected last leaf's HighKey to be %v, got %v", lnHash.HighKey, leaves[num-1].HighKey)
		}

		fmt.Println("Convert with LINKED and FINGERPRINT completed")
	})
}
