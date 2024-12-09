package blinkhash

//
//// 假设 Bucket 结构和相关方法已在 bucket.go 中定义，这里仅展示如何测试这些方法
//func TestBucketOperations(t *testing.T) {
//	b := NewBucket(10)
//	testKeys := []interface{}{"key1", "key2", "key3", nil, "key5"}
//	testValues := []interface{}{123, "hello", true, nil, 5.67}
//
//	// 填充测试数据
//	for i, key := range testKeys {
//		b.entries[i] = Entry{Key: key, Value: testValues[i]}
//	}
//
//	// 测试 Update
//	if !b.Update("key1", 321) {
//		t.Error("Update failed for existing key")
//	}
//	if b.Update("key4", "nonexistent") {
//		t.Error("Update should fail for non-existent key")
//	}
//
//	// 测试 Remove
//	if !b.Remove("key2") {
//		t.Error("Remove failed for existing key")
//	}
//	if b.Remove("key4") {
//		t.Error("Remove should fail for non-existent key")
//	}
//
//	// 测试 CollectKeys
//	//keys := make([]interface{}, 0)
//	keys, done := b.CollectKeys(3)
//	if done {
//		fmt.Println("Collected enough keys:", keys)
//	} else {
//		fmt.Println("Not enough keys, got:", keys)
//	}
//
//	// 测试 CollectAllKeys
//	allKeys := b.CollectAllKeys()
//	fmt.Println("all keys: ", allKeys)
//	// 打印更新后的值
//	for _, entry := range b.entries {
//		fmt.Printf("Key: %v, Value: %v\n", entry.Key, entry.Value)
//	}
//}
