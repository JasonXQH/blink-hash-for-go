package blinkhash

import (
	"testing"
)

// 测试 LNode 的 WriteUnlock 方法的多态行为
func TestWriteUnlockPolymorphism(t *testing.T) {
	// 创建一个 bytes.Buffer 来捕获输出
	// TestWriteUnlockVisual测试LNode的WriteUnlock方法的多态行为，通过观察输出进行手动验证。
	btreeNode := NewLNodeBTree()
	btreeNode.WriteUnlock()
	// 创建 LNodeHash 的实例并调用 WriteUnlock
	hashNode := NewLNodeHash()
	hashNode.WriteUnlock()
}

func TestConvertUnlockPolymorphism(t *testing.T) {
	// 创建一个 bytes.Buffer 来捕获输出
	// TestWriteUnlockVisual测试LNode的WriteUnlock方法的多态行为，通过观察输出进行手动验证。
	btreeNode := NewLNodeBTree()
	btreeNode.ConvertUnlock()
	// 创建 LNodeHash 的实例并调用 WriteUnlock
	hashNode := NewLNodeHash()
	hashNode.ConvertUnlock()
}
