package blinkhash

const (
	BASENode  NodeType = iota
	INNERNode NodeType = iota
	BTreeNode NodeType = iota
	HashNode  NodeType = iota
)

const (
	NeedRestart   = -1 // 需要重启
	InsertSuccess = 0  // 插入成功
	NeedSplit     = 1  // 需要分割
	KeyNotFound   = 1
	RemoveSuccess = 0
	UpdateSuccess = 0
	UpdateFailure = 1
)

var statusNames = map[int]string{
	NeedRestart:   "NEED_RESTART",
	InsertSuccess: "INSERT_SUCCESS",
	NeedSplit:     "NEED_SPLIT",
}

const (
	Number64_1 = 11400714785074694791
	Number64_2 = 14029467366897019727
	Number64_3 = 1609587929392839161
	Number64_4 = 9650029242287828579
	Number64_5 = 2870177450012600261
	Seed       = 0xc70f6907
)

func getStatusName(status int) string {
	if name, exists := statusNames[status]; exists {
		return name
	}
	return "UNKNOWN_STATUS"
}

// Key64  在Go中对应为 uint64 类型。
type Key64 uint64

// Value64  在Go中也对应为 uint64 类型。
type Value64 uint64

// 其他需要的常量或函数可以在这里定义。
const (
	EntryNum      = 32
	PageSize      = 512 // 示例页大小，具体值应根据实际情况调整
	FillFactor    = 0.8
	BITS_PER_LONG = 64
	LeafBTreeSize = PageSize
	LeafHashSize  = 1024 * 256
	HashFuncsNum  = 2
	NumSlot       = 4
)

// compareKeys 比较两个键，需要根据键的实际类型进行具体实现
func compareIntKeys(key1, key2 interface{}) int {
	// 示例实现，假设键类型为 int
	k1 := key1.(int)
	k2 := key2.(int)
	if k1 < k2 {
		return -1
	} else if k1 > k2 {
		return 1
	}
	return 0
}

const (
	LINKED      = false // 启用链接机制
	FINGERPRINT = false // 启用指纹机制
)
