package blinkhash

const (
	BASENode  NodeType = iota
	INNERNode NodeType = iota
	BTreeNode NodeType = iota
	HashNode  NodeType = iota
)

const (
	NEED_RESTART   = -1 // 需要重启
	INSERT_SUCCESS = 0  // 插入成功
	NEED_SPLIT     = 1  // 需要分割
)

var statusNames = map[int]string{
	NEED_RESTART:   "NEED_RESTART",
	INSERT_SUCCESS: "INSERT_SUCCESS",
	NEED_SPLIT:     "NEED_SPLIT",
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
