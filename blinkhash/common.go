package blinkhash

import (
	"log"
	"runtime"
	"unsafe"
)

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
	NeedConvert   = -2
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
	FillFactor    = 1.0
	BITS_PER_LONG = 64
	LeafBTreeSize = PageSize
	LeafHashSize  = 1024 * 256
)

// compareKeys 比较两个键，需要根据键的实际类型进行具体实现
func compareIntKeys(key1, key2 interface{}) int {
	if key1 == nil && key2 == nil {
		return 0 // 两者都为nil则相等
	} else if key1 == nil {
		// key1为nil但key2不为nil，则key1 < key2
		return -1
	} else if key2 == nil {
		// key2为nil但key1不为nil，则key1 > key2
		return 1
	}

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

// BLinkHash
const (
	LINKED           = false // 启用链接机制
	FINGERPRINT      = false // 启用指纹机制
	EmptyFingerprint = 0
)

// prod
const (
	LNodeHashCardinality  = (LeafHashSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Bucket{}))
	LNodeBTreeCardinality = (LeafBTreeSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Entry{}))
	INodeCardinality      = int((PageSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(new(interface{})))) / int(unsafe.Sizeof(Entry{})))
	EntryNum              = 32
	PageSize              = 512 // 示例页大小，具体值应根据实际情况调整
	HashFuncsNum          = 2
	NumSlot               = 4
	Adaption              = true //lnodeHash是否需要转换为bNode
)

// dev
// const (
//
//	LNodeHashCardinality  = 4
//	LNodeBTreeCardinality = 8
//	INodeCardinality      = 4
//	EntryNum              = 2
//	PageSize              = 4
//	HashFuncsNum          = 1
//	NumSlot               = 2
//	Adaption              = true //lnodeHash是否需要转换为bNode
//
// )
var EnableLockDebug = false // 全局开关，控制是否输出锁调试日志

func lockDebugLog(format string, args ...interface{}) {
	if EnableLockDebug {
		log.Printf(format, args...)
	}
}
func getCallerInfo(skip int) (funcName, fileName string, line int) {
	// skip 表示要跳过的栈帧层数，一般我们在锁函数内调用 getCallerInfo(1)，
	// 就能获取到上层调用者的信息。
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknownFunc", "unknownFile", 0
	}
	fn := runtime.FuncForPC(pc)
	funcName = "unknownFunc"
	if fn != nil {
		funcName = fn.Name() // 带包路径的函数名
	}
	return funcName, file, line
}
