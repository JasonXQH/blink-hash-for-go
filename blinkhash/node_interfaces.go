package blinkhash

type NodeType int

type NodeInterface interface {
	GetCount() int32
	GetLevel() int
	GetLock() uint64
	Print()
	WriteUnlock()
	WriteUnlockObsolete()
	TryReadLock() (uint64, bool)
	GetVersion() (uint64, bool)
	GetSiblingPtr() NodeInterface
	GetLeftmostPtr() NodeInterface
	GetHighKey() interface{}
	SanityCheck(prevHighKey interface{}, first bool)
	GetType() NodeType
	TryUpgradeWriteLock(version uint64) (bool, bool)
	IncrementCount()
	DecrementCount()
	EntryGetter
}

type INodeInterface interface {
	NodeInterface
	BatchInsertable
	CardinalityGetter
	Insertable
	INodeSplit
	NodeGetter
	NodeScanner
	FullJudger
}

//type LeafNodeInterface interface {
//	INodeInterface
//	Updatable
//	Removable
//	Finder
//	RangeLookuper
//	Utilizer
//	FootPrinter
//}

type LeafNodeInterface interface {
	NodeInterface
	Insertable
	Splittable
	Updatable
	Removable
	Finder
	RangeLookuper
	Utilizer
	NodeGetter
	FootPrinter
	CardinalityGetter
}

// Insertable 接口定义插入方法
type Insertable interface {
	Insert(key interface{}, value interface{}, version uint64) int
}

// Splittable 接口定义分裂方法
type Splittable interface {
	Split(key interface{}, value interface{}, version uint64) (Splittable, interface{})
}

// INodeSplit 接口定义分裂方法
type INodeSplit interface {
	Split() (INodeInterface, interface{})
}

// Updatable 接口定义更新方法
type Updatable interface {
	Update(key interface{}, value interface{}, version uint64) int
}

// Removable 接口定义移除方法
type Removable interface {
	Remove(key interface{}, version uint64) int
}

// Finder 接口定义查找方法
type Finder interface {
	Find(key interface{}) (interface{}, bool)
}

// RangeLookuper 接口定义范围查找方法
type RangeLookuper interface {
	RangeLookUp(key interface{}, buf *[]interface{}, count int, searchRange int, continued bool) int
}

// Utilizer 接口定义利用率方法
type Utilizer interface {
	Utilization() float64
}

type NodeGetter interface {
	GetNode() *Node
}

type FootPrinter interface {
	Footprint(metrics *FootprintMetrics)
}

type NodeScanner interface {
	ScanNode(key interface{}) NodeInterface
}
type FullJudger interface {
	IsFull() bool
}

type EntryGetter interface {
	GetEntries() []Entry
}
type CardinalityGetter interface {
	GetCardinality() int
}

type BatchInsertable interface {
	BatchInsertLastLevel(keys []interface{}, values []NodeInterface, num int, batchSize int) ([]*INode, error)
	BatchInsert(keys []interface{}, values []NodeInterface, num int) ([]*INode, error)
}
