package blinkhash

import (
	"fmt"
	"unsafe"
)

type LNodeHash struct {
	Node
	Type           NodeType
	Cardinality    int
	HighKey        interface{}
	Buckets        []Bucket
	LeftSiblingPtr NodeInterface
}

// TODO: LNodeHash构造函数
// NewLNodeHash 创建一个新的 LNodeHash 节点
func NewLNodeHash(level int) *LNodeHash {
	cardinality := (LeafHashSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Bucket{}))
	return &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  nil,
			leftmostPtr: nil,
			count:       0,
			level:       level,
		},
		Type:           HashNode,
		HighKey:        nil, // 需要在 Split 中设置
		Cardinality:    cardinality,
		Buckets:        make([]Bucket, 0, LeafHashSize),
		LeftSiblingPtr: nil,
	}
}

// NewLNodeHashWithSibling 创建一个新的 LNodeHash 节点，并设置兄弟节点、计数和层级
func NewLNodeHashWithSibling(sibling NodeInterface, count, level int) *LNodeHash {
	cardinality := (LeafHashSize - int(unsafe.Sizeof(Node{})) - int(unsafe.Sizeof(uintptr(0)))) / int(unsafe.Sizeof(Bucket{}))
	return &LNodeHash{
		Node: Node{
			lock:        0,
			siblingPtr:  sibling,
			leftmostPtr: nil,
			count:       count,
			level:       level,
		},
		Type:           HashNode,
		HighKey:        nil, // 需要在 Split 中设置
		Cardinality:    cardinality,
		Buckets:        make([]Bucket, count, LeafHashSize),
		LeftSiblingPtr: nil,
	}
}

// TODO: 实现Node_Interface接口

func (lh *LNodeHash) GetCount() int {
	return lh.count
}

func (lh *LNodeHash) GetLevel() int {
	return lh.level
}

func (lh *LNodeHash) GetLock() uint64 {
	return lh.lock
}

// Print 函数打印 Node 信息
func (lh *LNodeHash) Print() {
	fmt.Printf("LNodeHash Information:\n")
	fmt.Printf("Type: %v\n", lh.Type)
	fmt.Printf("HighKey: %v\n", lh.HighKey)
	fmt.Printf("Cardinality: %d\n", lh.Cardinality)
	lh.Node.Print()
	fmt.Printf("Buckets:\n")
	for i, bucket := range lh.Buckets {
		fmt.Printf("\tBucket %d information: \n", i)
		bucket.Print()
	}
	// 打印 LeftSiblingPtr 信息
	if lh.LeftSiblingPtr != nil {
		fmt.Printf("Left Sibling Pointer: %p\n", lh.LeftSiblingPtr)
	} else {
		fmt.Println("Left Sibling Pointer: nil")
	}
}

func (lh *LNodeHash) SanityCheck(_highKey interface{}, first bool) {
	fmt.Println("我是LNodeHash 调用 SanityCheck")
	sibling := lh.siblingPtr
	if sibling != nil {
		sibling.SanityCheck(_highKey, first)
	}
}

func (lh *LNodeHash) WriteUnlock() {
	// 实现具体方法
	fmt.Println("LNodeHash 打印WriteUnlock")
}

func (lh *LNodeHash) WriteUnlockObsolete() {
	//TODO implement me
	panic("implement me")
}

// TODO: 实现INodeLNodeInterface接口
func (lh *LNodeHash) Insert(key interface{}, value interface{}, version uint64) int {

	fmt.Println("我是LNodeHash，调用Insert")
	return 0
}

func (lh *LNodeHash) Split(splitKey interface{}, key interface{}, value interface{}, version interface{}) *Node {
	//TODO implement me
	fmt.Println("我是LNodeHash，调用Split")
	return nil
}

func (lh *LNodeHash) ConvertUnlock() {
	//TODO implement me
	fmt.Println("LNodeHash 执行ConvertUnlock ")
}

func (lh *LNodeHash) Update(key interface{}, value interface{}, version uint64) int {
	//TODO implement me
	panic("implement me")
}

func (lh *LNodeHash) Remove(key interface{}, version uint64) int {
	//TODO implement me
	panic("implement me")
}

func (lh *LNodeHash) Find(key interface{}) (interface{}, bool) {
	//TODO implement me
	panic("implement me")
}

func (lh *LNodeHash) RangeLookUp(key interface{}, buf *[]interface{}, count int, searchRange int, continued bool) int {
	//TODO implement me
	panic("implement me")
}

func (lh *LNodeHash) Utilization() float64 {
	//TODO implement me
	panic("implement me")
}
