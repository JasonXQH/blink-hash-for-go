package blinkhash

import (
	"fmt"
	"unsafe"
)

type LNodeHash struct {
	Cardinality int
	LNode
	Buckets []Bucket
}

func NewLNodeHash() *LNodeHash {
	cardinality := (LeafHashSize - unsafe.Sizeof(LNode{}) - unsafe.Sizeof(&LNode{})) / unsafe.Sizeof(Bucket{})
	lNodeHash := &LNodeHash{
		LNode:       *NewLNode(0, BTreeNode),
		Cardinality: int(cardinality),
		Buckets:     make([]Bucket, cardinality),
	}
	lNodeHash.LNode.Behavior = lNodeHash
	return lNodeHash
}

// NewLNodeHashWithLevel
//
//	@Description:
//	@param level
//	@return *LNodeHash
func NewLNodeHashWithLevel(level int) *LNodeHash {
	cardinality := (LeafHashSize - unsafe.Sizeof(LNode{}) - unsafe.Sizeof(&LNode{})) / unsafe.Sizeof(Bucket{})
	lNodeHash := &LNodeHash{
		LNode:       *NewLNode(level, BTreeNode),
		Cardinality: int(cardinality),
		Buckets:     make([]Bucket, cardinality),
	}
	lNodeHash.LNode.Behavior = lNodeHash
	return lNodeHash
}

func NewLNodeHashWithSibling(sibling *Node, count, level int) *LNodeHash {
	cardinality := (LeafHashSize - unsafe.Sizeof(LNode{}) - unsafe.Sizeof(&LNode{})) / unsafe.Sizeof(Bucket{})
	lNodeHash := &LNodeHash{
		Cardinality: int(cardinality),
		LNode:       *NewLNodeWithSibling(HashNode, sibling, count, level),
		Buckets:     make([]Bucket, cardinality),
	}
	lNodeHash.LNode.Behavior = lNodeHash
	return lNodeHash
}

func (b *LNodeHash) WriteUnlock() {
	// 实现具体方法
	fmt.Println("LNodeHash 打印WriteUnlock")
}

func (b *LNodeHash) ConvertUnlock() {
	//TODO implement me
	fmt.Println("LNodeHash 执行ConvertUnlock ")
}

func (b *LNodeHash) Insert(key interface{}, value interface{}, version uint64) int {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeHash) Split(splitKey interface{}, key interface{}, value interface{}, version interface{}) *Node {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeHash) Update(key interface{}, value interface{}, version uint64) int {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeHash) Remove(key interface{}, version uint64) int {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeHash) Find(key interface{}) (interface{}, bool) {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeHash) RangeLookUp(key interface{}, buf *[]interface{}, count int, searchRange int, continued bool) int {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeHash) Print() {
	//TODO implement me
	panic("implement me")
}

func (b *LNodeHash) SanityCheck(key interface{}, first bool) {
	fmt.Println("LNodeHash 打印 SanityCheck")
	panic("implement me")
}

func (b *LNodeHash) Utilization() float64 {
	//TODO implement me
	panic("implement me")
}
