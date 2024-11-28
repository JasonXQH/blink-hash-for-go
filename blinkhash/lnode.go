package blinkhash

import "fmt"

const (
	BTreeNode NodeType = iota
	HashNode
	LeafBTreeSize = PageSize
	LeafHashSize  = 1024 * 256
	HashFuncsNum  = 2
	NumSlot       = 4
)

type NodeType int

type LNode struct {
	Node
	Type     NodeType
	HighKey  interface{}
	Behavior LNodeInterface // 添加接口引用以实现多态
}

func (n *LNode) getLNodeType() NodeType {
	return n.Type
}

// LNodeInterface
//
//	@Description:
type LNodeInterface interface {
	WriteUnlock()
	ConvertUnlock()
	WriteUnlockObsolete()
	Insert(key interface{}, value interface{}, version uint64) int
	Split(splitKey interface{}, key interface{}, value interface{}, version interface{}) *Node
	Update(key interface{}, value interface{}, version uint64) int
	Remove(key interface{}, version uint64) int
	Find(key interface{}) (interface{}, bool)
	RangeLookUp(key interface{}, buf *[]interface{}, count int, searchRange int, continued bool) int
	Print()
	SanityCheck(key interface{}, first bool)
	Utilization() float64
}

// NewLNode 创建一个新的 LNode 实例
//
//	@Description:
//	@param level
//	@param nodeType
//	@return *LNode
//
// NewLNode 创建一个新的 LNode 实例
func NewLNode(level int, nodeType NodeType) *LNode {
	lnode := &LNode{
		Node: Node{
			level: level,
		},
		Type: nodeType,
	}
	lnode.Node.Behavior = lnode
	return lnode
}

// NewLNodeWithSibling
//
//	@Description:
//	@param nodeType
//	@param sibling
//	@param count
//	@param level
//	@return *LNode
//
// NewLNode 创建 LNode
func NewLNodeWithSibling(nodeType NodeType, sibling *Node, count, level int) *LNode {
	lnode := &LNode{
		Node: Node{
			siblingPtr: sibling,
			count:      count,
			level:      level,
		},
		Type: nodeType,
	}
	lnode.Node.Behavior = lnode
	return lnode
}

// WriteUnlock
//
//	@Description:
//	@receiver n
func (n *LNode) WriteUnlock() {
	if n.Behavior != nil {
		n.Behavior.WriteUnlock()
		return
	}
	fmt.Printf("write_unlock: node type error: %v\n", n.Type)
}

// ConvertUnlock
//
//	@Description:
//	@receiver n
func (n *LNode) ConvertUnlock() {
	if n.Behavior != nil {
		n.Behavior.ConvertUnlock()
		return
	}
	fmt.Printf("ConvertUnlock: node type error: %v\n", n.Type)
}

// WriteUnlockObsolete
//
//	@Description:
//	@receiver n
func (n *LNode) WriteUnlockObsolete() {
	if n.Behavior != nil {
		n.Behavior.ConvertUnlock()
		return
	}
	fmt.Printf("WriteUnlockObsolete: node type error: %v\n", n.Type)
}

// Insert
//
//	@Description:
//	@receiver n
//	@param key
//	@param value
//	@param version
//	@return int
func (n *LNode) Insert(key, value interface{}, version uint64) int {
	if n.Behavior != nil {
		return n.Behavior.Insert(key, value, version)
	}
	fmt.Printf("Insert: node type error: %v\n", n.Type)
	return 0
}

// Split
//
//	@Description:
//	@receiver n
//	@param splitKey
//	@param key
//	@param value
//	@param version
//	@return *Node
func (n *LNode) Split(splitKey, key, value interface{}, version uint64) *Node {
	if n.Behavior != nil {
		return n.Behavior.Split(splitKey, key, value, version)
	}
	fmt.Printf("split: node type error: %v\n", n.Type)
	return nil
}

// Update
//
//	@Description:
//	@receiver n
//	@param key
//	@param value
//	@param version
//	@return int
func (n *LNode) Update(key, value interface{}, version uint64) int {
	if n.Behavior != nil {
		return n.Behavior.Update(key, value, version)
	}
	fmt.Printf("Update: node type error: %v\n", n.Type)
	return 0
}

// Remove
//
//	@Description:
//	@receiver n
//	@param key
//	@param version
//	@return int
func (n *LNode) Remove(key interface{}, version uint64) int {
	if n.Behavior != nil {
		return n.Behavior.Remove(key, version)
	}
	fmt.Printf("Update: node type error: %v\n", n.Type)
	return 0
}

// Find
//
//	@Description:
//	@receiver n
//	@param key
//	@return interface{}
//	@return bool
func (n *LNode) Find(key interface{}) (interface{}, bool) {
	if n.Behavior != nil {
		return n.Behavior.Find(key)
	}
	fmt.Printf("Find: node type error: %v\n", n.Type)
	return nil, false
}

func (n *LNode) RangeLookup(key interface{}, buf *[]interface{}, count int, searchRange int, continued bool) int {
	if n.Behavior != nil {
		return n.Behavior.RangeLookUp(key, buf, count, searchRange, continued)
	}
	fmt.Printf("RangeLookup: node type error: %v\n", n.Type)
	return 0
}

// Print
//
//	@Description:
//	@receiver n
func (n *LNode) Print() {
	if n.Behavior != nil {
		n.Behavior.Print()
	}
	fmt.Printf("Print: node type error: %v\n", n.Type)
}

// SanityCheck
//
//	@Description:
//	@receiver n
//	@param key
//	@param first
func (n *LNode) SanityCheck(key interface{}, first bool) {
	if n.Behavior != nil {
		n.Behavior.SanityCheck(key, first)
	}
	fmt.Printf("Print: node type error: %v\n", n.Type)
}

// Utilization
//
//	@Description:
//	@receiver n
//	@return float64
func (n *LNode) Utilization() float64 {
	if n.Behavior != nil {
		return n.Behavior.Utilization()
	}
	fmt.Printf("Print: node type error: %v\n", n.Type)
	return 0
}
