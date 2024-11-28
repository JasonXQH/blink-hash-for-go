package blinkhash

type BTree[KeyType, ValueType any] struct {
	root   *Node   // 树的根节点
	epoche *Epoche // 用于管理线程安全和垃圾回收
}
