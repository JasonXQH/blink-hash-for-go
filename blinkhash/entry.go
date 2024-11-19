package blinkhash

// Entry 是一个泛型结构，在 Go 中通过使用 interface{} 来模拟泛型。
type Entry struct {
	Key   interface{}
	Value interface{}
}

// Empty 是用来表示空键的全局变量，这里以 interface{} 类型实现以模拟模板功能。
var Empty interface{} = nil
