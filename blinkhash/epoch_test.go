package blinkhash

import (
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestEpocheLifecycle(t *testing.T) {
	e := &Epoche{StartGCThreshold: 10}
	ti := NewThreadInfo(e)

	// 模拟进入纪元
	e.EnterEpoche(ti)
	if atomic.LoadUint64(&ti.DeletionList.LocalEpoche) != atomic.LoadUint64(&e.CurrentEpoche) {
		t.Errorf("Epoche entry did not synchronize the local epoche correctly")
	}

	// 模拟标记节点为删除，并手动增加 ThresholdCounter 至触发条件
	node := unsafe.Pointer(&struct{}{}) // 使用空结构体指针作为节点
	for i := 0; i < 11; i++ {
		e.MarkNodeForDeletion(node, ti) // 调用多次以确保达到 GC 阈值
	}

	// 模拟退出并清理
	e.ExitEpocheAndCleanup(ti)
	if ti.DeletionList.DeletionListCount != 0 {
		t.Errorf("Cleanup did not process deletion list correctly")
	}
}

func TestConcurrentEpocheEntries(t *testing.T) {
	e := &Epoche{StartGCThreshold: 10}
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ti := NewThreadInfo(e)
			e.EnterEpoche(ti)
			node := unsafe.Pointer(&struct{}{})
			e.MarkNodeForDeletion(node, ti)
			e.ExitEpocheAndCleanup(ti)
		}()
	}

	wg.Wait()
	if atomic.LoadUint64(&e.CurrentEpoche) == 0 {
		t.Errorf("Current epoche was not incremented correctly")
	}
}
