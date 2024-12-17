package test

import (
	"2024-11-timeseries-go/blinkhash"
	"fmt"
	"os"
	"time"
)

const (
	MaxCoreNum = 64
)

// 模拟 rdtsc，返回当前时间戳纳秒
func rdtsc() uint64 {
	return uint64(time.Now().UnixNano())
}

// 多线程启动函数
//func startThreads(tree *blinkhash.BTree, numThreads int, fn func(threadID int)) {
//	var wg sync.WaitGroup
//
//	for threadID := 0; threadID < numThreads; threadID++ {
//		wg.Add(1)
//		go func(tid int) {
//			defer wg.Done()
//			pinToCore(tid) // 占位符
//			fn(tid)
//		}(threadID)
//	}
//
//	wg.Wait()
//}
//

func RdtscTest() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run rdtsc.go <num_data> <num_threads>")
		return
	}

	// 参数解析
	numData := atoi(os.Args[1])
	numThreads := atoi(os.Args[2])

	// 初始化 BTree
	tree := blinkhash.NewBTree()
	fmt.Println("BTree initialized.")

	// 每个线程的插入函数
	funcInsert := func(tid int) {
		chunk := numData / numThreads
		for i := 0; i < chunk; i++ {
			ti := blinkhash.NewThreadInfo(tree.GetEpoche())
			key := (rdtsc() << 6) | uint64(tid) // 模拟 C++ RDTSC 行为
			tree.Insert(Key_t(key), Value_t(key), ti)
		}
	}

	// 性能计时
	startTime := time.Now()
	startThreads(numThreads, funcInsert)
	elapsed := time.Since(startTime)

	// 吞吐量计算
	tput := float64(numData) / elapsed.Seconds() / 1e6
	fmt.Printf("Throughput: %.2f mops/sec\n", tput)

	// 树的高度
	height := tree.GetHeight()
	fmt.Printf("Height of tree: %d\n", height+1)

	// 利用率
	utilization := tree.Utilization()
	fmt.Printf("Tree utilization: %.2f%%\n", utilization)
}
