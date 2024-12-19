package test

import (
	"fmt"
	"os"
	"sync"
	"time"
	"timeseries-go/blinkhash"
)

const (
	WarmupThreads = 64
	Range         = 50
)

// 绑定线程到核心（Go 目前没有直接等效的 CPU 亲和性设置）
func pinToCore(threadID int) {
	// Go 没有直接实现线程绑定到 CPU 核心的方法
	// 这里只是个占位符，未来可以添加外部库（如 `github.com/shirou/gopsutil/cpu`）来支持
}

func startThreads(numThreads int, fn func(tid int)) {
	var wg sync.WaitGroup
	for tid := 0; tid < numThreads; tid++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			pinToCore(threadID) // 占位符
			fn(threadID)
		}(tid)
	}
	wg.Wait()
}

func getNow() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func RangeTest() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run range_test.go <num_data> <num_threads>")
		return
	}

	numData := atoi(os.Args[1])
	numThreads := atoi(os.Args[2])

	keys := generateShuffledKeys(numData)

	// 初始化 BTree
	tree := blinkhash.NewBTree()
	fmt.Println("BTree initialized.")

	// warmup
	warmup := func(tid int) {
		chunk := numData / WarmupThreads
		from := chunk * tid
		to := from + chunk
		if tid == WarmupThreads-1 {
			to = numData
		}

		for i := from; i < to; i++ {
			ti := blinkhash.NewThreadInfo(tree.GetEpoche())
			tree.Insert(keys[i], Value_t(keys[i]), ti)
		}
	}

	// 扫描阶段
	scan := func(tid int) {
		chunk := numData / numThreads
		from := chunk * tid
		to := from + chunk
		if tid == numThreads-1 {
			to = numData
		}

		buf := make([]interface{}, Range)
		for i := from; i < to; i++ {
			ti := blinkhash.NewThreadInfo(tree.GetEpoche())
			_ = tree.RangeLookup(keys[i], Range, buf, ti)
		}
	}

	fmt.Println("Warmup starts")
	startThreads(WarmupThreads, warmup)

	height := tree.GetHeight()
	fmt.Printf("Height of tree: %d\n", height+1)

	// 扫描阶段
	fmt.Println("Scan starts")
	startTime := getNow()
	var start = time.Now()

	startThreads(numThreads, scan)

	end := time.Now()
	endTime := getNow()

	elapsed := end.Sub(start).Nanoseconds()
	tput := float64(numData) / (float64(elapsed) / 1e9) / 1e6

	fmt.Printf("Elapsed time: %.2f usec\n", float64(elapsed)/1000.0)
	fmt.Printf("Throughput: %.2f mops/sec\n", tput)

	tput = float64(numData) / (endTime - startTime) / 1e6
	fmt.Printf("Throughput: %.2f mops/sec\n", tput)

	height = tree.GetHeight()
	fmt.Printf("Height of tree: %d\n", height+1)

	// 更新操作验证
	updateFail := 0
	for i := 0; i < numData; i++ {
		ti := blinkhash.NewThreadInfo(tree.GetEpoche())
		ret := tree.Update(keys[i], Value_t(keys[i])+1, ti)
		if ret != true {
			updateFail++
		}
	}
	fmt.Printf("Update failures: %d\n", updateFail)
}
