package test

import (
	"fmt"
	"os"
	"time"
	"timeseries-go/blinkhash"
)

func SingleThreadTest() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run test.go <num_data>")
		return
	}

	numData := atoi(os.Args[1])
	keys := generateShuffledKeys(numData)

	tree := blinkhash.NewBTree()
	ti := blinkhash.NewThreadInfo(tree.GetEpoche())

	//half := numData / 2
	half := 1

	// Warmup
	fmt.Println("Warmup starts (single thread)")
	start := time.Now()
	for i := 0; i < numData; i++ {
		tree.Insert(keys[i], Value_t(keys[i]), ti)
	}
	fmt.Println("Warmup done in", time.Since(start))

	// Mixed or purely RangeLookup
	// 这里你可以自己定义是只查还是插+查混合
	// 比如：只测试 RangeLookup
	fmt.Println("Single-thread RangeLookups start")
	start = time.Now()
	for i := 0; i < 100; i++ {
		_ = tree.RangeLookup(keys[0], 100, ti)
		//fmt.Printf("RangeLookup minKey=%v", keys[i])
		//results := tree.RangeLookup(keys[i], 100, ti)
		//fmt.Printf("RangeLookup minKey=%v got %d results: %v\n", keys[i], len(results), results)
	}
	elapsed := time.Since(start)
	fmt.Println("RangeLookup done in", elapsed, "for", half, "ops")
	// 吞吐量计算
	fmt.Printf("RangeLookUp Throughput: %.2f ops/sec\n", float64(half)/elapsed.Seconds())
	fmt.Printf("RangeLookUp Throughput: %.2f mops/sec\n", float64(half)/elapsed.Seconds()/1e6)

	//也可以随时打断点查看树结构：
	tree.PrintTree()
}
