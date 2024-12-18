package test

import (
	"2024-11-timeseries-go/blinkhash"
	"fmt"
	"os"
	"sync"
	"time"
)

func InsertTest() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run insert.go <num_data> <num_threads> <insert_ratio>")
		return
	}

	numData := atoi(os.Args[1])
	numThreads := atoi(os.Args[2])
	//insertRatio := atoi(os.Args[3])

	keys := generateShuffledKeys(numData)
	//keys := generateSerializedKeys(numData)
	//转换成int
	// 将 keys 转换为 []int 类型
	var insertWg sync.WaitGroup
	tree := blinkhash.NewBTree()

	threadInfo := make([]*blinkhash.ThreadInfo, numThreads)
	for i := 0; i < numThreads; i++ {
		threadInfo[i] = blinkhash.NewThreadInfo(tree.GetEpoche())
	}

	half := numData / 2
	chunk := half / numThreads

	// Insert workload function (no range lookup)
	insert := func(from, to int, ti *blinkhash.ThreadInfo) {
		defer insertWg.Done()
		for i := from; i < to; i++ {
			tree.Insert(keys[i], Value_t(keys[i]), ti)
		}
		tree.PrintTree()

	}

	fmt.Println("Insert starts")
	start := time.Now()
	for i := 0; i < numThreads; i++ {
		from, to := chunk*i, chunk*(i+1)
		if i == numThreads-1 {
			to = half
		}
		insertWg.Add(1)
		go insert(from, to, threadInfo[i])
	}
	insertWg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Elapsed time: %.2f usec\n", float64(elapsed.Microseconds()))
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(half)/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f mops/sec\n", float64(half)/elapsed.Seconds()/1e6)
}
