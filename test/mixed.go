package test

import (
	"2024-11-timeseries-go/blinkhash"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type Key_t = int
type Value_t = int

func _Rdtsc() int {
	return int(time.Now().UnixNano())
}

func MixedTest() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run test.go <num_data> <num_threads> <insert_ratio>")
		return
	}

	numData := atoi(os.Args[1])
	numThreads := atoi(os.Args[2])
	insertRatio := atoi(os.Args[3])

	keys := generateShuffledKeys(numData)
	//转换成int
	// 将 keys 转换为 []int 类型
	var warmupWg, mixedWg sync.WaitGroup
	tree := blinkhash.NewBTree()

	threadInfo := make([]*blinkhash.ThreadInfo, numThreads)
	for i := 0; i < numThreads; i++ {
		threadInfo[i] = blinkhash.NewThreadInfo(tree.GetEpoche())
	}

	half := numData / 2
	chunk := half / numThreads

	// Warmup function
	warmup := func(from, to int, ti *blinkhash.ThreadInfo) {
		defer warmupWg.Done()
		for i := from; i < to; i++ {
			tree.Insert(keys[i], Value_t(keys[i]), ti)
		}
	}

	// Mixed workload function
	mixed := func(from, to int, tid int, ti *blinkhash.ThreadInfo) {
		defer mixedWg.Done()
		for i := from; i < to; i++ {
			ratio := rand.Intn(100)
			if ratio < insertRatio {
				tree.Insert(keys[i+half], Value_t(keys[i+half]), ti)
			} else {
				var buf []interface{}
				_ = tree.RangeLookup(keys[i], 100, buf, ti)
			}
		}
	}

	fmt.Println("Warmup starts")
	start := time.Now()
	for i := 0; i < numThreads; i++ {
		from, to := chunk*i, chunk*(i+1)
		if i == numThreads-1 {
			to = half
		}
		warmupWg.Add(1)
		go warmup(from, to, threadInfo[i])
	}
	warmupWg.Wait()

	fmt.Println("Mixed starts")
	start = time.Now()
	for i := 0; i < numThreads; i++ {
		from, to := chunk*i, chunk*(i+1)
		if i == numThreads-1 {
			to = half
		}
		mixedWg.Add(1)
		go mixed(from, to, i, threadInfo[i])
	}
	mixedWg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Elapsed time: %.2f usec\n", float64(elapsed.Microseconds()))
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(half)/elapsed.Seconds())
	fmt.Printf("Throughput: %.2f mops/sec\n", float64(half)/elapsed.Seconds()/1e6)
}

func atoi(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return val
}

func generateShuffledKeys(n int) []Key_t {
	keys := make([]Key_t, n)
	for i := 0; i < n; i++ {
		keys[i] = Key_t(i + 1)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return keys
}
