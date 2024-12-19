package test

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
	"timeseries-go/blinkhash"
)

func Test() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run main.go <num_data> <num_threads> <insert_only>")
		os.Exit(1)
	}

	// 参数解析
	numData, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid num_data:", os.Args[1])
		os.Exit(1)
	}

	numThreads, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Invalid num_threads:", os.Args[2])
		os.Exit(1)
	}

	insertOnly, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("Invalid insert_only:", os.Args[3])
		os.Exit(1)
	}

	// 初始化keys并打乱顺序
	keys := make([]Key_t, numData)
	for i := 0; i < numData; i++ {
		keys[i] = Key_t(i + 1)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(numData, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	// 创建BTree实例
	tree := blinkhash.NewBTree()

	// 在C++中打印:
	//  "inode_size(...) , lnode_btree_size(...) , lnode_hash_size(...)"
	// 假设在Go中blinkhash包中有类似的全局变量或方法可获取
	// 如无此类信息，可注释掉该行。
	fmt.Printf("lnode_btree_size(%d), lnode_hash_size(%d)\n",
		blinkhash.LNodeBTreeCardinality, blinkhash.LNodeHashCardinality)

	// notFoundKeys存储未找到的键
	notFoundKeys := make([][]Key_t, numThreads)

	// 计算每个线程处理的数据量
	chunk := numData / numThreads

	// 准备线程信息（假设GetEpoche可用）
	threadInfo := make([]*blinkhash.ThreadInfo, numThreads)
	for i := 0; i < numThreads; i++ {
		threadInfo[i] = blinkhash.NewThreadInfo(tree.GetEpoche())
	}

	// 定义插入函数
	insertFunc := func(tid int, wg *sync.WaitGroup) {
		defer wg.Done()
		from := chunk * tid
		to := chunk * (tid + 1)
		if to > numData {
			to = numData
		}
		ti := threadInfo[tid]
		for i := from; i < to; i++ {
			// 将key本身作为value存储
			tree.Insert(keys[i], Value_t(keys[i]), ti)
		}
	}

	// 定义搜索函数
	searchFunc := func(tid int, wg *sync.WaitGroup) {
		defer wg.Done()
		from := chunk * tid
		to := chunk * (tid + 1)
		if to > numData {
			to = numData
		}
		ti := threadInfo[tid]
		for i := from; i < to; i++ {
			ret := tree.Lookup(keys[i], ti)
			// 如果值不等于插入时的值（即keys[i]），则认为未找到正确结果
			if ret != Value_t(keys[i]) {
				notFoundKeys[tid] = append(notFoundKeys[tid], keys[i])
			}
		}
	}

	// 插入阶段
	fmt.Println("Insertion starts")
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go insertFunc(i, &wg)
	}
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("elapsed time: %.2f usec\n", float64(elapsed.Microseconds()))
	tput := float64(numData) / elapsed.Seconds() / 1e6
	fmt.Printf("throughput: %.2f mops/sec\n", tput)

	// 如果insert_only不为0，则执行搜索
	if insertOnly != 0 {
		fmt.Println("Search starts")
		start = time.Now()

		wg.Add(numThreads)
		for i := 0; i < numThreads; i++ {
			go searchFunc(i, &wg)
		}
		wg.Wait()

		elapsed = time.Since(start)
		fmt.Printf("elapsed time: %.2f usec\n", float64(elapsed.Microseconds()))
		tput = float64(numData) / elapsed.Seconds() / 1e6
		fmt.Printf("throughput: %.2f mops/sec\n", tput)
	}

	// 完整性检查、树高度和利用率计算
	tree.SanityCheck()
	height := tree.GetHeight()
	fmt.Printf("Height of tree: %d\n", height+1)
	util := tree.Utilization()
	fmt.Printf("utilization of leaf nodes: %.2f %%\n", util)
}
