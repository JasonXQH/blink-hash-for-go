package test

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"2024-11-timeseries-go/blinkhash"
)

// KVPair 定义键值对结构
type KVPair struct {
	Key   uint64
	Value uint64
}

// 操作类型常量
const (
	OP_INSERT = iota
	OP_READ
	OP_SCAN
)

// Rdtsc 在Go中无法直接访问CPU时间戳计数器，这里使用纳秒时间代替
func Rdtsc() uint64 {
	return uint64(time.Now().UnixNano())
}

func TimeStampTest() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: ./test <num_data> <num_threads> [mode]")
		os.Exit(1)
	}

	// 解析命令行参数
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

	mode := 0 // 默认模式
	if len(os.Args) >= 4 {
		mode, err = strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Println("Invalid mode:", os.Args[3])
			os.Exit(1)
		}
		// 模式说明
		// 0: 仅扫描
		// 1: 仅读取
		// 2: 平衡模式
		// 3: 仅插入
	}

	// 创建BTree实例
	tree := blinkhash.NewBTree()

	// 初始化每个线程的键值对切片
	keys := make([][]KVPair, numThreads)
	for i := 0; i < numThreads; i++ {
		chunk := numData / numThreads
		keys[i] = make([]KVPair, 0, chunk)
	}

	var loadNum = make([]uint64, numThreads)
	earliest := false

	// 定义插入操作
	loadEarliest := func(tid int, wg *sync.WaitGroup) {
		defer wg.Done()
		sensorID := 0
		chunk := numData / numThreads
		kv := make([]KVPair, chunk)

		for i := 0; i < chunk; i++ {
			key := (Rdtsc()<<16 | uint64(sensorID)<<6) | uint64(tid)
			value := uint64(uintptr(unsafe.Pointer(&kv[i].Key)))
			kv[i] = KVPair{Key: key, Value: value}
			tree.Insert(key, value, tree.GetThreadInfo())

			if sensorID == 1024 {
				sensorID = 0
			}

			if earliest {
				loadNum[tid] = uint64(i)
				return
			}
		}
		loadNum[tid] = uint64(chunk)
		earliest = true
	}

	load := func(tid int, wg *sync.WaitGroup) {
		defer wg.Done()
		sensorID := 0
		chunk := numData / numThreads
		kv := make([]KVPair, chunk)

		for i := 0; i < chunk; i++ {
			key := (Rdtsc()<<16 | uint64(sensorID)<<6) | uint64(tid)
			value := uint64(uintptr(unsafe.Pointer(&kv[i].Key)))
			kv[i] = KVPair{Key: key, Value: value}
			keys[tid] = append(keys[tid], kv[i])
			tree.Insert(key, value, tree.GetThreadInfo())

			if sensorID == 1024 {
				sensorID = 0
			}
		}
	}

	// 定义WaitGroup用于同步
	var wg sync.WaitGroup

	// 记录开始时间
	start := time.Now()

	// 根据模式选择加载方式
	if mode == 3 { // 仅插入
		wg.Add(numThreads)
		for i := 0; i < numThreads; i++ {
			go loadEarliest(i, &wg)
		}
	} else { // 其他模式
		wg.Add(numThreads)
		for i := 0; i < numThreads; i++ {
			go load(i, &wg)
		}
	}
	wg.Wait()

	// 记录结束时间
	end := time.Now()

	// 计算插入吞吐量
	if mode == 3 {
		var total uint64
		for i := 0; i < numThreads; i++ {
			total += loadNum[i]
		}
		numData = int(total)
	}
	elapsed := end.Sub(start).Seconds()
	tput := float64(numData) / elapsed / 1e6
	fmt.Printf("Insertion: %.2f mops/sec\n", tput)

	if mode == 3 {
		return
	}

	// 准备操作集合
	type Operation struct {
		Pair  KVPair
		Op    int
		Range int
	}

	ops := make([]Operation, 0, numData)
	if mode == 0 { // 仅扫描
		fmt.Println("Scan 100%")
		for i := 0; i < numThreads; i++ {
			for _, v := range keys[i] {
				r := rand.Intn(100)
				ops = append(ops, Operation{Pair: v, Op: OP_SCAN, Range: r})
			}
		}
	} else if mode == 1 { // 仅读取
		fmt.Println("Read 100%")
		for i := 0; i < numThreads; i++ {
			for _, v := range keys[i] {
				ops = append(ops, Operation{Pair: v, Op: OP_READ, Range: 0})
			}
		}
	} else if mode == 2 { // 平衡模式
		fmt.Println("Insert 50%, Short scan 30%, Long scan 10%, Read 10%")
		for i := 0; i < numThreads; i++ {
			for _, v := range keys[i] {
				r := rand.Intn(100)
				if r < 50 {
					ops = append(ops, Operation{Pair: v, Op: OP_INSERT, Range: 0})
				} else if r < 80 {
					rangeVal := rand.Intn(5) + 5
					ops = append(ops, Operation{Pair: v, Op: OP_SCAN, Range: rangeVal})
				} else if r < 90 {
					rangeVal := rand.Intn(90) + 10
					ops = append(ops, Operation{Pair: v, Op: OP_SCAN, Range: rangeVal})
				} else {
					ops = append(ops, Operation{Pair: v, Op: OP_READ, Range: 0})
				}
			}
		}
	} else {
		fmt.Println("Invalid workload configuration")
		os.Exit(1)
	}

	// 打乱操作顺序
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(ops), func(i, j int) {
		ops[i], ops[j] = ops[j], ops[i]
	})

	// 定义操作执行函数
	runNum := make([]uint64, numThreads)
	earliest = false

	scan := func(tid int, wg *sync.WaitGroup) {
		defer wg.Done()
		chunk := numData / numThreads
		startIdx := chunk * tid
		endIdx := chunk * (tid + 1)
		if endIdx > numData {
			endIdx = numData
		}

		for i := startIdx; i < endIdx; i++ {
			buf := make([]interface{}, ops[i].Range)
			_ = tree.RangeLookup(ops[i].Pair.Key, ops[i].Range, buf, tree.GetThreadInfo())
			if err != nil && earliest {
				runNum[tid] = uint64(i - startIdx)
				return
			}
		}
		runNum[tid] = uint64(endIdx - startIdx)
		earliest = true
	}

	read := func(tid int, wg *sync.WaitGroup) {
		defer wg.Done()
		chunk := numData / numThreads
		startIdx := chunk * tid
		endIdx := chunk * (tid + 1)
		if endIdx > numData {
			endIdx = numData
		}

		for i := startIdx; i < endIdx; i++ {
			_ = tree.Lookup(ops[i].Pair.Key, tree.GetThreadInfo())
			if err != nil && earliest {
				runNum[tid] = uint64(i - startIdx)
				return
			}
		}
		runNum[tid] = uint64(endIdx - startIdx)
		earliest = true
	}

	mix := func(tid int, wg *sync.WaitGroup) {
		defer wg.Done()
		chunk := numData / numThreads
		startIdx := chunk * tid
		endIdx := chunk * (tid + 1)
		if endIdx > numData {
			endIdx = numData
		}

		sensorID := 0

		for i := startIdx; i < endIdx; i++ {
			op := ops[i].Op
			switch op {
			case OP_INSERT:
				key := (Rdtsc()<<16 | uint64(sensorID)<<6) | uint64(tid)
				value := uint64(uintptr(unsafe.Pointer(&key)))
				tree.Insert(key, value, tree.GetThreadInfo())
				if sensorID == 1024 {
					sensorID = 0
				}
			case OP_SCAN:
				buf := make([]interface{}, ops[i].Range)
				tree.RangeLookup(ops[i].Pair.Key, ops[i].Range, buf, tree.GetThreadInfo())
			case OP_READ:
				tree.Lookup(ops[i].Pair.Key, tree.GetThreadInfo())
			}

			if earliest {
				runNum[tid] = uint64(i - startIdx)
				return
			}
		}
		runNum[tid] = uint64(endIdx - startIdx)
		earliest = true
	}

	// 根据模式选择执行函数
	var operationFunc func(int, *sync.WaitGroup)
	if mode == 0 {
		operationFunc = scan
		fmt.Println("Executing Scan operations")
	} else if mode == 1 {
		operationFunc = read
		fmt.Println("Executing Read operations")
	} else {
		operationFunc = mix
		fmt.Println("Executing Mixed operations")
	}

	// 记录操作开始时间
	start = time.Now()

	// 启动操作并发执行
	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go operationFunc(i, &wg)
	}
	wg.Wait()

	// 记录操作结束时间
	end = time.Now()

	// 计算吞吐量
	var totalOps uint64
	for i := 0; i < numThreads; i++ {
		totalOps += runNum[i]
	}
	elapsed = end.Sub(start).Seconds()
	tput = float64(totalOps) / elapsed / 1e6
	switch mode {
	case 0:
		fmt.Printf("Scan: %.2f mops/sec\n", tput)
	case 1:
		fmt.Printf("Read: %.2f mops/sec\n", tput)
	default:
		fmt.Printf("Mix: %.2f mops/sec\n", tput)
	}
}
