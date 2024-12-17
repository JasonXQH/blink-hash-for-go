package main

import (
	"2024-11-timeseries-go/test"
	"os"
)

//TIP To run your code, right-click the code and select <b>Run</b>. Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.

func main() {
	// 模拟命令行参数
	os.Args = append(os.Args, "100", "4", "50") // 比如 num_data=10000, num_threads=4, insert_ratio=50
	//var _ blinkhash.INodeInterface = (*blinkhash.LNodeHash)(nil)
	// 调用 MixedTest
	test.MixedTest()

	//test.RangeTest()
	//
	//test.RdtscTest()

	//test.Test()
}

//TIP See GoLand help at <a href="https://www.jetbrains.com/help/go/">jetbrains.com/help/go/</a>.
// Also, you can try interactive lessons for GoLand by selecting 'Help | Learn IDE Features' from the main menu.
