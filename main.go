package main

import (
	"2024-11-timeseries-go/blinkhash"
	"fmt"
)

//TIP To run your code, right-click the code and select <b>Run</b>. Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.

func main() {

	// Example usage of the Bucket methods
	b := blinkhash.NewBucket(5)
	keys := make([]interface{}, 0)
	b.CollectAllKeys(&keys)
	fmt.Println("Collected keys:", keys)
}

//TIP See GoLand help at <a href="https://www.jetbrains.com/help/go/">jetbrains.com/help/go/</a>.
// Also, you can try interactive lessons for GoLand by selecting 'Help | Learn IDE Features' from the main menu.
