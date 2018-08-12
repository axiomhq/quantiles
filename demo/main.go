package main

import (
	"fmt"

	"github.com/axiomhq/quantiles"
)

func main() {
	qstream, err := quantiles.New(0.01, 1<<4)
	if err != nil {
		panic(err)
	}
	for i := 0.0; i < 1e6; i++ {
		if err := qstream.Push(i, 1.0); err != nil {
			panic(err)
		}
	}
	fmt.Println(qstream.ApproximationError(1))
	fmt.Println(qstream.Finalize())
	fmt.Println(qstream.GenerateQuantiles(4))
	fmt.Println(qstream.GenerateQuantiles(10))
	fmt.Println(qstream.GenerateQuantiles(4))
	sum, err := qstream.FinalSummary()
	if err != nil {
		panic(err)
	}
	fmt.Println(sum.GenerateQuantiles(4))
}
