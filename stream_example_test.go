package quantiles_test

import (
	"fmt"

	"github.com/axiomhq/quantiles"
)

func Example() {
	qstream, err := quantiles.New(0.01, 1<<4)
	if err != nil {
		panic(err)
	}
	for i := 0.0; i < 1e6; i++ {
		if err := qstream.Push(i, 1.0); err != nil {
			panic(err)
		}
	}
	fmt.Print("ApproximationError:")
	fmt.Println(qstream.ApproximationError(1))

	fmt.Print("Finalize:")
	fmt.Println(qstream.Finalize())

	fmt.Print("GenerateQuantiles(4):")
	fmt.Println(qstream.GenerateQuantiles(4))

	fmt.Print("GenerateQuantiles(10):")
	fmt.Println(qstream.GenerateQuantiles(10))

	fmt.Print("GenerateQuantiles(4):")
	fmt.Println(qstream.GenerateQuantiles(4))

	sum, err := qstream.FinalSummary()
	if err != nil {
		panic(err)
	}
	fmt.Print("GenerateQuantiles(4):")
	fmt.Println(sum.GenerateQuantiles(4))

	// Output:
	// ApproximationError:0 <nil>
	// Finalize:<nil>
	// GenerateQuantiles(4):[0 251865 503730 746595 999999] <nil>
	// GenerateQuantiles(10):[0 98946 197892 296838 395789 503730 602676 701622 800568 899514 999999] <nil>
	// GenerateQuantiles(4):[0 251865 503730 746595 999999] <nil>
	// GenerateQuantiles(4):[0 251865 503730 746595 999999]
}
