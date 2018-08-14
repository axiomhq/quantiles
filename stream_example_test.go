package quantiles_test

import (
	"fmt"

	"github.com/axiomhq/quantiles"
)

func Example() {
	sketch := quantiles.NewDefault()
	for i := 0.0; i < 1e6; i++ {
		if err := sketch.Push(i, 1.0); err != nil {
			panic(err)
		}
	}
	fmt.Print("ApproximationError:")
	fmt.Println(sketch.ApproximationError(1))

	fmt.Print("Finalize:")
	fmt.Println(sketch.Finalize())

	fmt.Print("GenerateQuantiles(4):")
	fmt.Println(sketch.GenerateQuantiles(4))

	fmt.Print("GenerateQuantiles(10):")
	fmt.Println(sketch.GenerateQuantiles(10))

	sum, err := sketch.FinalSummary()
	if err != nil {
		panic(err)
	}
	fmt.Print("GenerateQuantiles(4):")
	fmt.Println(sum.GenerateQuantiles(4))

	// Output:
	// ApproximationError:0.006218905472636816 <nil>
	// Finalize:<nil>
	// GenerateQuantiles(4):[0 249854 499710 749566 999999] <nil>
	// GenerateQuantiles(10):[0 98302 200702 299006 401406 499710 598014 700414 798718 900094 999999] <nil>
	// GenerateQuantiles(4):[0 249854 499710 749566 999999]
}
