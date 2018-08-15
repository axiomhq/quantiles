package main

import (
	"fmt"
	"time"

	"github.com/axiomhq/quantiles"
	"github.com/beorn7/perks/quantile"
	"github.com/stripe/veneur/tdigest"
)

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func veneur() {
	t := tdigest.NewMerging(20, false)
	now := time.Now()
	for i := 0.0; i < 1e6; i++ {
		t.Add(i, 1.0)
	}
	fmt.Println("veneur:", time.Since(now))
}

func axiom() {
	qstream, _ := quantiles.New(0.01, 1000)
	now := time.Now()
	for i := 0.0; i < 1e6; i++ {
		if err := qstream.Push(i, 1.0); err != nil {
			panic(err)
		}
	}
	fmt.Println("axiom:", time.Since(now))
}

func prom() {
	tstream := quantile.NewLowBiased(0.01)
	now := time.Now()
	for i := 0.0; i < 1e6; i++ {
		tstream.Insert(i)
	}
	fmt.Println("prometheus:", time.Since(now))
}

func main() {
	veneur()
	prom()
	axiom()
}
