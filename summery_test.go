package quantiles

import "testing"

type WeightedQuantilesSummaryDummy struct {
	buffer1 *WeightedQuantilesBuffer
	buffer2 *WeightedQuantilesBuffer

	buffer1MinValue    float64
	buffer1MaxValue    float64
	buffer1TotalWeight float64

	buffer2MinValue    float64
	buffer2MaxValue    float64
	buffer2TotalWeight float64

	*WeightedQuantilesSummary
}

func NewWeightedQuantilesSummaryDummy() (*WeightedQuantilesSummaryDummy, error) {
	sum := &WeightedQuantilesSummary{
		entries: make([]*SummaryEntry, 0),
	}
	wqsd := &WeightedQuantilesSummaryDummy{
		WeightedQuantilesSummary: sum,
		buffer1MinValue:          -13,
		buffer1MaxValue:          21,
		buffer1TotalWeight:       45,
		buffer2MinValue:          -7,
		buffer2MaxValue:          11,
		buffer2TotalWeight:       30,
	}
	if err := wqsd.setup(); err != nil {
		return nil, err
	}
	return wqsd, nil
}

func (wqsd *WeightedQuantilesSummaryDummy) setup() error {
	var err error
	wqsd.buffer1, err = NewWeightedQuantilesBuffer(10, 1000)
	if err != nil {
		return err
	}
	for _, val := range [][2]float64{
		[2]float64{5, 9},
		[2]float64{2, 3},
		[2]float64{-1, 7},
		[2]float64{-7, 1},
		[2]float64{3, 2},
		[2]float64{-2, 3},
		[2]float64{21, 8},
		[2]float64{-13, 4},
		[2]float64{8, 2},
		[2]float64{-5, 6},
	} {
		if err := wqsd.buffer1.PushEntry(val[0], val[1]); err != nil {
			return err
		}
	}

	wqsd.buffer2, err = NewWeightedQuantilesBuffer(7, 1000)
	if err != nil {
		return err
	}
	for _, val := range [][2]float64{
		[2]float64{9, 2},
		[2]float64{-7, 3},
		[2]float64{2, 1},
		[2]float64{4, 13},
		[2]float64{0, 5},
		[2]float64{-5, 3},
		[2]float64{11, 3},
	} {
		if err := wqsd.buffer2.PushEntry(val[0], val[1]); err != nil {
			return err
		}
	}
	return nil
}

func TestBuildFromBuffer(t *testing.T) {
	wqsd, err := NewWeightedQuantilesSummaryDummy()
	if err != nil {
		t.Error(err)
	}
	sum := &WeightedQuantilesSummary{
		entries: make([]*SummaryEntry, 0),
	}
	sum.BuildFromBufferEntries(wqsd.buffer1.GenerateEntryList())

	// We expect no approximation error because no compress operation occurred.
	if approx := sum.ApproximationError(); approx != 0 {
		t.Error("expected no approximation error, got", approx)
	}
}
