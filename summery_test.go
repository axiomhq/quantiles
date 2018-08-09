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
	sum := &WeightedQuantilesSummary{}
	sum.BuildFromBufferEntries(wqsd.buffer1.GenerateEntryList())

	// We expect no approximation error because no compress operation occurred.
	if approx := sum.ApproximationError(); approx != 0 {
		t.Error("expected no approximation error, got", approx)
	}

	entries := sum.entries

	// First element's rmin should be zero.
	// EXPECT_EQ(summary.MinValue(), buffer1_min_value_)
	if val := sum.MinValue(); val != wqsd.buffer1MinValue {
		t.Error("first element's rmin should be zero, got", val)
	}
	// EXPECT_EQ(entries.front(), SummaryEntry(-13, 4, 0, 4))
	exp := SummaryEntry{
		value: -13, weight: 4, minRank: 0, maxRank: 4,
	}
	if val := entries[0]; *val != exp {
		t.Errorf("expected %v, got %v", exp, val)
	}

	// Last element's rmax should be cumulative weight.
	// EXPECT_EQ(summary.MaxValue(), buffer1_max_value_)
	if val := sum.MaxValue(); val != wqsd.buffer1MaxValue {
		t.Errorf("expected %v, got %v", wqsd.buffer1MaxValue, val)
	}

	//EXPECT_EQ(entries.back(), SummaryEntry(21, 8, 37, 45))
	exp = SummaryEntry{
		value: 21, weight: 8, minRank: 37, maxRank: 45,
	}
	if val := entries[len(entries)-1]; *val != exp {
		t.Errorf("expected %v, got %v", exp, val)
	}

	// Check total weight.
	// EXPECT_EQ(summary.TotalWeight(), buffer1_total_weight_)
	if val := sum.TotalWeight(); val != wqsd.buffer1TotalWeight {
		t.Errorf("expected %v, got %v", wqsd.buffer1TotalWeight, val)
	}
}

func TestCompressSeparately(t *testing.T) {
	wqsd, err := NewWeightedQuantilesSummaryDummy()
	if err != nil {
		t.Error(err)
	}
	entryList := wqsd.buffer1.GenerateEntryList()
	for newSize := int64(9); newSize >= 2; newSize-- {
		sum := &WeightedQuantilesSummary{}
		sum.BuildFromBufferEntries(entryList)
		sum.Compress(newSize, 0)

		// Expect a max approximation error of 1 / n
		// ie. eps0 + 1/n but eps0 = 0.

		// EXPECT_TRUE(summary.Size() >= new_size && summary.Size() <= new_size + 2);
		if val := sum.Size(); val < newSize {
			t.Errorf("expected val >= newSize, got %v < %v", val, newSize)
		} else if val > newSize+2 {
			t.Errorf("expected val <= newSize+2, got %v > %v", val, newSize+2)
		}

		// EXPECT_LE(summary.ApproximationError(), 1.0 / new_size);
		if approx := sum.ApproximationError(); approx > 1.0/float64(newSize) {
			t.Errorf("expected approx <= newSize, got %v > %v", approx, 1.0/float64(newSize))
		}

		// Min/Max elements and total weight should not change.
		// EXPECT_EQ(summary.MinValue(), buffer1_min_value_)
		if sum.MinValue() != wqsd.buffer1MinValue {
			t.Errorf("expected %v, got %v", wqsd.buffer1MinValue, sum.MinValue())
		}
		// EXPECT_EQ(summary.MaxValue(), buffer1_max_value_)
		if sum.MaxValue() != wqsd.buffer1MaxValue {
			t.Errorf("expected %v, got %v", wqsd.buffer1MaxValue, sum.MaxValue())
		}
		// EXPECT_EQ(summary.TotalWeight(), buffer1_total_weight_)
		if sum.TotalWeight() != wqsd.buffer1TotalWeight {
			t.Errorf("expected %v, got %v", wqsd.buffer1TotalWeight, sum.TotalWeight())
		}
	}
}
