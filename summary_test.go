package quantiles

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

type SummaryDummy struct {
	buffer1 *buffer
	buffer2 *buffer

	buffer1MinValue    float64
	buffer1MaxValue    float64
	buffer1TotalWeight float64

	buffer2MinValue    float64
	buffer2MaxValue    float64
	buffer2TotalWeight float64

	*Summary
}

func NewWeightedQuantilesSummaryDummy() (*SummaryDummy, error) {
	sum := &Summary{
		entries: make([]SumEntry, 0),
	}
	wqsd := &SummaryDummy{
		Summary:            sum,
		buffer1MinValue:    -13,
		buffer1MaxValue:    21,
		buffer1TotalWeight: 45,
		buffer2MinValue:    -7,
		buffer2MaxValue:    11,
		buffer2TotalWeight: 30,
	}
	if err := wqsd.setup(); err != nil {
		return nil, err
	}
	return wqsd, nil
}

func (wqsd *SummaryDummy) setup() error {
	var err error
	wqsd.buffer1, err = newBuffer(10, 1000)
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
		if err := wqsd.buffer1.push(val[0], val[1]); err != nil {
			return err
		}
	}

	wqsd.buffer2, err = newBuffer(7, 1000)
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
		if err := wqsd.buffer2.push(val[0], val[1]); err != nil {
			return err
		}
	}
	return nil
}

func TestSummaryBuildFromBuffer(t *testing.T) {
	wqsd, err := NewWeightedQuantilesSummaryDummy()
	if err != nil {
		t.Error(err)
	}
	sum := &Summary{}
	sum.buildFromBufferEntries(wqsd.buffer1.generateEntryList())

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
	exp := SumEntry{
		Value: -13, Weight: 4, MinRank: 0, MaxRank: 4,
	}
	if val := entries[0]; val != exp {
		t.Errorf("expected %v, got %v", exp, val)
	}

	// Last element's rmax should be cumulative weight.
	// EXPECT_EQ(summary.MaxValue(), buffer1_max_value_)
	if val := sum.MaxValue(); val != wqsd.buffer1MaxValue {
		t.Errorf("expected %v, got %v", wqsd.buffer1MaxValue, val)
	}

	//EXPECT_EQ(entries.back(), SummaryEntry(21, 8, 37, 45))
	exp = SumEntry{
		Value: 21, Weight: 8, MinRank: 37, MaxRank: 45,
	}
	if val := entries[len(entries)-1]; val != exp {
		t.Errorf("expected %v, got %v", exp, val)
	}

	// Check total weight.
	// EXPECT_EQ(summary.TotalWeight(), buffer1_total_weight_)
	if val := sum.TotalWeight(); val != wqsd.buffer1TotalWeight {
		t.Errorf("expected %v, got %v", wqsd.buffer1TotalWeight, val)
	}
}

func TestSummaryCompressSeparately(t *testing.T) {
	wqsd, err := NewWeightedQuantilesSummaryDummy()
	if err != nil {
		t.Error(err)
	}
	entryList := wqsd.buffer1.generateEntryList()
	for newSize := int64(9); newSize >= 2; newSize-- {
		sum := &Summary{}
		sum.buildFromBufferEntries(entryList)
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
func TestSummaryCompressSequentially(t *testing.T) {
	wqsd, err := NewWeightedQuantilesSummaryDummy()
	if err != nil {
		t.Error(err)
	}
	entryList := wqsd.buffer1.generateEntryList()
	sum := &Summary{}
	sum.buildFromBufferEntries(entryList)
	for newSize := int64(9); newSize >= 2; newSize -= 2 {

		prevEps := sum.ApproximationError()
		sum.Compress(newSize, 0)

		// Expect a max approximation error of prev_eps + 1 / n.

		// EXPECT_TRUE(summary.Size() >= new_size && summary.Size() <= new_size + 2);
		if val := sum.Size(); val < newSize {
			t.Errorf("expected val >= newSize, got %v < %v", val, newSize)
		} else if val > newSize+2 {
			t.Errorf("expected val <= newSize+2, got %v > %v", val, newSize+2)
		}

		// EXPECT_LE(summary.ApproximationError(), 1.0 / new_size);
		if approx := sum.ApproximationError(); approx > prevEps+1.0/float64(newSize) {
			t.Errorf("expected approx <= newSize, got %v > %v", approx, prevEps+1.0/float64(newSize))
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

func TestSummaryCompressRandomized(t *testing.T) {
	var (
		prevSize int64 = 1
		size     int64 = 2
		maxValue       = float64(1 << 20)
	)

	for size < (1 << 16) {
		buffer, err := newBuffer(size, size<<4)
		if err != nil {
			t.Error("expected no error, got", err)
		}
		for i := int64(0); i < size; i++ {
			buffer.push(
				rand.Float64()*maxValue,
				rand.Float64()*maxValue,
			)
		}

		sum := &Summary{}
		sum.buildFromBufferEntries(buffer.generateEntryList())
		newSize := maxInt64(rand.Int63n(size), 2)
		sum.Compress(newSize, 0)

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

		lastSize := size
		size += prevSize
		prevSize = lastSize
	}
}

func TestSummaryMergeSymmetry(t *testing.T) {
	assert := assert.New(t)

	wqsd, err := NewWeightedQuantilesSummaryDummy()
	if err != nil {
		t.Error(err)
	}

	list1 := wqsd.buffer1.generateEntryList()
	list2 := wqsd.buffer2.generateEntryList()
	sum1 := &Summary{}
	sum1.buildFromBufferEntries(list1)
	sum2 := &Summary{}
	sum2.buildFromBufferEntries(list2)

	sum1.Merge(sum2)
	assert.Equal(sum1.ApproximationError(), 0.0)
	assert.Equal(sum1.MinValue(),
		minFloat64(wqsd.buffer1MinValue, wqsd.buffer2MinValue))

	assert.Equal(sum1.MaxValue(),
		maxFloat64(wqsd.buffer1MaxValue, wqsd.buffer2MaxValue))
	assert.Equal(sum1.TotalWeight(),
		wqsd.buffer1TotalWeight+wqsd.buffer2TotalWeight)
	assert.Equal(sum1.Size(), int64(14))

	sum1.buildFromBufferEntries(list1)
	sum2.Merge(sum1)
	assert.Equal(sum2.ApproximationError(), 0.0)
	assert.Equal(sum2.MinValue(),
		minFloat64(wqsd.buffer1MinValue, wqsd.buffer2MinValue))
	assert.Equal(sum2.MaxValue(),
		maxFloat64(wqsd.buffer1MaxValue, wqsd.buffer2MaxValue))
	assert.Equal(sum2.TotalWeight(),
		wqsd.buffer1TotalWeight+wqsd.buffer2TotalWeight)
	assert.Equal(sum2.Size(), int64(14))
}

func TestSummaryCompressThenMerge(t *testing.T) {
	assert := assert.New(t)
	wqsd, err := NewWeightedQuantilesSummaryDummy()
	if err != nil {
		t.Error(err)
	}

	sum1 := &Summary{}
	sum1.buildFromBufferEntries(wqsd.buffer1.generateEntryList())
	sum2 := &Summary{}
	sum2.buildFromBufferEntries(wqsd.buffer2.generateEntryList())

	sum1.Compress(5, 0)
	eps1 := 1.0 / 5
	// EXPECT_LE(summary.ApproximationError(), 1.0 / new_size);
	if approx := sum1.ApproximationError(); approx > eps1 {
		t.Errorf("expected approx <= newSize, got %v > %v", approx, eps1)
	}
	sum2.Compress(3, 0)
	eps2 := 1.0 / 3
	// EXPECT_LE(summary.ApproximationError(), 1.0 / new_size);
	if approx := sum1.ApproximationError(); approx > eps1 {
		t.Errorf("expected approx <= newSize, got %v > %v", approx, eps2)
	}

	// Merge guarantees an approximation error of max(eps1, eps2).
	// Merge summary 2 into 1 and verify.
	sum1.Merge(sum2)
	if approx := sum1.ApproximationError(); approx > maxFloat64(eps1, eps2) {
		t.Errorf("expected approx <= newSize, got %v > %v", approx, maxFloat64(eps1, eps2))
	}
	assert.Equal(sum1.MinValue(),
		minFloat64(wqsd.buffer1MinValue, wqsd.buffer2MinValue))
	assert.Equal(sum1.MaxValue(),
		maxFloat64(wqsd.buffer1MaxValue, wqsd.buffer2MaxValue))
	assert.Equal(sum1.TotalWeight(),
		wqsd.buffer1TotalWeight+wqsd.buffer2TotalWeight)
}
