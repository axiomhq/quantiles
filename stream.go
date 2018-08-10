package quantiles

import (
	"fmt"
	"math"
)

// WeightedQuantilesStream ...
type WeightedQuantilesStream struct {
	eps           float64
	maxLevels     int64
	blockSize     int64
	buffer        *WeightedQuantilesBuffer
	localSummary  *WeightedQuantilesSummary
	summaryLevels []*WeightedQuantilesSummary
	finalized     bool
}

// NewWeightedQuantilesStream ...
func NewWeightedQuantilesStream(eps float64, maxElements int64) (*WeightedQuantilesStream, error) {
	if eps <= 0 {
		return nil, fmt.Errorf("an epsilon value of zero is not allowed")
	}

	maxLevels, blockSize, err := getQuantileSpecs(eps, maxElements)
	if err != nil {
		return nil, err
	}

	buffer, err := NewWeightedQuantilesBuffer(blockSize, maxLevels)
	if err != nil {
		return nil, err
	}

	stream := &WeightedQuantilesStream{
		eps:           eps,
		buffer:        buffer,
		finalized:     false,
		maxLevels:     maxLevels,
		blockSize:     blockSize,
		localSummary:  NewWeightedQuantilesSummary(),
		summaryLevels: []*WeightedQuantilesSummary{},
	}
	return stream, nil
}

// PushEntry ...
func (wqs *WeightedQuantilesStream) PushEntry(value float64, weight float64) error {
	// Validate state.
	var err error
	if wqs.finalized {
		return fmt.Errorf("Finalize() already called")
	}

	if err = wqs.buffer.PushEntry(value, weight); err != nil {
		return err
	}

	if wqs.buffer.IsFull() {
		fmt.Println(">>>")
		err = wqs.PushBuffer(wqs.buffer)
	}
	return err
}

// PushBuffer ...
func (wqs *WeightedQuantilesStream) PushBuffer(buffer *WeightedQuantilesBuffer) error {
	// Validate state.
	if wqs.finalized {
		return fmt.Errorf("Finalize() already called")
	}
	wqs.localSummary.BuildFromBufferEntries(wqs.buffer.GenerateEntryList())
	wqs.localSummary.Compress(wqs.blockSize, wqs.eps)
	return wqs.propagateLocalSummary()
}

// PushSummary pushes full summary while maintaining approximation error invariants.
func (wqs *WeightedQuantilesStream) PushSummary(summary []*SummaryEntry) error {
	// Validate state.
	if wqs.finalized {
		return fmt.Errorf("Finalize() already called")
	}
	wqs.localSummary.BuildFromSummaryEntries(summary)
	wqs.localSummary.Compress(wqs.blockSize, wqs.eps)
	return wqs.propagateLocalSummary()
}

// Finalize flushes approximator and finalizes state.
func (wqs *WeightedQuantilesStream) Finalize() error {
	// Validate state.
	if wqs.finalized {
		return fmt.Errorf("Finalize() already called")
	}

	// Flush any remaining buffer elements.
	wqs.PushBuffer(wqs.buffer)

	// Create final merged summary
	wqs.localSummary.Clear()
	for _, summary := range wqs.summaryLevels {
		wqs.localSummary.Merge(summary)
	}

	wqs.summaryLevels = []*WeightedQuantilesSummary{}
	wqs.finalized = true
	return nil
}

/*
propagates local summary through summary levels while maintaining
approximation error invariants.
*/
func (wqs *WeightedQuantilesStream) propagateLocalSummary() error {
	// Validate state.
	if wqs.finalized {
		return fmt.Errorf("Finalize() already called")
	}

	// No-op if there's nothing to add.
	if wqs.localSummary.Size() <= 0 {
		return nil
	}

	var level int64
	for settled := false; !settled; level++ {
		// Ensure we have enough depth.
		if int64(len(wqs.summaryLevels)) <= level {
			wqs.summaryLevels = append(wqs.summaryLevels, &WeightedQuantilesSummary{})
		}

		// Merge summaries.
		currentSummary := wqs.summaryLevels[level]
		wqs.localSummary.Merge(currentSummary)

		// Check if we need to compress and propagate summary higher.
		if currentSummary.Size() == 0 ||
			wqs.localSummary.Size() <= wqs.blockSize+1 {
			fmt.Println(wqs.localSummary, currentSummary)
			*currentSummary = *(wqs.localSummary)
			wqs.localSummary = NewWeightedQuantilesSummary()
			settled = true

		} else {
			// Compress, empty current level and propagate.
			wqs.localSummary.Compress(wqs.blockSize, wqs.eps)
			currentSummary.Clear()
		}
	}
	return nil
}

/*
GenerateQuantiles generates requested number of quantiles after finalizing stream.
The returned quantiles can be queried using std::lower_bound to get
the bucket for a given value.
*/
func (wqs *WeightedQuantilesStream) GenerateQuantiles(numQuantiles int64) ([]float64, error) {
	if !wqs.finalized {
		return nil, fmt.Errorf("Finalize() must be called before generating quantiles")
	}
	return wqs.localSummary.GenerateQuantiles(numQuantiles), nil
}

/*
GenerateBoundaries generates requested number of boundaries after finalizing stream.
The returned boundaries can be queried using std::lower_bound to get
the bucket for a given value.
The boundaries, while still guaranteeing approximation bounds, don't
necessarily represent the actual quantiles of the distribution.
Boundaries are preferable over quantiles when the caller is less
interested in the actual quantiles distribution and more interested in
getting a representative sample of boundary values.
*/
func (wqs *WeightedQuantilesStream) GenerateBoundaries(numBoundaries int64) ([]float64, error) {
	if !wqs.finalized {
		return nil, fmt.Errorf("Finalize() must be called before generating quantiles")
	}
	return wqs.localSummary.GenerateBoundaries(numBoundaries), nil
}

/*
ApproximationError calculates approximation error for the specified level.
If the passed level is negative, the approximation error for the entire
summary is returned. Note that after Finalize is called, only the overall
error is available.
*/
func (wqs *WeightedQuantilesStream) ApproximationError(level int64) (float64, error) {
	if wqs.finalized {
		if level > 0 {
			return 0, fmt.Errorf("only overall error is available after Finalize()")
		}
		return wqs.localSummary.ApproximationError(), nil
	}

	if len(wqs.summaryLevels) == 0 {
		// No error even if base buffer isn't empty.
		return 0, nil
	}

	// If level is negative, we get the approximation error
	// for the top-most level which is the max approximation error
	// in all summaries by construction.
	if level < 0 {
		level = int64(len(wqs.summaryLevels)) - 1
	}
	if level >= int64(len(wqs.summaryLevels)) {
		return 0, fmt.Errorf("invalid level")
	}
	return wqs.summaryLevels[level].ApproximationError(), nil
}

// MaxDepth ...
func (wqs *WeightedQuantilesStream) MaxDepth() int {
	return len(wqs.summaryLevels)
}

// GetFinalSummary ...
func (wqs *WeightedQuantilesStream) GetFinalSummary() (*WeightedQuantilesSummary, error) {
	if !wqs.finalized {
		return nil, fmt.Errorf("Finalize() must be called before generating quantiles")
	}
	return wqs.localSummary, nil
}

// GetQuantileSpecs ...
func getQuantileSpecs(eps float64, maxElements int64) (int64, int64, error) {
	var (
		maxLevel  int64 = 1
		blockSize int64 = 2
	)
	if eps < 0 || eps >= 1 {
		return maxLevel, blockSize, fmt.Errorf("eps should be element of [0, 1)")
	}
	if maxElements <= 0 {
		return maxLevel, blockSize, fmt.Errorf("maxElements should be > 0")
	}

	if eps <= math.SmallestNonzeroFloat64 {
		// Exact quantile computation at the expense of RAM.
		maxLevel = 1
		blockSize = maxInt64(maxElements, 2)
	} else {
		// The bottom-most level will become full at most
		// (max_elements / block_size) times, the level above will become full
		// (max_elements / 2 * block_size) times and generally level l becomes
		// full (max_elements / 2^l * block_size) times until the last
		// level max_level becomes full at most once meaning when the inequality
		// (2^max_level * block_size >= max_elements) is satisfied.
		// In what follows, we jointly solve for max_level and block_size by
		// gradually increasing the level until the inequality above is satisfied.
		// We could alternatively set max_level = ceil(log2(eps * max_elements));
		// and block_size = ceil(max_level / eps) + 1 but that tends to give more
		// pessimistic bounds and wastes RAM needlessly.

		blockSize = 2
		for maxLevel = 1; (uint64(1)<<uint64(maxLevel))*uint64(blockSize) < uint64(maxElements); maxLevel++ {
			// Update upper bound on block size at current level, we always
			// increase the estimate by 2 to hold the min/max elements seen so far.
			blockSize = int64(math.Ceil(float64(maxLevel)/eps) + 1)
		}
	}
	return maxLevel, maxInt64(blockSize, 2), nil
}
