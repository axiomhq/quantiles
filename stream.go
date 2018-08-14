package quantiles

import (
	"fmt"
	"math"
)

// Stream ...
type Stream struct {
	eps           float64
	maxLevels     int64
	blockSize     int64
	buffer        *buffer
	localSummary  *Summary
	summaryLevels []*Summary
	finalized     bool
}

// New ...
func New(eps float64, maxElements int64) (*Stream, error) {
	if eps <= 0 {
		return nil, fmt.Errorf("an epsilon value of zero is not allowed")
	}

	maxLevels, blockSize, err := getQuantileSpecs(eps, maxElements)
	if err != nil {
		return nil, err
	}

	buffer, err := newBuffer(blockSize, maxElements)
	if err != nil {
		return nil, err
	}

	stream := &Stream{
		eps:           eps,
		buffer:        buffer,
		finalized:     false,
		maxLevels:     maxLevels,
		blockSize:     blockSize,
		localSummary:  newSummary(),
		summaryLevels: []*Summary{},
	}
	return stream, nil
}

func (stream *Stream) clone() *Stream {
	newStream := &Stream{
		eps:           stream.eps,
		buffer:        stream.buffer.clone(),
		finalized:     stream.finalized,
		maxLevels:     stream.maxLevels,
		blockSize:     stream.blockSize,
		localSummary:  stream.localSummary.clone(),
		summaryLevels: stream.summaryLevels,
	}
	for i, sum := range stream.summaryLevels {
		newStream.summaryLevels[i] = sum.clone()
	}
	return newStream
}

// QuickQuantiles returns current quantiles without having a final state of the stream
func (stream *Stream) QuickQuantiles(numQuantiles int64) ([]float64, error) {
	tmpStream := stream.clone()
	if err := tmpStream.Finalize(); err != nil {
		return nil, err
	}
	return tmpStream.GenerateQuantiles(numQuantiles)
}

// Push a value and a weight into the stream
func (stream *Stream) Push(value float64, weight float64) error {
	// Validate state.
	var err error
	if stream.finalized {
		return fmt.Errorf("Finalize() already called")
	}

	if err = stream.buffer.push(value, weight); err != nil {
		return err
	}

	if stream.buffer.isFull() {
		err = stream.pushBuffer(stream.buffer)
	}
	return err
}

func (stream *Stream) pushBuffer(buf *buffer) error {
	// Validate state.
	if stream.finalized {
		return fmt.Errorf("Finalize() already called")
	}
	stream.localSummary.buildFromBufferEntries(buf.generateEntryList())
	stream.localSummary.compress(stream.blockSize, stream.eps)
	return stream.propagateLocalSummary()
}

// PushSummary pushes full summary while maintaining approximation error invariants.
func (stream *Stream) PushSummary(summary []SumEntry) error {
	// Validate state.
	if stream.finalized {
		return fmt.Errorf("Finalize() already called")
	}
	stream.localSummary.buildFromSummaryEntries(summary)
	//stream.localSummary.compress(stream.blockSize, stream.eps)
	return stream.propagateLocalSummary()
}

// Finalize flushes approximator and finalizes state.
func (stream *Stream) Finalize() error {
	// Validate state.
	if stream.finalized {
		return fmt.Errorf("Finalize() already called")
	}

	// Flush any remaining buffer elements.
	stream.pushBuffer(stream.buffer)

	// Create final merged summary
	stream.localSummary.Clear()
	for _, summary := range stream.summaryLevels {
		stream.localSummary.Merge(summary)
	}

	stream.summaryLevels = []*Summary{}
	stream.finalized = true
	return nil
}

/*
propagates local summary through summary levels while maintaining
approximation error invariants.
*/
func (stream *Stream) propagateLocalSummary() error {
	// Validate state.
	if stream.finalized {
		return fmt.Errorf("Finalize() already called")
	}

	// No-op if there's nothing to add.
	if stream.localSummary.Size() <= 0 {
		return nil
	}

	for level, settled := int64(0), false; !settled; level++ {
		// Ensure we have enough depth.
		if int64(len(stream.summaryLevels)) <= level {
			stream.summaryLevels = append(stream.summaryLevels, &Summary{})
		}

		// Merge summaries.
		currentSummary := stream.summaryLevels[level]
		stream.localSummary.Merge(currentSummary)

		// Check if we need to compress and propagate summary higher.
		if currentSummary.Size() == 0 ||
			stream.localSummary.Size() <= stream.blockSize+1 {
			*currentSummary = *(stream.localSummary)
			stream.localSummary = newSummary()
			settled = true
		} else {
			// Compress, empty current level and propagate.
			stream.localSummary.compress(stream.blockSize, stream.eps)
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
func (stream *Stream) GenerateQuantiles(numQuantiles int64) ([]float64, error) {
	if !stream.finalized {
		return nil, fmt.Errorf("Finalize() must be called before generating quantiles")
	}
	return stream.localSummary.GenerateQuantiles(numQuantiles), nil
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
func (stream *Stream) GenerateBoundaries(numBoundaries int64) ([]float64, error) {
	if !stream.finalized {
		return nil, fmt.Errorf("Finalize() must be called before generating quantiles")
	}
	return stream.localSummary.GenerateBoundaries(numBoundaries), nil
}

/*
ApproximationError calculates approximation error for the specified level.
If the passed level is negative, the approximation error for the entire
summary is returned. Note that after Finalize is called, only the overall
error is available.
*/
func (stream *Stream) ApproximationError(level int64) (float64, error) {
	if stream.finalized {
		if level > 0 {
			return 0, fmt.Errorf("only overall error is available after Finalize()")
		}
		return stream.localSummary.ApproximationError(), nil
	}

	if len(stream.summaryLevels) == 0 {
		// No error even if base buffer isn't empty.
		return 0, nil
	}

	// If level is negative, we get the approximation error
	// for the top-most level which is the max approximation error
	// in all summaries by construction.
	if level < 0 {
		level = int64(len(stream.summaryLevels)) - 1
	}
	if level >= int64(len(stream.summaryLevels)) {
		return 0, fmt.Errorf("invalid level")
	}
	return stream.summaryLevels[level].ApproximationError(), nil
}

// MaxDepth ...
func (stream *Stream) MaxDepth() int {
	return len(stream.summaryLevels)
}

// FinalSummary ...
func (stream *Stream) FinalSummary() (*Summary, error) {
	if !stream.finalized {
		return nil, fmt.Errorf("Finalize() must be called before generating quantiles")
	}
	return stream.localSummary, nil
}

/*
Helper method which, given the desired approximation error
and an upper bound on the number of elements, computes the optimal
number of levels and block size and returns them in the tuple.
*/
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
