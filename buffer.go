package quantiles

import (
	"fmt"
	"sort"
)

// BufferEntry ...
type BufferEntry struct {
	value  float64
	weight float64
}

// LessThan ...
func (be BufferEntry) LessThan(o BufferEntry) bool {
	return be.value < o.value
}

// Equals ...
func (be *BufferEntry) Equals(o *BufferEntry) bool {
	return be.value == o.value && be.weight == o.weight
}

// WeightedQuantilesBuffer ...
type WeightedQuantilesBuffer struct {
	vec     []BufferEntry
	maxSize int64
}

// NewWeightedQuantilesBuffer ...
func NewWeightedQuantilesBuffer(blockSize, maxElements int64) (*WeightedQuantilesBuffer, error) {
	maxSize := blockSize << 1
	if maxSize > maxElements {
		maxSize = maxElements
	}

	if maxSize <= 0 {
		return nil, fmt.Errorf("Invalid buffer specification: (%v, %v)", blockSize, maxElements)
	}

	return &WeightedQuantilesBuffer{
		maxSize: maxSize,
		vec:     make([]BufferEntry, 0),
	}, nil
}

// PushEntry ...
func (wqb *WeightedQuantilesBuffer) PushEntry(value, weight float64) error {
	//QCHECK magic
	if wqb.IsFull() {
		return fmt.Errorf("Buffer already full: %v", wqb.maxSize)
	}

	if weight > 0 {
		wqb.vec = append(wqb.vec, BufferEntry{value, weight})
	}
	return nil
}

// GenerateEntryList returns a sorted vector view of the base buffer and clears the buffer.
// Callers should minimize how often this is called, ideally only right after
// the buffer becomes full.
func (wqb *WeightedQuantilesBuffer) GenerateEntryList() []BufferEntry {
	sort.Slice(wqb.vec, func(i, j int) bool { return wqb.vec[i].LessThan(wqb.vec[j]) })
	ret := make([]BufferEntry, len(wqb.vec), len(wqb.vec))
	for i, val := range wqb.vec {
		ret[i] = val
	}
	wqb.vec = []BufferEntry{}

	numEntries := 0
	for i := 1; i < len(ret); i++ {
		if ret[i].value != ret[i-1].value {
			tmp := ret[i]
			numEntries++
			ret[numEntries] = tmp
		} else {
			ret[numEntries].weight += ret[i].weight
		}
	}
	if numEntries == 0 {
		return ret
	}
	return ret[:numEntries+1]
}

// Size ...
func (wqb *WeightedQuantilesBuffer) Size() int {
	return len(wqb.vec)
}

// IsFull ...
func (wqb *WeightedQuantilesBuffer) IsFull() bool {
	return int64(len(wqb.vec)) >= wqb.maxSize
}

// Clear ...
func (wqb *WeightedQuantilesBuffer) Clear() {
	wqb.vec = make([]BufferEntry, 0)
}
