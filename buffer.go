package quantiles

import (
	"fmt"
	"sort"
)

// bufEntry ...
type bufEntry struct {
	value  float64
	weight float64
}

type buffer struct {
	vec     []bufEntry
	maxSize int64
}

func newBuffer(blockSize, maxElements int64) (*buffer, error) {
	maxSize := blockSize << 1
	if maxSize > maxElements {
		maxSize = maxElements
	}

	if maxSize <= 0 {
		return nil, fmt.Errorf("Invalid buffer specification: (%v, %v)", blockSize, maxElements)
	}

	return &buffer{
		maxSize: maxSize,
		vec:     make([]bufEntry, 0),
	}, nil
}

func (buf *buffer) clone() *buffer {
	newBuffer := *buf
	return &newBuffer
}

func (buf *buffer) push(value, weight float64) error {
	//QCHECK magic
	if buf.isFull() {
		return fmt.Errorf("Buffer already full: %v", buf.maxSize)
	}

	if weight > 0 {
		buf.vec = append(buf.vec, bufEntry{value, weight})
	}
	return nil
}

// generateEntryList returns a sorted vector view of the base buffer and clears the buffer.
// Callers should minimize how often this is called, ideally only right after
// the buffer becomes full.
func (buf *buffer) generateEntryList() []bufEntry {
	sort.Slice(buf.vec, func(i, j int) bool { return buf.vec[i].value < buf.vec[j].value })
	ret := make([]bufEntry, len(buf.vec), len(buf.vec))
	for i, val := range buf.vec {
		ret[i] = val
	}
	buf.vec = []bufEntry{}

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

// isFull ...
func (buf *buffer) isFull() bool {
	return int64(len(buf.vec)) >= buf.maxSize
}
