package quantiles

import (
	"fmt"
	"sort"
)

// byValue implements sort.Interface based on the value field.
type byValue []bufEntry

func (a byValue) Len() int           { return len(a) }
func (a byValue) Less(i, j int) bool { return a[i].value < a[j].value }
func (a byValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// bufEntry ...
type bufEntry struct {
	value  float64
	weight float64
}

type buffer struct {
	vec     byValue
	maxSize int64
	curSize int64
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
		curSize: 0,
		vec:     make([]bufEntry, maxSize),
	}, nil
}

func (buf *buffer) clone() *buffer {
	newBuffer := &buffer{
		maxSize: buf.maxSize,
		curSize: buf.curSize,
		vec:     make([]bufEntry, buf.maxSize),
	}
	for i, e := range buf.vec {
		newBuffer.vec[i] = e
	}
	return newBuffer
}

func (buf *buffer) push(value, weight float64) error {
	//QCHECK magic
	if buf.isFull() {
		return fmt.Errorf("Buffer already full: %v", buf.maxSize)
	}

	if weight > 0 {
		buf.vec[buf.curSize] = bufEntry{value, weight}
		buf.curSize++
	}
	return nil
}

// generateEntryList returns a sorted vector view of the base buffer and clears the buffer.
// Callers should minimize how often this is called, ideally only right after
// the buffer becomes full.
func (buf *buffer) generateEntryList() []bufEntry {
	sort.Sort(buf.vec[:buf.curSize])
	ret := buf.vec[:buf.curSize]
	buf.vec = make([]bufEntry, buf.maxSize)
	buf.curSize = 0

	numEntries := 0
	for i := 1; i < len(ret); i++ {
		if ret[i].value != ret[i-1].value {
			numEntries++
			ret[numEntries] = ret[i]
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
	return buf.curSize >= buf.maxSize
}
