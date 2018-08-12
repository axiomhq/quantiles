package quantiles

import (
	"reflect"
	"testing"
)

func TestBufferInvalid(t *testing.T) {
	if _, err := newBuffer(2, 0); err == nil {
		t.Error("expected error, got nil")
	}
	if _, err := newBuffer(0, 2); err == nil {
		t.Error("expected error, got nil")
	}
}

func TestBufferPushEntryNotFull(t *testing.T) {
	buf, err := newBuffer(2, 100)
	if err != nil {
		t.Error("expected no err, got", err)
	}
	buf.push(5, 9)
	buf.push(2, 3)
	buf.push(-1, 7)
	buf.push(3, 0)

	if buf.isFull() {
		t.Error("expected not full, got full")
	}
	if val := len(buf.vec); val == 2 {
		t.Error("expected 3, got full", val)
	}
}

func TestBufferPushEntryFull(t *testing.T) {
	buf, err := newBuffer(2, 100)
	if err != nil {
		t.Error("expected no err, got", err)
	}
	buf.push(5, 9)
	buf.push(2, 3)
	buf.push(-1, 7)
	buf.push(2, 1)

	expected := []bufEntry{}
	expected = append(expected, bufEntry{-1, 7})
	expected = append(expected, bufEntry{2, 4})
	expected = append(expected, bufEntry{5, 9})

	if !buf.isFull() {
		t.Error("expected full, got not full")
	}
	if got := buf.generateEntryList(); !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}
func TestBufferPushEntryFullDeath(t *testing.T) {
	buf, err := newBuffer(2, 100)
	if err != nil {
		t.Error("expected no err, got", err)
	}
	buf.push(5, 9)
	buf.push(2, 3)
	buf.push(-1, 7)
	buf.push(2, 1)

	expected := []bufEntry{}
	expected = append(expected, bufEntry{-1, 7})
	expected = append(expected, bufEntry{2, 4})
	expected = append(expected, bufEntry{5, 9})

	if !buf.isFull() {
		t.Error("expected full, got not full")
	}
	if err := buf.push(6, 6); err == nil {
		t.Error("expected buffer already full")
	}
}
