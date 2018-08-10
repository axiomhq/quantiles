package quantiles

import (
	"reflect"
	"testing"
)

func TestBufferInvalid(t *testing.T) {
	if _, err := NewWeightedQuantilesBuffer(2, 0); err == nil {
		t.Error("expected error, got nil")
	}
	if _, err := NewWeightedQuantilesBuffer(0, 2); err == nil {
		t.Error("expected error, got nil")
	}
}

func TestBufferPushEntryNotFull(t *testing.T) {
	buf, err := NewWeightedQuantilesBuffer(2, 100)
	if err != nil {
		t.Error("expected no err, got", err)
	}
	buf.PushEntry(5, 9)
	buf.PushEntry(2, 3)
	buf.PushEntry(-1, 7)
	buf.PushEntry(3, 0)

	if buf.IsFull() {
		t.Error("expected not full, got full")
	}
	if val := buf.Size(); val == 2 {
		t.Error("expected 3, got full", val)
	}
}

func TestBufferPushEntryFull(t *testing.T) {
	buf, err := NewWeightedQuantilesBuffer(2, 100)
	if err != nil {
		t.Error("expected no err, got", err)
	}
	buf.PushEntry(5, 9)
	buf.PushEntry(2, 3)
	buf.PushEntry(-1, 7)
	buf.PushEntry(2, 1)

	expected := []BufferEntry{}
	expected = append(expected, BufferEntry{-1, 7})
	expected = append(expected, BufferEntry{2, 4})
	expected = append(expected, BufferEntry{5, 9})

	if !buf.IsFull() {
		t.Error("expected full, got not full")
	}
	if got := buf.GenerateEntryList(); !reflect.DeepEqual(expected, got) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}
func TestBufferPushEntryFullDeath(t *testing.T) {
	buf, err := NewWeightedQuantilesBuffer(2, 100)
	if err != nil {
		t.Error("expected no err, got", err)
	}
	buf.PushEntry(5, 9)
	buf.PushEntry(2, 3)
	buf.PushEntry(-1, 7)
	buf.PushEntry(2, 1)

	expected := []BufferEntry{}
	expected = append(expected, BufferEntry{-1, 7})
	expected = append(expected, BufferEntry{2, 4})
	expected = append(expected, BufferEntry{5, 9})

	if !buf.IsFull() {
		t.Error("expected full, got not full")
	}
	if err := buf.PushEntry(6, 6); err == nil {
		t.Error("expected buffer already full")
	}
}
