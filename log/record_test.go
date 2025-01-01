package log

import (
	"testing"
)

func TestRecordBytes(t *testing.T) {
	data := []byte("test")
	r := NewRecord(data)
	expected := []byte{4, 0, 0, 0, 't', 'e', 's', 't'}

	got := r.Bytes()

	if !bytesEqual(got, expected) {
		t.Errorf("Bytes = %v, want %v", got, expected)
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
