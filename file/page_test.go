package file

import (
	"errors"
	"testing"
)

func TestPageWriteRead(t *testing.T) {
	page := NewPage(16)

	// Write data to the page
	data := []byte("Hello, world!")
	n, err := page.Write(0, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Write returned %d, expected %d", n, len(data))
	}

	// Read the page from the beginning
	got := make([]byte, len(data))
	page.Read(0, got)
	want := []byte("Hello, world!")
	if string(got) != string(want) {
		t.Fatalf("Write failed: got %q, want %q", string(got), string(want))
	}

	// Write data at an offset
	data = []byte("SimpleDB!")
	n, err = page.Write(7, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Write returned %d, expected %d", n, len(data))
	}

	// Read the entire page
	want = []byte("Hello, SimpleDB!")
	got = page.Bytes()
	if string(got) != string(want) {
		t.Fatalf("Write failed: got %q, want %q", string(got), string(want))
	}

	// Read only a section of the page
	want = []byte("SimpleDB")
	got = make([]byte, len(want))
	page.Read(7, got)
	if string(got) != string(want) {
		t.Fatalf("Write failed: got %s, want %s", string(got), string(want))
	}

	// Write data at an offset
	data = []byte("longer data")
	n, err = page.Write(10, data)
	expectedErr := errors.New("data exceeds page bounds")
	if err == nil || err.Error() != expectedErr.Error() {
		t.Fatalf("Write returned no error or what was not expected: got %q, expected %q", err, expectedErr)
	}
}
