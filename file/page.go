package file

import (
	"errors"
)

type Page struct {
	bytes []byte
}

// NewPage creates a new page with the specified size.
func NewPage(size int) *Page {
	return &Page{
		bytes: make([]byte, size),
	}
}

func (p *Page) Write(offset int, data []byte) (int, error) {
	if offset+len(data) > p.Size() {
		return 0, errors.New("data exceeds page bounds")
	}

	n := copy(p.bytes[offset:], data)
	return n, nil
}

// Read copies data from the page at the specified offset and writes it to the data slice.
func (p *Page) Read(offset int, data []byte) int {
	return copy(data, p.bytes[offset:])
}

func (p *Page) Bytes() []byte {
	return p.bytes
}

func (p *Page) Size() int {
	return len(p.bytes)
}
