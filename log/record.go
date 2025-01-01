package log

import (
	"encoding/binary"
)

type record struct {
	Length int
	Data   []byte
}

func NewRecord(data []byte) *record {
	return &record{
		Length: len(data),
		Data:   data,
	}
}

func (r *record) bytes() []byte {
	lengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, uint32(r.Length))

	return append(lengthBytes, r.Data...)
}

// totalLength returns the total length of the record, including the length 4-byte metadata field.
func (r *record) totalLength() int {
	return 4 + r.Length
}
