package log

import (
	"encoding/binary"
)

type Record struct {
	Length int
	Data   []byte
}

func NewRecord(data []byte) *Record {
	return &Record{
		Length: len(data),
		Data:   data,
	}
}

func (r *Record) Bytes() []byte {
	lengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, uint32(r.Length))

	return append(lengthBytes, r.Data...)
}

// TotalLength returns the total length of the record, including the length 4-byte metadata field.
func (r *Record) TotalLength() int {
	return 4 + r.Length
}
