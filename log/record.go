package log

import (
	"encoding/binary"
)

type Record struct {
	length int

	Data []byte
}

func NewRecord(data []byte) *Record {
	return &Record{
		length: len(data),
		Data:   data,
	}
}

// bytes returns whole record bytes, length 4-byte metadata field plus data.	
func (r *Record) bytes() []byte {
	lengthBytes := make([]byte, 4)
	binary.NativeEndian.PutUint32(lengthBytes, uint32(r.length))

	return append(lengthBytes, r.Data...)
}

// totalLength returns the total length of the record, including the length 4-byte metadata field.
func (r *Record) totalLength() int {
	return 4 + r.length
}
