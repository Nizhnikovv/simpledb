package log

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

// bytes returns whole record bytes, length 4-byte metadata field plus data.
func (r *Record) bytes() []byte {
	lengthBytes := make([]byte, intBytesSize)
	endian.PutUint32(lengthBytes, uint32(r.Length))

	return append(lengthBytes, r.Data...)
}

// totalLength returns the total length of the record, including the length 4-byte metadata field.
func (r *Record) totalLength() int {
	return intBytesSize + r.Length
}
