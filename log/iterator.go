package log

import (
	"fmt"

	"github.com/inelpandzic/simpledb/file"
)

type iterator struct {
	fm           *file.FileMgr
	currentBlock *file.BlockID
	currentPos   int
	page         *file.Page
}

func newIterator(fm *file.FileMgr, blockID file.BlockID) (*iterator, error) {
	page := file.NewPage(fm.BlockSize)

	_, err := fm.Read(&blockID, page)
	if err != nil {
		return nil, fmt.Errorf("failed to read block: %w", err)
	}

	currentPosBytes := make([]byte, intBytesSize)
	page.Read(0, currentPosBytes)
	currentPos := int(endian.Uint32(currentPosBytes))

	return &iterator{
		fm:           fm,
		currentBlock: &blockID,
		currentPos:   currentPos,
		page:         page,
	}, nil
}

func (i *iterator) HasNext() bool {
	return i.currentPos < i.fm.BlockSize || i.currentBlock.Number > 0
}

func (i *iterator) Next() (*Record, error) {
	if i.currentPos == i.fm.BlockSize {
		// move to the next block in reverse
		i.currentBlock.Number--

		_, err := i.fm.Read(i.currentBlock, i.page)
		if err != nil {
			return nil, fmt.Errorf("failed to read next block: %w", err)
		}

		currentPosBytes := make([]byte, intBytesSize)
		i.page.Read(0, currentPosBytes)
		i.currentPos = int(endian.Uint32(currentPosBytes))
	}

	lengthBytes := make([]byte, intBytesSize)
	i.page.Read(i.currentPos, lengthBytes)
	length := endian.Uint32(lengthBytes)

	data := make([]byte, length)
	i.page.Read(i.currentPos+intBytesSize, data)

	rec := &Record{
		length: int(length),
		Data:   data,
	}

	i.currentPos = i.currentPos + rec.totalLength()

	return rec, nil
}
