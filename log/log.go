package log

import (
	"encoding/binary"
	"fmt"

	"github.com/inelpandzic/simpledb/file"
)

var endian = binary.NativeEndian

type LogMgr struct {
	logFile          string
	fm               *file.FileMgr
	logPage          *file.Page
	currentBlock     *file.BlockID
	latestLSN        int
	latestDurableLSN int
}

func NewLogMgr(fm *file.FileMgr, logFile string) *LogMgr {
	currentBlock := &file.BlockID{
		Filename: logFile,
		Number:   0,
	}

	logPage := file.NewPage(fm.BlockSize)

	logSize, err := fm.FileSize(logFile)
	if err != nil {
		panic(err)
	}

	if logSize == 0 {
		offsetBytes := make([]byte, 4)
		endian.PutUint32(offsetBytes, uint32(fm.BlockSize))
		logPage.Write(0, offsetBytes)

		_, err := fm.Write(currentBlock, logPage)
		if err != nil {
			panic(err)
		}
	} else {
		currentBlock.Number = logSize - 1

		_, err = fm.Read(currentBlock, logPage)
		if err != nil {
			panic(err)
		}
	}

	return &LogMgr{
		fm:           fm,
		logFile:      logFile,
		logPage:      logPage,
		currentBlock: currentBlock,
	}
}

// Log logs the record to the log page.
func (lm *LogMgr) Log(record *Record) error {
	offsetBytes := make([]byte, 4)
	lm.logPage.Read(0, offsetBytes)

	offset := endian.Uint32(offsetBytes) // offset where the last record starts

	buffCap := int(offset) - 4
	if record.Length > buffCap {
		err := lm.Flush()
		if err != nil {
			return err
		}

		// create a new block by incrementing the block number
		// filename stays the same
		lm.currentBlock.Number++
		offset = uint32(lm.fm.BlockSize)

		endian.PutUint32(offsetBytes, offset)
		lm.logPage.Write(0, offsetBytes)
		_, err = lm.fm.Write(lm.currentBlock, lm.logPage)
	}

	recPos := int(offset) - record.TotalLength()

	_, err := lm.logPage.Write(recPos, record.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	endian.PutUint32(offsetBytes, uint32(recPos))
	_, err = lm.logPage.Write(0, offsetBytes)
	if err != nil {
		return fmt.Errorf("failed to write new offset: %w", err)
	}

	lm.latestLSN++

	return nil
}

// Flush writes the log page to disk.
func (lm *LogMgr) Flush() error {
	_, err := lm.fm.Write(lm.currentBlock, lm.logPage)
	if err != nil {
		return fmt.Errorf("failed to flush log page: %w", err)
	}

	lm.latestDurableLSN = lm.latestLSN
	return nil
}
