package log

import (
	"encoding/binary"
	"fmt"
	"sync"

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

	mu sync.Mutex
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
		err := logPage.WriteInt(0, fm.BlockSize)
		if err != nil {
			panic(err)
		}

		_, err = fm.Write(currentBlock, logPage)
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
func (lm *LogMgr) Log(record *record) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	offsetBytes := make([]byte, 4)
	lm.logPage.Read(0, offsetBytes)

	offset := endian.Uint32(offsetBytes) // offset where the last record starts

	buffCap := int(offset) - 4
	if record.Length > buffCap {
		err := lm.Flush()
		if err != nil {
			return err
		}

		// create a new blockID by incrementing the block number
		lm.currentBlock.Number++

		// We can reuse the existing log page and overwrite it with the new data,
		// but a fresh page is created here for simplicity and ease of testing and inspection.
		// The old page will be garbage collected.
		lm.logPage = file.NewPage(lm.fm.BlockSize)

		offset = uint32(lm.fm.BlockSize)
		lm.logPage.WriteInt(0, int(offset))
		_, err = lm.fm.Write(lm.currentBlock, lm.logPage)
		if err != nil {
			return fmt.Errorf("failed to write log page: %w", err)
		}

	}

	recPos := int(offset) - record.totalLength()

	_, err := lm.logPage.Write(recPos, record.bytes())
	if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	err = lm.logPage.WriteInt(0, recPos)
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
