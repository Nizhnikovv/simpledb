package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var ErrBlockOutOfBound = fmt.Errorf("block number greater than file size")

type FileMgr struct {
	BlockSize int

	dataDir     string
	openedFiles map[string]*os.File

	mu sync.Mutex
}

func NewFileMgr(dataDir string, blockSize int) *FileMgr {
	return &FileMgr{
		BlockSize:   blockSize,
		dataDir:     dataDir,
		openedFiles: make(map[string]*os.File),
	}
}

// Read reads the contents of the specified block into the provided page.
func (fm *FileMgr) Read(blockID *BlockID, p *Page) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blockID.Filename)
	if err != nil {
		return 0, err
	}

	size, err := fm.FileSize(blockID.Filename)
	if err != nil {
		return 0, err
	}

	if blockID.Number >= size {
		return 0, ErrBlockOutOfBound
	}

	n, err := f.ReadAt(p.Bytes(), int64(blockID.Number*fm.BlockSize))
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("failed to read file: %w", err)
	}

	return n, nil
}

// Write writes the contents of the provided page to the specified block.
func (fm *FileMgr) Write(blockID *BlockID, p *Page) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(blockID.Filename)
	if err != nil {
		return 0, err
	}

	n, err := f.WriteAt(p.Bytes(), int64(blockID.Number*fm.BlockSize))
	if err != nil {
		return 0, fmt.Errorf("failed to write to file: %w", err)
	}

	return n, nil
}

// Close closes all opened files.
func (fm *FileMgr) Close() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, f := range fm.openedFiles {
		err := f.Close()
		if err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}
	}

	return nil
}

// FileSize returns the number of blocks in the specified file.
func (fm *FileMgr) FileSize(fileName string) (int, error) {
	file, err := fm.getFile(fileName)
	if err != nil {
		return 0, fmt.Errorf("failed to get file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %w", err)
	}

	return int(fileInfo.Size() / int64(fm.BlockSize)), nil
}

// getFile returns the file with the specified filename, creating it if it does not exist.
func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	f, ok := fm.openedFiles[filename]
	var err error
	if !ok {

		// This opens the file at the specified path with read and write permissions (os.O_RDWR),
		// creates the file if it does not exist (os.O_CREATE),
		// and ensures that writes are synchronized to stable storage (os.O_SYNC).
		// 0666 sets the file permissions to be readable and writable by all users.
		f, err = os.OpenFile(filepath.Join(fm.dataDir, filename), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
		fm.openedFiles[filename] = f
	}

	return f, nil
}
