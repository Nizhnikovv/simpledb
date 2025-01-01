package log

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/inelpandzic/simpledb/file"
)

func TestNewMgr(t *testing.T) {
	dataDir := "testdata"
	logFile := "testlogfile"

	fileMgr := file.NewFileMgr(dataDir, 32)
	t.Cleanup(func() {
		fileMgr.Close()
		os.Remove(filepath.Join(dataDir, logFile))
	})

	logMgr := NewLogMgr(fileMgr, logFile)

	offsetBytes := make([]byte, 4)
	logMgr.logPage.Read(0, offsetBytes)
	offset := endian.Uint32(offsetBytes)
	if offset != 32 {
		t.Errorf("offset = %d, want %d", offset, 32)
	}

	logSize, err := fileMgr.FileSize(logFile)
	if err != nil {
		t.Fatalf("FileSize failed: %v", err)
	}

	if logSize != 1 {
		t.Errorf("logSize = %d, want %d", logSize, 1)
	}

	// Test when log file already exists
	fileMgr.Write(&file.BlockID{
		Filename: logFile,
		Number:   1,
	}, file.NewPage(fileMgr.BlockSize))

	_ = NewLogMgr(fileMgr, logFile)

	logSize, err = fileMgr.FileSize(logFile)
	if err != nil {
		t.Fatalf("FileSize failed: %v", err)
	}

	if logSize != 2 {
		t.Errorf("logSize = %d, want %d", logSize, 2)
	}
}

func TestLog(t *testing.T) {
	dataDir := "testdata"
	logFile := "testlogfile1"

	fileMgr := file.NewFileMgr(dataDir, 32)
	t.Cleanup(func() {
		fileMgr.Close()
		os.Remove(filepath.Join(dataDir, logFile))
	})

	logMgr := NewLogMgr(fileMgr, logFile)

	tests := []struct {
		name            string
		record          *record
		expectedLogSize int
		expectedOffset  int
	}{
		{
			name:            "test loging first record",
			record:          NewRecord([]byte("test record")),
			expectedLogSize: 1,
			expectedOffset:  17, // 32 (offset before the write) - 15 (4 bytes for length and 11 bytes for data)
		},
		{
			name:            "test logging second record to be flushed to the same first block",
			record:          NewRecord([]byte("record 2")),
			expectedLogSize: 1,
			expectedOffset:  5, // 17 (offset before the write) - 12 (4 bytes for length and 8 bytes for data)
		},
		{
			name:            "test logging third record to be flushed to the new seconf block",
			record:          NewRecord([]byte("record 3")),
			expectedLogSize: 2,
			expectedOffset:  20, // 32 (offset before the write) - 12 (4 bytes for length and 8 bytes for data)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := logMgr.Log(tt.record)
			if err != nil {
				t.Fatalf("Log failed: %v", err)
			}

			offsetBytes := make([]byte, 4)
			logMgr.logPage.Read(0, offsetBytes)
			offset := binary.NativeEndian.Uint32(offsetBytes)

			if offset != uint32(tt.expectedOffset) {
				t.Errorf("offset = %d, want %d", offset, tt.expectedOffset)
			}

			logSize, err := fileMgr.FileSize(logFile)
			if err != nil {
				t.Fatalf("FileSize failed: %v", err)
			}
			if logSize != tt.expectedLogSize {
				t.Errorf("logSize = %d, want %d", logSize, tt.expectedLogSize)
			}
		})
	}

	err := logMgr.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}
