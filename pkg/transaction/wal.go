package transaction

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// LogEntryType represents the type of log entry
type LogEntryType uint8

const (
	LogEntryTypeBegin LogEntryType = iota
	LogEntryTypeCommit
	LogEntryTypeRollback
	LogEntryTypeCheckpoint
	LogEntryTypeOperation
	LogEntryTypeEndOfLog
)

// LogEntry represents a single entry in the transaction log
type LogEntry struct {
	Type          LogEntryType
	TransactionID uint64
	Version       uint64
	Timestamp     int64
	DataSize      uint32
	Data          []byte
	Checksum      uint32
}

// TransactionLog represents the Write-Ahead Log
type TransactionLog struct {
	file       *os.File
	filePath   string
	mutex      sync.RWMutex
	position   int64
	syncMode   SyncMode
	bufferSize int

	// Checkpoint tracking
	lastCheckpoint     int64
	checkpointInterval time.Duration

	// Recovery state
	recoveryMode bool
	entries      []*LogEntry
}

// SyncMode defines when to sync the log to disk
type SyncMode int

const (
	SyncModeNone      SyncMode = iota // No explicit sync
	SyncModeCommit                    // Sync on commit
	SyncModeImmediate                 // Sync immediately
)

// LogConfig contains configuration for the transaction log
type LogConfig struct {
	FilePath           string
	SyncMode           SyncMode
	BufferSize         int
	CheckpointInterval time.Duration
}

// NewTransactionLog creates a new transaction log
func NewTransactionLog(config LogConfig) (*TransactionLog, error) {
	if config.FilePath == "" {
		return nil, fmt.Errorf("log file path cannot be empty")
	}

	if config.BufferSize <= 0 {
		config.BufferSize = 64 * 1024 // 64KB default
	}

	if config.CheckpointInterval <= 0 {
		config.CheckpointInterval = 5 * time.Minute // 5 minutes default
	}

	log := &TransactionLog{
		filePath:           config.FilePath,
		syncMode:           config.SyncMode,
		bufferSize:         config.BufferSize,
		checkpointInterval: config.CheckpointInterval,
		entries:            make([]*LogEntry, 0),
	}

	if err := log.open(); err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	return log, nil
}

// open opens the log file for writing
func (tl *TransactionLog) open() error {
	var err error
	tl.file, err = os.OpenFile(tl.filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	// Get current position
	tl.position, err = tl.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to get file position: %w", err)
	}

	return nil
}

// Close closes the transaction log
func (tl *TransactionLog) Close() error {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()

	if tl.file != nil {
		if err := tl.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync log file: %w", err)
		}
		if err := tl.file.Close(); err != nil {
			return fmt.Errorf("failed to close log file: %w", err)
		}
		tl.file = nil
	}

	return nil
}

// WriteEntry writes a log entry to the transaction log
func (tl *TransactionLog) WriteEntry(entry *LogEntry) error {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()

	if tl.file == nil {
		return fmt.Errorf("log file is not open")
	}

	// Calculate checksum
	entry.Checksum = tl.calculateChecksum(entry)

	// Serialize entry
	data, err := tl.serializeEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize entry: %w", err)
	}

	// Write to file
	n, err := tl.file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	tl.position += int64(n)

	// Sync if required
	if tl.syncMode == SyncModeImmediate {
		if err := tl.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync log file: %w", err)
		}
	}

	// Store entry for recovery
	tl.entries = append(tl.entries, entry)

	return nil
}

// LogBegin logs the beginning of a transaction
func (tl *TransactionLog) LogBegin(transactionID uint64, version uint64) error {
	entry := &LogEntry{
		Type:          LogEntryTypeBegin,
		TransactionID: transactionID,
		Version:       version,
		Timestamp:     time.Now().UnixNano(),
		DataSize:      0,
		Data:          nil,
	}

	return tl.WriteEntry(entry)
}

// LogCommit logs the commit of a transaction
func (tl *TransactionLog) LogCommit(transactionID uint64, version uint64, data []byte) error {
	entry := &LogEntry{
		Type:          LogEntryTypeCommit,
		TransactionID: transactionID,
		Version:       version,
		Timestamp:     time.Now().UnixNano(),
		DataSize:      uint32(len(data)),
		Data:          data,
	}

	err := tl.WriteEntry(entry)
	if err != nil {
		return err
	}

	// Sync on commit if required
	if tl.syncMode == SyncModeCommit {
		tl.mutex.Lock()
		defer tl.mutex.Unlock()
		if err := tl.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync log file on commit: %w", err)
		}
	}

	return nil
}

// LogRollback logs the rollback of a transaction
func (tl *TransactionLog) LogRollback(transactionID uint64, version uint64) error {
	entry := &LogEntry{
		Type:          LogEntryTypeRollback,
		TransactionID: transactionID,
		Version:       version,
		Timestamp:     time.Now().UnixNano(),
		DataSize:      0,
		Data:          nil,
	}

	return tl.WriteEntry(entry)
}

// LogOperation logs a transaction operation
func (tl *TransactionLog) LogOperation(transactionID uint64, version uint64, operation []byte) error {
	entry := &LogEntry{
		Type:          LogEntryTypeOperation,
		TransactionID: transactionID,
		Version:       version,
		Timestamp:     time.Now().UnixNano(),
		DataSize:      uint32(len(operation)),
		Data:          operation,
	}

	return tl.WriteEntry(entry)
}

// Checkpoint creates a checkpoint in the log
func (tl *TransactionLog) Checkpoint(version uint64) error {
	entry := &LogEntry{
		Type:      LogEntryTypeCheckpoint,
		Version:   version,
		Timestamp: time.Now().UnixNano(),
		DataSize:  0,
		Data:      nil,
	}

	err := tl.WriteEntry(entry)
	if err != nil {
		return err
	}

	tl.mutex.Lock()
	tl.lastCheckpoint = tl.position
	tl.mutex.Unlock()

	// Force sync on checkpoint
	tl.mutex.Lock()
	defer tl.mutex.Unlock()
	if err := tl.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file on checkpoint: %w", err)
	}

	return nil
}

// serializeEntry serializes a log entry to bytes
func (tl *TransactionLog) serializeEntry(entry *LogEntry) ([]byte, error) {
	// Calculate total size
	totalSize := 1 + 8 + 8 + 8 + 4 + int(entry.DataSize) + 4 // type + txnID + version + timestamp + dataSize + data + checksum

	buf := make([]byte, totalSize)
	offset := 0

	// Type
	buf[offset] = byte(entry.Type)
	offset++

	// Transaction ID
	binary.LittleEndian.PutUint64(buf[offset:], entry.TransactionID)
	offset += 8

	// Version
	binary.LittleEndian.PutUint64(buf[offset:], entry.Version)
	offset += 8

	// Timestamp
	binary.LittleEndian.PutUint64(buf[offset:], uint64(entry.Timestamp))
	offset += 8

	// Data size
	binary.LittleEndian.PutUint32(buf[offset:], entry.DataSize)
	offset += 4

	// Data
	if entry.DataSize > 0 {
		copy(buf[offset:], entry.Data)
		offset += int(entry.DataSize)
	}

	// Checksum
	binary.LittleEndian.PutUint32(buf[offset:], entry.Checksum)

	return buf, nil
}

// calculateChecksum calculates a simple checksum for the entry
func (tl *TransactionLog) calculateChecksum(entry *LogEntry) uint32 {
	// Simple CRC32-like checksum
	checksum := uint32(entry.Type)
	checksum ^= uint32(entry.TransactionID)
	checksum ^= uint32(entry.Version)
	checksum ^= uint32(entry.Timestamp)
	checksum ^= entry.DataSize

	for _, b := range entry.Data {
		checksum ^= uint32(b)
	}

	return checksum
}

// GetPosition returns the current position in the log file
func (tl *TransactionLog) GetPosition() int64 {
	tl.mutex.RLock()
	defer tl.mutex.RUnlock()
	return tl.position
}

// GetLastCheckpoint returns the position of the last checkpoint
func (tl *TransactionLog) GetLastCheckpoint() int64 {
	tl.mutex.RLock()
	defer tl.mutex.RUnlock()
	return tl.lastCheckpoint
}

// Sync forces a sync of the log to disk
func (tl *TransactionLog) Sync() error {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()

	if tl.file == nil {
		return fmt.Errorf("log file is not open")
	}

	return tl.file.Sync()
}
