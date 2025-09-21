package test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/turdb/tur/pkg/transaction"
)

func TestWALBasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "test.wal")

	config := transaction.LogConfig{
		FilePath:           logPath,
		SyncMode:           transaction.SyncModeCommit,
		BufferSize:         4096,
		CheckpointInterval: time.Minute,
	}

	wal, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Test writing different types of log entries
	testCases := []struct {
		name      string
		entryType transaction.LogEntryType
		txnID     uint64
		data      []byte
	}{
		{"Begin Entry", transaction.LogEntryTypeBegin, 1, []byte("begin_data")},
		{"Operation Entry", transaction.LogEntryTypeOperation, 1, []byte("operation_data")},
		{"Commit Entry", transaction.LogEntryTypeCommit, 1, []byte("commit_data")},
		{"Checkpoint Entry", transaction.LogEntryTypeCheckpoint, 0, []byte("checkpoint_data")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			entry := &transaction.LogEntry{
				Type:          tc.entryType,
				TransactionID: tc.txnID,
				Version:       1,
				Timestamp:     time.Now().UnixNano(),
				Data:          tc.data,
			}

			err := wal.WriteEntry(entry)
			if err != nil {
				t.Errorf("Failed to write %s: %v", tc.name, err)
			}
		})
	}

	// Test sync
	err = wal.Sync()
	if err != nil {
		t.Errorf("Failed to sync WAL: %v", err)
	}
}

func TestWALRecovery(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "recovery_test.wal")

	config := transaction.LogConfig{
		FilePath:           logPath,
		SyncMode:           transaction.SyncModeCommit,
		BufferSize:         4096,
		CheckpointInterval: time.Minute,
	}

	// Create WAL and write some entries
	wal, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	entries := []*transaction.LogEntry{
		{
			Type:          transaction.LogEntryTypeBegin,
			TransactionID: 1,
			Version:       1,
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("begin_1"),
		},
		{
			Type:          transaction.LogEntryTypeOperation,
			TransactionID: 1,
			Version:       1,
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("op_1"),
		},
		{
			Type:          transaction.LogEntryTypeCommit,
			TransactionID: 1,
			Version:       1,
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("commit_1"),
		},
	}

	for _, entry := range entries {
		err := wal.WriteEntry(entry)
		if err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}
	wal.Close()

	// Test recovery
	recovery, err := transaction.NewRecovery(logPath)
	if err != nil {
		t.Fatalf("Failed to create recovery: %v", err)
	}

	state, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}

	if state == nil {
		t.Fatal("Recovery state is nil")
	}

	// Verify recovery state
	if state.LastCommittedVersion == 0 {
		t.Error("Expected non-zero last committed version")
	}

	if len(state.PendingTransactions) != 0 {
		t.Errorf("Expected 0 pending transactions, got %d", len(state.PendingTransactions))
	}
}

func TestWALCorruption(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "corruption_test.wal")

	config := transaction.LogConfig{
		FilePath:           logPath,
		SyncMode:           transaction.SyncModeCommit,
		BufferSize:         4096,
		CheckpointInterval: time.Minute,
	}

	// Create WAL and write some entries
	wal, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	entry := &transaction.LogEntry{
		Type:          transaction.LogEntryTypeBegin,
		TransactionID: 1,
		Version:       1,
		Timestamp:     time.Now().UnixNano(),
		Data:          []byte("test_data"),
	}

	err = wal.WriteEntry(entry)
	if err != nil {
		t.Fatalf("Failed to write entry: %v", err)
	}

	err = wal.Sync()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}
	wal.Close()

	// Corrupt the file by truncating it
	file, err := os.OpenFile(logPath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open file for corruption: %v", err)
	}

	err = file.Truncate(10) // Truncate to invalid size
	if err != nil {
		t.Fatalf("Failed to truncate file: %v", err)
	}
	file.Close()

	// Test recovery with corrupted file
	recovery, err := transaction.NewRecovery(logPath)
	if err != nil {
		t.Fatalf("Failed to create recovery: %v", err)
	}

	_, err = recovery.Recover()
	if err == nil {
		t.Error("Expected error when recovering from corrupted file")
	}
}

func TestWALConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "concurrent_test.wal")

	config := transaction.LogConfig{
		FilePath:           logPath,
		SyncMode:           transaction.SyncModeCommit,
		BufferSize:         4096,
		CheckpointInterval: time.Minute,
	}

	wal, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Test concurrent writes
	done := make(chan bool, 10)
	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		go func(txnID int) {
			entry := &transaction.LogEntry{
				Type:          transaction.LogEntryTypeBegin,
				TransactionID: uint64(txnID),
				Version:       1,
				Timestamp:     time.Now().UnixNano(),
				Data:          []byte("concurrent_data"),
			}

			err := wal.WriteEntry(entry)
			if err != nil {
				errors <- err
				return
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		select {
		case <-done:
			// Success
		case err := <-errors:
			t.Errorf("Concurrent write failed: %v", err)
		case <-time.After(5 * time.Second):
			t.Error("Timeout waiting for concurrent writes")
		}
	}
}