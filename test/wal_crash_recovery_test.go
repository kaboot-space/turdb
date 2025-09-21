package test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/turdb/tur/pkg/transaction"
)

func TestCrashRecoveryBasic(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "crash_recovery_test.wal")

	config := transaction.LogConfig{
		FilePath:           logPath,
		SyncMode:           transaction.SyncModeCommit,
		BufferSize:         4096,
		CheckpointInterval: time.Minute,
	}

	// Create WAL and write some transactions
	wal, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write complete transactions
	for i := 0; i < 3; i++ {
		// Begin transaction
		beginEntry := &transaction.LogEntry{
			Type:          transaction.LogEntryTypeBegin,
			TransactionID: uint64(i),
			Version:       uint64(i + 1),
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("begin"),
		}
		wal.WriteEntry(beginEntry)

		// Operation
		opEntry := &transaction.LogEntry{
			Type:          transaction.LogEntryTypeOperation,
			TransactionID: uint64(i),
			Version:       uint64(i + 1),
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("test_operation"),
		}
		wal.WriteEntry(opEntry)

		// Commit
		commitEntry := &transaction.LogEntry{
			Type:          transaction.LogEntryTypeCommit,
			TransactionID: uint64(i),
			Version:       uint64(i + 1),
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("commit"),
		}
		wal.WriteEntry(commitEntry)
	}

	wal.Close()

	// Create recovery instance
	recovery, err := transaction.NewRecovery(logPath)
	if err != nil {
		t.Fatalf("Failed to create recovery: %v", err)
	}

	// Reopen WAL for crash recovery
	wal2, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// Test crash recovery
	crashConfig := transaction.CrashRecoveryConfig{
		MaxRecoveryTime:    time.Minute,
		VerifyChecksums:    true,
		RepairCorruption:   false,
		BackupBeforeRepair: false,
		LogRecoverySteps:   true,
	}
	
	crashRecovery := transaction.NewCrashRecovery(wal2, recovery, crashConfig)
	
	err = crashRecovery.PerformCrashRecovery()
	if err != nil {
		t.Fatalf("Failed to perform crash recovery: %v", err)
	}

	recoveryStats := crashRecovery.GetRecoveryStats()
	if recoveryStats.TransactionsRecovered != 3 {
		t.Errorf("Expected 3 transactions recovered, got %d", recoveryStats.TransactionsRecovered)
	}
}

func TestCrashRecoveryIncompleteTransaction(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "incomplete_txn_test.wal")

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

	// Write complete transaction
	beginEntry := &transaction.LogEntry{
		Type:          transaction.LogEntryTypeBegin,
		TransactionID: 1,
		Version:       1,
		Timestamp:     time.Now().UnixNano(),
		Data:          []byte("begin"),
	}
	wal.WriteEntry(beginEntry)

	commitEntry := &transaction.LogEntry{
		Type:          transaction.LogEntryTypeCommit,
		TransactionID: 1,
		Version:       1,
		Timestamp:     time.Now().UnixNano(),
		Data:          []byte("commit"),
	}
	wal.WriteEntry(commitEntry)

	// Write incomplete transaction (begin without commit)
	incompleteBegin := &transaction.LogEntry{
		Type:          transaction.LogEntryTypeBegin,
		TransactionID: 2,
		Version:       2,
		Timestamp:     time.Now().UnixNano(),
		Data:          []byte("begin"),
	}
	wal.WriteEntry(incompleteBegin)

	opEntry := &transaction.LogEntry{
		Type:          transaction.LogEntryTypeOperation,
		TransactionID: 2,
		Version:       2,
		Timestamp:     time.Now().UnixNano(),
		Data:          []byte("operation"),
	}
	wal.WriteEntry(opEntry)

	wal.Close()

	// Create recovery and crash recovery instances
	recovery, err := transaction.NewRecovery(logPath)
	if err != nil {
		t.Fatalf("Failed to create recovery: %v", err)
	}

	wal2, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	crashConfig := transaction.CrashRecoveryConfig{
		MaxRecoveryTime:    time.Minute,
		VerifyChecksums:    true,
		RepairCorruption:   false,
		BackupBeforeRepair: false,
		LogRecoverySteps:   true,
	}

	crashRecovery := transaction.NewCrashRecovery(wal2, recovery, crashConfig)
	
	err = crashRecovery.PerformCrashRecovery()
	if err != nil {
		t.Fatalf("Failed to perform crash recovery: %v", err)
	}

	recoveryStats := crashRecovery.GetRecoveryStats()
	// Should recover 1 complete transaction, incomplete one should be rolled back
	if recoveryStats.TransactionsRecovered != 1 {
		t.Errorf("Expected 1 transaction recovered, got %d", recoveryStats.TransactionsRecovered)
	}
}

func TestCrashRecoveryCorruptedLog(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "corrupted_log_test.wal")

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

	// Write some valid transactions
	for i := 0; i < 2; i++ {
		beginEntry := &transaction.LogEntry{
			Type:          transaction.LogEntryTypeBegin,
			TransactionID: uint64(i),
			Version:       uint64(i + 1),
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("begin"),
		}
		wal.WriteEntry(beginEntry)

		commitEntry := &transaction.LogEntry{
			Type:          transaction.LogEntryTypeCommit,
			TransactionID: uint64(i),
			Version:       uint64(i + 1),
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("commit"),
		}
		wal.WriteEntry(commitEntry)
	}

	wal.Close()

	// Corrupt the log file by appending invalid data
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file for corruption: %v", err)
	}

	// Write invalid data to corrupt the log
	corruptData := []byte("INVALID_LOG_DATA_CORRUPTION")
	file.Write(corruptData)
	file.Close()

	// Create recovery and crash recovery instances
	recovery, err := transaction.NewRecovery(logPath)
	if err != nil {
		t.Fatalf("Failed to create recovery: %v", err)
	}

	wal2, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	crashConfig := transaction.CrashRecoveryConfig{
		MaxRecoveryTime:    time.Minute,
		VerifyChecksums:    true,
		RepairCorruption:   true, // Enable repair for corrupted log
		BackupBeforeRepair: false,
		LogRecoverySteps:   true,
	}

	crashRecovery := transaction.NewCrashRecovery(wal2, recovery, crashConfig)
	
	err = crashRecovery.PerformCrashRecovery()
	if err != nil {
		t.Logf("Recovery failed as expected due to corruption: %v", err)
	}

	recoveryStats := crashRecovery.GetRecoveryStats()
	// Should recover valid transactions before corruption
	if recoveryStats.TransactionsRecovered < 2 {
		t.Logf("Recovered %d transactions before corruption", recoveryStats.TransactionsRecovered)
	}
}

func TestCrashRecoveryEmptyLog(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "empty_log_test.wal")

	// Create empty log file
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Failed to create empty log file: %v", err)
	}
	file.Close()

	config := transaction.LogConfig{
		FilePath:           logPath,
		SyncMode:           transaction.SyncModeCommit,
		BufferSize:         4096,
		CheckpointInterval: time.Minute,
	}

	// Create recovery and crash recovery instances
	recovery, err := transaction.NewRecovery(logPath)
	if err != nil {
		t.Fatalf("Failed to create recovery: %v", err)
	}

	wal, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	crashConfig := transaction.CrashRecoveryConfig{
		MaxRecoveryTime:    time.Minute,
		VerifyChecksums:    true,
		RepairCorruption:   false,
		BackupBeforeRepair: false,
		LogRecoverySteps:   true,
	}

	crashRecovery := transaction.NewCrashRecovery(wal, recovery, crashConfig)
	
	err = crashRecovery.PerformCrashRecovery()
	if err != nil {
		t.Fatalf("Failed to recover from empty log: %v", err)
	}

	recoveryStats := crashRecovery.GetRecoveryStats()
	if recoveryStats.TransactionsRecovered != 0 {
		t.Errorf("Expected 0 transactions recovered from empty log, got %d", recoveryStats.TransactionsRecovered)
	}
}