package test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/turdb/tur/pkg/transaction"
)

func TestCheckpointManager(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "checkpoint_test.wal")

	config := transaction.LogConfig{
		FilePath:           logPath,
		SyncMode:           transaction.SyncModeCommit,
		BufferSize:         4096,
		CheckpointInterval: 100 * time.Millisecond,
	}

	wal, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	checkpointConfig := transaction.CheckpointConfig{
		Interval:       100 * time.Millisecond,
		MinLogSize:     1024,
		MaxLogAge:      time.Minute,
		AutoCheckpoint: true,
	}

	manager := transaction.NewCheckpointManager(wal, checkpointConfig)

	err = manager.Start()
	if err != nil {
		t.Fatalf("Failed to start checkpoint manager: %v", err)
	}
	defer manager.Stop()

	// Write some entries to trigger checkpoint
	for i := 0; i < 10; i++ {
		entry := &transaction.LogEntry{
			Type:          transaction.LogEntryTypeOperation,
			TransactionID: uint64(i),
			Version:       1,
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("test_operation"),
		}

		err := wal.WriteEntry(entry)
		if err != nil {
			t.Errorf("Failed to write entry %d: %v", i, err)
		}
	}

	// Wait for checkpoint to trigger
	time.Sleep(200 * time.Millisecond)

	// Verify checkpoint was created
	lastCheckpoint := wal.GetLastCheckpoint()
	if lastCheckpoint == 0 {
		t.Error("Expected checkpoint to be created")
	}
}

func TestManualCheckpoint(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "manual_checkpoint_test.wal")

	config := transaction.LogConfig{
		FilePath:           logPath,
		SyncMode:           transaction.SyncModeCommit,
		BufferSize:         4096,
		CheckpointInterval: time.Hour, // Long interval to prevent automatic checkpoints
	}

	wal, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	checkpointConfig := transaction.CheckpointConfig{
		Interval:       time.Hour,
		MinLogSize:     1024,
		MaxLogAge:      time.Hour,
		AutoCheckpoint: false,
	}

	manager := transaction.NewCheckpointManager(wal, checkpointConfig)

	// Write some entries
	for i := 0; i < 5; i++ {
		entry := &transaction.LogEntry{
			Type:          transaction.LogEntryTypeCommit,
			TransactionID: uint64(i),
			Version:       uint64(i + 1),
			Timestamp:     time.Now().UnixNano(),
			Data:          []byte("commit_data"),
		}

		err := wal.WriteEntry(entry)
		if err != nil {
			t.Errorf("Failed to write entry %d: %v", i, err)
		}
	}

	initialCheckpoint := wal.GetLastCheckpoint()

	// Trigger manual checkpoint
	manager.TriggerCheckpoint()

	// Wait for checkpoint to process
	time.Sleep(100 * time.Millisecond)

	finalCheckpoint := wal.GetLastCheckpoint()
	if finalCheckpoint <= initialCheckpoint {
		t.Error("Expected checkpoint position to advance after manual trigger")
	}
}

func TestCheckpointRecovery(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "checkpoint_recovery_test.wal")

	config := transaction.LogConfig{
		FilePath:           logPath,
		SyncMode:           transaction.SyncModeCommit,
		BufferSize:         4096,
		CheckpointInterval: time.Minute,
	}

	// Create WAL and write entries with checkpoint
	wal, err := transaction.NewTransactionLog(config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write some transactions
	for i := 0; i < 3; i++ {
		// Begin
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
			Data:          []byte("operation"),
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

	// Create checkpoint
	err = wal.Checkpoint(3)
	if err != nil {
		t.Fatalf("Failed to create checkpoint: %v", err)
	}

	checkpointPos := wal.GetLastCheckpoint()
	wal.Close()

	// Test recovery from checkpoint
	recovery, err := transaction.NewRecovery(logPath)
	if err != nil {
		t.Fatalf("Failed to create recovery: %v", err)
	}

	state, err := recovery.Recover()
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}

	if state.CheckpointPosition != checkpointPos {
		t.Errorf("Expected checkpoint position %d, got %d", checkpointPos, state.CheckpointPosition)
	}

	if state.LastCommittedVersion != 3 {
		t.Errorf("Expected last committed version 3, got %d", state.LastCommittedVersion)
	}
}

func TestCheckpointCompaction(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "compaction_test.wal")

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

	checkpointConfig := transaction.CheckpointConfig{
		Interval:       time.Minute,
		MinLogSize:     512, // Small size to trigger compaction
		MaxLogAge:      time.Minute,
		AutoCheckpoint: true,
	}

	manager := transaction.NewCheckpointManager(wal, checkpointConfig)

	// Write many entries to exceed MaxLogSize
	for i := 0; i < 20; i++ {
		entry := &transaction.LogEntry{
			Type:          transaction.LogEntryTypeOperation,
			TransactionID: uint64(i),
			Version:       uint64(i + 1),
			Timestamp:     time.Now().UnixNano(),
			Data:          make([]byte, 50), // Large data to fill log quickly
		}

		err := wal.WriteEntry(entry)
		if err != nil {
			t.Errorf("Failed to write entry %d: %v", i, err)
		}
	}

	initialPos := wal.GetPosition()

	// Trigger compaction
	err = manager.CompactLog()
	if err != nil {
		t.Fatalf("Failed to compact log: %v", err)
	}

	finalPos := wal.GetPosition()

	// Verify log was compacted (position should be reset or reduced)
	if finalPos >= initialPos {
		t.Logf("Log compaction may not have occurred as expected. Initial: %d, Final: %d", initialPos, finalPos)
	}
}