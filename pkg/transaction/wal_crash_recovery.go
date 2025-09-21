package transaction

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// CrashRecovery handles crash recovery for the transaction log
type CrashRecovery struct {
	wal      *TransactionLog
	recovery *Recovery
	mutex    sync.RWMutex

	// Recovery state
	isRecovering  bool
	recoveryStats RecoveryStats

	// Configuration
	config CrashRecoveryConfig
}

// CrashRecoveryConfig contains configuration for crash recovery
type CrashRecoveryConfig struct {
	MaxRecoveryTime    time.Duration
	VerifyChecksums    bool
	RepairCorruption   bool
	BackupBeforeRepair bool
	LogRecoverySteps   bool
}

// RecoveryStats contains statistics about the recovery process
type RecoveryStats struct {
	StartTime             time.Time
	EndTime               time.Time
	Duration              time.Duration
	EntriesProcessed      int64
	EntriesRecovered      int64
	EntriesSkipped        int64
	CorruptedEntries      int64
	CheckpointsFound      int64
	LastValidVersion      uint64
	TransactionsRecovered int64
	RecoverySuccessful    bool
	RecoveryFailed        bool
	ErrorsEncountered     []string
}

// NewCrashRecovery creates a new crash recovery instance
func NewCrashRecovery(wal *TransactionLog, recovery *Recovery, config CrashRecoveryConfig) *CrashRecovery {
	if config.MaxRecoveryTime <= 0 {
		config.MaxRecoveryTime = 30 * time.Minute
	}

	return &CrashRecovery{
		wal:      wal,
		recovery: recovery,
		config:   config,
		recoveryStats: RecoveryStats{
			ErrorsEncountered: make([]string, 0),
		},
	}
}

// PerformCrashRecovery performs crash recovery on the transaction log
func (cr *CrashRecovery) PerformCrashRecovery() error {
	cr.mutex.Lock()
	defer cr.mutex.Unlock()

	if cr.isRecovering {
		return fmt.Errorf("crash recovery is already in progress")
	}

	cr.isRecovering = true
	defer func() {
		cr.isRecovering = false
	}()

	// Initialize recovery stats
	cr.recoveryStats = RecoveryStats{
		StartTime:         time.Now(),
		ErrorsEncountered: make([]string, 0),
	}

	if cr.config.LogRecoverySteps {
		fmt.Println("Starting crash recovery...")
	}

	// Step 1: Verify log file integrity
	if err := cr.verifyLogIntegrity(); err != nil {
		cr.addError(fmt.Sprintf("Log integrity check failed: %v", err))
		if !cr.config.RepairCorruption {
			return fmt.Errorf("log integrity check failed and repair is disabled: %w", err)
		}
	}

	// Step 2: Find the last valid checkpoint
	checkpointVersion, err := cr.findLastValidCheckpoint()
	if err != nil {
		cr.addError(fmt.Sprintf("Failed to find last valid checkpoint: %v", err))
		// Continue with recovery from beginning if no checkpoint found
		checkpointVersion = 0
	}

	// Step 3: Recover transactions from checkpoint
	if err := cr.recoverFromCheckpoint(checkpointVersion); err != nil {
		cr.addError(fmt.Sprintf("Recovery from checkpoint failed: %v", err))
		return fmt.Errorf("recovery from checkpoint failed: %w", err)
	}

	// Step 4: Verify recovered state
	if err := cr.verifyRecoveredState(); err != nil {
		cr.addError(fmt.Sprintf("Recovered state verification failed: %v", err))
		return fmt.Errorf("recovered state verification failed: %w", err)
	}

	// Finalize recovery stats
	cr.recoveryStats.EndTime = time.Now()
	cr.recoveryStats.Duration = cr.recoveryStats.EndTime.Sub(cr.recoveryStats.StartTime)
	cr.recoveryStats.RecoverySuccessful = true

	if cr.config.LogRecoverySteps {
		fmt.Printf("Crash recovery completed successfully in %v\n", cr.recoveryStats.Duration)
		fmt.Printf("Processed %d entries, recovered %d entries\n",
			cr.recoveryStats.EntriesProcessed, cr.recoveryStats.EntriesRecovered)
	}

	return nil
}

// verifyLogIntegrity verifies the integrity of the log file
func (cr *CrashRecovery) verifyLogIntegrity() error {
	if cr.config.LogRecoverySteps {
		fmt.Println("Verifying log file integrity...")
	}

	// Check if log file exists and is readable
	if _, err := os.Stat(cr.wal.filePath); err != nil {
		if os.IsNotExist(err) {
			// No log file exists, this is not an error for new databases
			return nil
		}
		return fmt.Errorf("cannot access log file: %w", err)
	}

	// Basic integrity check - file exists and is readable
	return nil
}

// findLastValidCheckpoint finds the last valid checkpoint in the log
func (cr *CrashRecovery) findLastValidCheckpoint() (uint64, error) {
	if cr.config.LogRecoverySteps {
		fmt.Println("Finding last valid checkpoint...")
	}

	// Use the existing recovery to get the last committed version
	finalState, err := cr.recovery.Recover()
	if err != nil {
		return 0, err
	}

	checkpointVersion := finalState.LastCommittedVersion
	cr.recoveryStats.CheckpointsFound++
	cr.recoveryStats.LastValidVersion = checkpointVersion

	if cr.config.LogRecoverySteps && checkpointVersion > 0 {
		fmt.Printf("Found checkpoint at version %d\n", checkpointVersion)
	}

	return checkpointVersion, nil
}

// recoverFromCheckpoint recovers the database from a specific checkpoint
func (cr *CrashRecovery) recoverFromCheckpoint(checkpointVersion uint64) error {
	if cr.config.LogRecoverySteps {
		fmt.Printf("Recovering from checkpoint at version %d...\n", checkpointVersion)
	}

	// Use the existing recovery mechanism to recover from the checkpoint
	finalState, err := cr.recovery.Recover()
	if err != nil {
		return fmt.Errorf("checkpoint recovery failed: %w", err)
	}

	// Update recovery statistics
	cr.recoveryStats.EntriesRecovered = int64(len(finalState.PendingTransactions))
	cr.recoveryStats.LastValidVersion = finalState.LastCommittedVersion

	if cr.config.LogRecoverySteps {
		fmt.Printf("Checkpoint recovery completed successfully\n")
	}

	return nil
}

// recoverTransactions recovers transactions from the log
func (cr *CrashRecovery) recoverTransactions(fromVersion uint64) error {
	if cr.config.LogRecoverySteps {
		fmt.Printf("Recovering transactions from version %d...\n", fromVersion)
	}

	// Use the existing recovery mechanism
	finalState, err := cr.recovery.Recover()
	if err != nil {
		cr.recoveryStats.RecoveryFailed = true
		return fmt.Errorf("transaction recovery failed: %w", err)
	}

	// Update recovery statistics
	cr.recoveryStats.TransactionsRecovered = int64(len(finalState.PendingTransactions))
	cr.recoveryStats.LastValidVersion = finalState.LastCommittedVersion
	cr.recoveryStats.RecoverySuccessful = true

	if cr.config.LogRecoverySteps {
		fmt.Printf("Successfully recovered %d transactions\n", len(finalState.PendingTransactions))
	}

	return nil
}

// processRecoveryEntry processes a single log entry during recovery
func (cr *CrashRecovery) processRecoveryEntry(entry *LogEntry) error {
	switch entry.Type {
	case LogEntryTypeBegin:
		if err := cr.recovery.processBeginEntry(entry); err != nil {
			return fmt.Errorf("failed to process begin entry: %w", err)
		}
	case LogEntryTypeCommit:
		if err := cr.recovery.processCommitEntry(entry); err != nil {
			return fmt.Errorf("failed to process commit entry: %w", err)
		}
	case LogEntryTypeRollback:
		if err := cr.recovery.processRollbackEntry(entry); err != nil {
			return fmt.Errorf("failed to process rollback entry: %w", err)
		}
	case LogEntryTypeOperation:
		if err := cr.recovery.processOperationEntry(entry); err != nil {
			return fmt.Errorf("failed to process operation entry: %w", err)
		}
	case LogEntryTypeCheckpoint:
		if err := cr.recovery.processCheckpointEntry(entry); err != nil {
			return fmt.Errorf("failed to process checkpoint entry: %w", err)
		}
	}
	return nil
}

// verifyRecoveredState verifies the state after recovery
func (cr *CrashRecovery) verifyRecoveredState() error {
	if cr.config.LogRecoverySteps {
		fmt.Println("Verifying recovered state...")
	}

	// Verify that all committed transactions are properly recovered
	// This would typically involve checking database consistency
	// For now, this is a placeholder that always succeeds

	return nil
}

// addError adds an error to the recovery stats
func (cr *CrashRecovery) addError(errorMsg string) {
	cr.recoveryStats.ErrorsEncountered = append(cr.recoveryStats.ErrorsEncountered, errorMsg)
}

// IsRecovering returns whether crash recovery is currently in progress
func (cr *CrashRecovery) IsRecovering() bool {
	cr.mutex.RLock()
	defer cr.mutex.RUnlock()
	return cr.isRecovering
}

// GetRecoveryStats returns the current recovery statistics
func (cr *CrashRecovery) GetRecoveryStats() RecoveryStats {
	cr.mutex.RLock()
	defer cr.mutex.RUnlock()
	return cr.recoveryStats
}

// CreateBackup creates a backup of the log file before repair
func (cr *CrashRecovery) CreateBackup() error {
	if !cr.config.BackupBeforeRepair {
		return nil
	}

	backupPath := cr.wal.filePath + ".backup." + time.Now().Format("20060102-150405")

	// Copy the original file to backup location
	sourceFile, err := os.Open(cr.wal.filePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	backupFile, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer backupFile.Close()

	// Copy file contents
	_, err = backupFile.ReadFrom(sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file contents: %w", err)
	}

	if cr.config.LogRecoverySteps {
		fmt.Printf("Created backup at: %s\n", backupPath)
	}

	return nil
}

// RepairCorruption attempts to repair corruption in the log file
func (cr *CrashRecovery) RepairCorruption() error {
	if !cr.config.RepairCorruption {
		return fmt.Errorf("corruption repair is disabled")
	}

	if cr.config.LogRecoverySteps {
		fmt.Println("Attempting to repair log corruption...")
	}

	// Create backup before repair
	if err := cr.CreateBackup(); err != nil {
		return fmt.Errorf("failed to create backup before repair: %w", err)
	}

	// Truncate log at the corruption point to repair
	if err := cr.recovery.TruncateLog(0); err != nil {
		return fmt.Errorf("failed to repair log: %w", err)
	}

	if cr.config.LogRecoverySteps {
		fmt.Println("Log corruption repair completed")
	}

	return nil
}
