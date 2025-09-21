package transaction

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// RecoveryState represents the state of recovery process
type RecoveryState struct {
	LastCommittedVersion uint64
	PendingTransactions  map[uint64]*TransactionState
	CheckpointPosition   int64
	LogPosition          int64
}

// TransactionState represents the state of a transaction during recovery
type TransactionState struct {
	ID        uint64
	Version   uint64
	BeginTime int64
	Operations [][]byte
	Committed  bool
	Rolledback bool
}

// Recovery handles crash recovery from the transaction log
type Recovery struct {
	log      *TransactionLog
	state    *RecoveryState
	filePath string
}

// NewRecovery creates a new recovery instance
func NewRecovery(logFilePath string) (*Recovery, error) {
	recovery := &Recovery{
		filePath: logFilePath,
		state: &RecoveryState{
			PendingTransactions: make(map[uint64]*TransactionState),
		},
	}

	return recovery, nil
}

// Recover performs crash recovery from the transaction log
func (r *Recovery) Recover() (*RecoveryState, error) {
	// Check if log file exists
	if _, err := os.Stat(r.filePath); os.IsNotExist(err) {
		// No log file, nothing to recover
		return r.state, nil
	}

	file, err := os.Open(r.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file for recovery: %w", err)
	}
	defer file.Close()

	// Read and process all log entries
	for {
		entry, err := r.readLogEntry(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read log entry: %w", err)
		}

		if err := r.processLogEntry(entry); err != nil {
			return nil, fmt.Errorf("failed to process log entry: %w", err)
		}
	}

	// Finalize recovery state
	r.finalizeRecovery()

	return r.state, nil
}

// readLogEntry reads a single log entry from the file
func (r *Recovery) readLogEntry(file *os.File) (*LogEntry, error) {
	// Read entry header
	header := make([]byte, 29) // type(1) + txnID(8) + version(8) + timestamp(8) + dataSize(4)
	n, err := file.Read(header)
	if err != nil {
		return nil, err
	}
	if n != 29 {
		return nil, fmt.Errorf("incomplete header read: expected 29 bytes, got %d", n)
	}

	entry := &LogEntry{}
	offset := 0

	// Parse header
	entry.Type = LogEntryType(header[offset])
	offset++

	entry.TransactionID = binary.LittleEndian.Uint64(header[offset:])
	offset += 8

	entry.Version = binary.LittleEndian.Uint64(header[offset:])
	offset += 8

	entry.Timestamp = int64(binary.LittleEndian.Uint64(header[offset:]))
	offset += 8

	entry.DataSize = binary.LittleEndian.Uint32(header[offset:])

	// Read data if present
	if entry.DataSize > 0 {
		entry.Data = make([]byte, entry.DataSize)
		n, err := file.Read(entry.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to read entry data: %w", err)
		}
		if n != int(entry.DataSize) {
			return nil, fmt.Errorf("incomplete data read: expected %d bytes, got %d", entry.DataSize, n)
		}
	}

	// Read checksum
	checksumBytes := make([]byte, 4)
	n, err = file.Read(checksumBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read checksum: %w", err)
	}
	if n != 4 {
		return nil, fmt.Errorf("incomplete checksum read: expected 4 bytes, got %d", n)
	}

	entry.Checksum = binary.LittleEndian.Uint32(checksumBytes)

	// Verify checksum
	if !r.verifyChecksum(entry) {
		return nil, fmt.Errorf("checksum verification failed for entry")
	}

	return entry, nil
}

// processLogEntry processes a single log entry during recovery
func (r *Recovery) processLogEntry(entry *LogEntry) error {
	switch entry.Type {
	case LogEntryTypeBegin:
		return r.processBeginEntry(entry)
	case LogEntryTypeCommit:
		return r.processCommitEntry(entry)
	case LogEntryTypeRollback:
		return r.processRollbackEntry(entry)
	case LogEntryTypeOperation:
		return r.processOperationEntry(entry)
	case LogEntryTypeCheckpoint:
		return r.processCheckpointEntry(entry)
	case LogEntryTypeEndOfLog:
		return nil // End of log marker
	default:
		return fmt.Errorf("unknown log entry type: %d", entry.Type)
	}
}

// processBeginEntry processes a transaction begin entry
func (r *Recovery) processBeginEntry(entry *LogEntry) error {
	txnState := &TransactionState{
		ID:         entry.TransactionID,
		Version:    entry.Version,
		BeginTime:  entry.Timestamp,
		Operations: make([][]byte, 0),
		Committed:  false,
		Rolledback: false,
	}

	r.state.PendingTransactions[entry.TransactionID] = txnState
	return nil
}

// processCommitEntry processes a transaction commit entry
func (r *Recovery) processCommitEntry(entry *LogEntry) error {
	txnState, exists := r.state.PendingTransactions[entry.TransactionID]
	if !exists {
		// Transaction not found, might be a partial log
		return fmt.Errorf("commit entry for unknown transaction: %d", entry.TransactionID)
	}

	txnState.Committed = true
	r.state.LastCommittedVersion = entry.Version

	// Remove from pending transactions
	delete(r.state.PendingTransactions, entry.TransactionID)

	return nil
}

// processRollbackEntry processes a transaction rollback entry
func (r *Recovery) processRollbackEntry(entry *LogEntry) error {
	txnState, exists := r.state.PendingTransactions[entry.TransactionID]
	if !exists {
		// Transaction not found, might be a partial log
		return fmt.Errorf("rollback entry for unknown transaction: %d", entry.TransactionID)
	}

	txnState.Rolledback = true

	// Remove from pending transactions
	delete(r.state.PendingTransactions, entry.TransactionID)

	return nil
}

// processOperationEntry processes a transaction operation entry
func (r *Recovery) processOperationEntry(entry *LogEntry) error {
	txnState, exists := r.state.PendingTransactions[entry.TransactionID]
	if !exists {
		// Transaction not found, might be a partial log
		return fmt.Errorf("operation entry for unknown transaction: %d", entry.TransactionID)
	}

	// Store operation data
	txnState.Operations = append(txnState.Operations, entry.Data)

	return nil
}

// processCheckpointEntry processes a checkpoint entry
func (r *Recovery) processCheckpointEntry(entry *LogEntry) error {
	r.state.CheckpointPosition = r.state.LogPosition
	return nil
}

// finalizeRecovery finalizes the recovery process
func (r *Recovery) finalizeRecovery() {
	// Any pending transactions that were not committed or rolled back
	// should be considered as incomplete and need to be rolled back
	for txnID, txnState := range r.state.PendingTransactions {
		if !txnState.Committed && !txnState.Rolledback {
			// Mark as needing rollback
			txnState.Rolledback = true
		}
		// Keep in pending for application to handle
		_ = txnID // Avoid unused variable
	}
}

// verifyChecksum verifies the checksum of a log entry
func (r *Recovery) verifyChecksum(entry *LogEntry) bool {
	// Calculate expected checksum
	expectedChecksum := uint32(entry.Type)
	expectedChecksum ^= uint32(entry.TransactionID)
	expectedChecksum ^= uint32(entry.Version)
	expectedChecksum ^= uint32(entry.Timestamp)
	expectedChecksum ^= entry.DataSize

	for _, b := range entry.Data {
		expectedChecksum ^= uint32(b)
	}

	return expectedChecksum == entry.Checksum
}

// GetPendingTransactions returns transactions that need to be rolled back
func (r *Recovery) GetPendingTransactions() map[uint64]*TransactionState {
	return r.state.PendingTransactions
}

// GetLastCommittedVersion returns the last committed version
func (r *Recovery) GetLastCommittedVersion() uint64 {
	return r.state.LastCommittedVersion
}

// TruncateLog truncates the log file to remove incomplete transactions
func (r *Recovery) TruncateLog(position int64) error {
	file, err := os.OpenFile(r.filePath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file for truncation: %w", err)
	}
	defer file.Close()

	if err := file.Truncate(position); err != nil {
		return fmt.Errorf("failed to truncate log file: %w", err)
	}

	return nil
}