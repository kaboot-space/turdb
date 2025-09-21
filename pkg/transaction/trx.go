package transaction

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/storage"
)

// TransactionStage represents the current stage of a transaction
type TransactionStage int

const (
	TransactionStageReady TransactionStage = iota
	TransactionStageReading
	TransactionStageWriting
	TransactionStageCommitting
	TransactionStageRollingBack
)

// AsyncState represents the asynchronous state of a transaction
type AsyncState int

const (
	AsyncStateNone AsyncState = iota
	AsyncStateWaiting
	AsyncStateCompleting
	AsyncStateCompleted
)

// ReadLockInfo contains information about read locks
type ReadLockInfo struct {
	Version   uint64
	ReaderID  uint64
	Timestamp int64
}

// DBRef represents a reference to the database
type DBRef interface {
	DoCommit(t *Transaction) (uint64, error)
	GetVersion() uint64
	GetVersionOfLatestSnapshot() uint64
	AcquireWriteLock() error
	ReleaseWriteLock() error
	IsWriteLockHeld() bool
}

// Transaction represents a database transaction
type Transaction struct {
	// Core components - similar to C++ Transaction constructor parameters
	db       DBRef
	alloc    *core.SlabAlloc
	readLock *ReadLockInfo

	// Transaction state
	stage      TransactionStage
	asyncState AsyncState
	version    uint64
	logID      uint64

	// Group reference - inherits from Group in C++
	group *storage.Group

	// Transaction properties
	isWritable bool
	isAttached bool

	// MVCC components
	mvccVersion *VersionID // MVCC version for this transaction
	snapshot    *Snapshot  // Snapshot for consistent reads

	// Synchronization
	mutex sync.RWMutex

	// Commit tracking
	commitSize atomic.Uint64

	// Write-Ahead Logging
	wal               *TransactionLog
	checkpointManager *CheckpointManager
}

// NewTransaction creates a new transaction
// Equivalent to Transaction constructor in C++
func NewTransaction(db DBRef, alloc *core.SlabAlloc, readLock *ReadLockInfo, stage TransactionStage) (*Transaction, error) {
	if db == nil {
		return nil, fmt.Errorf("database reference cannot be nil")
	}

	if alloc == nil {
		return nil, fmt.Errorf("allocator cannot be nil")
	}

	if readLock == nil {
		return nil, fmt.Errorf("read lock info cannot be nil")
	}

	t := &Transaction{
		db:          db,
		alloc:       alloc,
		readLock:    readLock,
		stage:       stage,
		asyncState:  AsyncStateNone,
		version:     readLock.Version,
		logID:       0, // Will be set during initialization
		isWritable:  stage == TransactionStageWriting,
		isAttached:  true,
		mvccVersion: NewVersionID(readLock.Version), // Initialize MVCC version
		snapshot:    nil,                            // Will be set by MVCC manager
	}

	// Initialize the transaction
	if err := t.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize transaction: %w", err)
	}

	return t, nil
}

// NewTransactionWithWAL creates a new transaction with Write-Ahead Logging
func NewTransactionWithWAL(db DBRef, alloc *core.SlabAlloc, readLock *ReadLockInfo, stage TransactionStage, wal *TransactionLog, checkpointManager *CheckpointManager) (*Transaction, error) {
	t, err := NewTransaction(db, alloc, readLock, stage)
	if err != nil {
		return nil, err
	}

	t.wal = wal
	t.checkpointManager = checkpointManager
	return t, nil
}

// initialize performs transaction initialization
func (t *Transaction) initialize() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Set transaction stage
	t.stage = TransactionStageReady

	// Attach shared resources (equivalent to C++ attach_shared)
	// This would involve setting up the group and other shared resources

	return nil
}

// GetVersion returns the transaction version
func (t *Transaction) GetVersion() uint64 {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.version
}

// GetVersionOfLatestSnapshot returns the version of the latest snapshot
func (t *Transaction) GetVersionOfLatestSnapshot() uint64 {
	return t.db.GetVersionOfLatestSnapshot()
}

// GetVersionOfCurrentTransaction returns the current transaction version
func (t *Transaction) GetVersionOfCurrentTransaction() uint64 {
	return t.db.GetVersion()
}

// Close closes the transaction and releases resources
func (t *Transaction) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Stop checkpoint manager if it exists
	if t.checkpointManager != nil {
		if err := t.checkpointManager.Stop(); err != nil {
			fmt.Printf("Failed to stop checkpoint manager: %v\n", err)
		}
	}

	// Close WAL if it exists
	if t.wal != nil {
		if err := t.wal.Close(); err != nil {
			fmt.Printf("Failed to close WAL: %v\n", err)
		}
	}

	// Reset transaction state
	t.stage = TransactionStageReady
	t.isAttached = false

	return nil
}

// IsAttached returns whether the transaction is attached
func (t *Transaction) IsAttached() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.isAttached
}

// GetTransactionStage returns the current transaction stage
func (t *Transaction) GetTransactionStage() TransactionStage {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.stage
}

// HoldsWriteMutex returns whether this transaction holds the write mutex
func (t *Transaction) HoldsWriteMutex() bool {
	return t.db.IsWriteLockHeld()
}

// PromoteToWrite promotes a read transaction to a write transaction
func (t *Transaction) PromoteToWrite() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.isWritable {
		return nil // Already writable
	}

	// Acquire write lock
	if err := t.db.AcquireWriteLock(); err != nil {
		return fmt.Errorf("failed to acquire write lock: %w", err)
	}

	// Update transaction state
	t.isWritable = true
	t.stage = TransactionStageWriting

	// Log transaction begin if WAL is enabled
	if t.wal != nil {
		if err := t.wal.LogBegin(t.logID, t.version); err != nil {
			// Rollback write lock on WAL error
			t.db.ReleaseWriteLock()
			t.isWritable = false
			t.stage = TransactionStageReady
			return fmt.Errorf("failed to log transaction begin: %w", err)
		}
	}

	return nil
}

// GetCommitSize returns the size of data to be committed
func (t *Transaction) GetCommitSize() uint64 {
	return t.commitSize.Load()
}

// Commit commits the transaction
// Equivalent to Transaction::commit in C++
func (t *Transaction) Commit() (uint64, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isAttached {
		return 0, fmt.Errorf("transaction is not attached")
	}

	// For read transactions, just return the current version
	if !t.isWritable {
		return t.version, nil
	}

	// Set stage to committing
	t.stage = TransactionStageCommitting

	// Flush accessors (equivalent to C++ flush_accessors_for_commit)
	if err := t.flushAccessorsForCommit(); err != nil {
		return 0, fmt.Errorf("failed to flush accessors: %w", err)
	}

	// Log commit to WAL before actual commit if WAL is enabled
	var commitData []byte
	if t.wal != nil {
		// Serialize transaction changes for WAL
		// This would contain the actual changes made during the transaction
		commitData = t.SerializeChanges()
		if err := t.wal.LogCommit(t.logID, t.version, commitData); err != nil {
			t.stage = TransactionStageWriting // Revert stage on error
			return 0, fmt.Errorf("failed to log commit to WAL: %w", err)
		}
	}

	// Perform the actual commit through the database
	newVersion, err := t.db.DoCommit(t)
	if err != nil {
		t.stage = TransactionStageWriting // Revert stage on error
		// If WAL logging succeeded but commit failed, we need to log rollback
		if t.wal != nil {
			t.wal.LogRollback(t.logID, t.version)
		}
		return 0, fmt.Errorf("commit failed: %w", err)
	}

	// Trigger checkpoint after successful commit if checkpoint manager is available
	if t.checkpointManager != nil {
		t.checkpointManager.TriggerCheckpoint()
	}

	// Update version and stage
	t.version = newVersion
	t.stage = TransactionStageReady
	t.isWritable = false

	// Release write lock
	if err := t.db.ReleaseWriteLock(); err != nil {
		// Log error but don't fail the commit since it succeeded
		fmt.Printf("Warning: failed to release write lock after commit: %v\n", err)
	}

	return newVersion, nil
}

// Rollback rolls back the transaction
// Equivalent to Transaction::rollback in C++
func (t *Transaction) Rollback() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isAttached {
		return fmt.Errorf("transaction is not attached")
	}

	// Check if already in ready stage (idempotent)
	if t.stage == TransactionStageReady {
		return nil
	}

	if !t.isWritable {
		return fmt.Errorf("transaction is not writable")
	}

	// Set stage to rolling back
	t.stage = TransactionStageRollingBack

	// Log rollback to WAL if enabled
	if t.wal != nil {
		if err := t.wal.LogRollback(t.logID, t.version); err != nil {
			// Continue with rollback even if WAL logging fails
			fmt.Printf("Warning: failed to log rollback to WAL: %v\n", err)
		}
	}

	// Reset free space tracking (equivalent to C++ reset_free_space_tracking)
	if err := t.resetFreeSpaceTracking(); err != nil {
		return fmt.Errorf("failed to reset free space tracking: %w", err)
	}

	// End write transaction
	if t.HoldsWriteMutex() {
		if err := t.db.ReleaseWriteLock(); err != nil {
			return fmt.Errorf("failed to release write lock: %w", err)
		}
	}

	// Reset transaction state
	t.stage = TransactionStageReady
	t.isWritable = false
	t.commitSize.Store(0)

	return nil
}

// SerializeChanges serializes the transaction changes for WAL logging
func (t *Transaction) SerializeChanges() []byte {
	// This is a placeholder implementation
	// In a real implementation, this would serialize all the changes
	// made during the transaction for recovery purposes
	return []byte("transaction_changes_placeholder")
}

// LogOperation logs a transaction operation to WAL
func (t *Transaction) LogOperation(operation []byte) error {
	if t.wal == nil {
		return nil // WAL not enabled
	}

	return t.wal.LogOperation(t.logID, t.version, operation)
}

// IsSynchronizing returns whether the transaction is synchronizing
func (t *Transaction) IsSynchronizing() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.asyncState != AsyncStateNone
}

// PromoteToAsync promotes the transaction to async mode
func (t *Transaction) PromoteToAsync() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.asyncState != AsyncStateNone {
		return nil // Already async
	}

	t.asyncState = AsyncStateWaiting
	return nil
}

// AsyncCompleteWrites completes async writes
func (t *Transaction) AsyncCompleteWrites() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.asyncState != AsyncStateWaiting {
		return fmt.Errorf("transaction is not in async waiting state")
	}

	t.asyncState = AsyncStateCompleting

	// Perform async completion logic here
	// This would involve completing any pending writes

	t.asyncState = AsyncStateCompleted
	return nil
}

// prepareForClose prepares the transaction for closing
func (t *Transaction) prepareForClose() error {
	// Implementation for preparing transaction close
	// This would involve cleanup of resources, flushing pending operations, etc.
	return nil
}

// flushAccessorsForCommit flushes accessors before commit
func (t *Transaction) flushAccessorsForCommit() error {
	// Implementation for flushing accessors
	// This would involve ensuring all pending changes are written
	return nil
}

// resetFreeSpaceTracking resets free space tracking
func (t *Transaction) resetFreeSpaceTracking() error {
	// Implementation for resetting free space tracking
	// This would involve clearing any tracked free space allocations
	return nil
}

// GetGroup returns the associated group
func (t *Transaction) GetGroup() *storage.Group {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.group
}

// SetGroup sets the associated group
func (t *Transaction) SetGroup(group *storage.Group) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.group = group
}

// GetAllocator returns the slab allocator
func (t *Transaction) GetAllocator() *core.SlabAlloc {
	return t.alloc
}

// GetReadLockInfo returns the read lock information
func (t *Transaction) GetReadLockInfo() *ReadLockInfo {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.readLock
}

// GetMVCCVersion returns the MVCC version for this transaction
func (t *Transaction) GetMVCCVersion() *VersionID {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.mvccVersion
}

// SetMVCCVersion sets the MVCC version for this transaction
func (t *Transaction) SetMVCCVersion(version *VersionID) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.mvccVersion = version
}

// GetSnapshot returns the snapshot for this transaction
func (t *Transaction) GetSnapshot() *Snapshot {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.snapshot
}

// SetSnapshot sets the snapshot for this transaction
func (t *Transaction) SetSnapshot(snapshot *Snapshot) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.snapshot = snapshot
}

// IsVersionVisible checks if a version is visible to this transaction
func (t *Transaction) IsVersionVisible(version *VersionID) bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if t.snapshot == nil || version == nil {
		return false
	}

	// For read transactions, use snapshot isolation
	if !t.isWritable {
		snapshotVersion := t.snapshot.GetVersion()
		return version.IsOlderThan(snapshotVersion) || version.ID == snapshotVersion.ID
	}

	// For write transactions, can see own writes and committed versions
	return version.ID <= t.mvccVersion.ID
}
