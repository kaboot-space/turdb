package transaction

import (
	"fmt"
	"sync"
	"time"

	"github.com/turdb/tur/pkg/core"
)

// TransactionManager manages database transactions with MVCC support
type TransactionManager struct {
	db                 DBRef
	alloc              *core.SlabAlloc
	activeTransactions map[uint64]*Transaction
	mutex              sync.RWMutex
	nextTxnID          uint64

	// MVCC components
	mvccManager *MVCCManager
}

// NewTransactionManager creates a new transaction manager with MVCC support
func NewTransactionManager(db DBRef, alloc *core.SlabAlloc) *TransactionManager {
	return &TransactionManager{
		db:                 db,
		alloc:              alloc,
		activeTransactions: make(map[uint64]*Transaction),
		nextTxnID:          1,
		mvccManager:        NewMVCCManager(),
	}
}

// BeginRead starts a new read-only transaction with snapshot isolation
func (tm *TransactionManager) BeginRead() (*Transaction, error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// Get current database version
	currentVersion := tm.db.GetVersion()

	// Create read lock info
	readLock := &ReadLockInfo{
		Version:   currentVersion,
		ReaderID:  tm.nextTxnID,
		Timestamp: time.Now().UnixNano(),
	}

	// Create transaction
	txn, err := NewTransaction(tm.db, tm.alloc, readLock, TransactionStageReading)
	if err != nil {
		return nil, fmt.Errorf("failed to create read transaction: %w", err)
	}

	// Set up MVCC snapshot for consistent reads
	snapshot := tm.mvccManager.BeginReadTransaction()
	txn.SetSnapshot(snapshot)

	// Track the transaction
	txnID := tm.nextTxnID
	tm.nextTxnID++
	tm.activeTransactions[txnID] = txn

	return txn, nil
}

// BeginWrite starts a new write transaction with MVCC version
func (tm *TransactionManager) BeginWrite() (*Transaction, error) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// Acquire write lock first
	if err := tm.db.AcquireWriteLock(); err != nil {
		return nil, fmt.Errorf("failed to acquire write lock: %w", err)
	}

	// Get current database version
	currentVersion := tm.db.GetVersion()

	// Create read lock info for write transaction
	readLock := &ReadLockInfo{
		Version:   currentVersion,
		ReaderID:  tm.nextTxnID,
		Timestamp: time.Now().UnixNano(),
	}

	// Create transaction
	txn, err := NewTransaction(tm.db, tm.alloc, readLock, TransactionStageWriting)
	if err != nil {
		// Release write lock on error
		tm.db.ReleaseWriteLock()
		return nil, fmt.Errorf("failed to create write transaction: %w", err)
	}

	// Set up MVCC for write transaction
	writeVersion, snapshot := tm.mvccManager.BeginWriteTransaction()
	txn.SetMVCCVersion(writeVersion)
	txn.SetSnapshot(snapshot)

	// Track the transaction
	txnID := tm.nextTxnID
	tm.nextTxnID++
	tm.activeTransactions[txnID] = txn

	return txn, nil
}

// CommitTransaction commits a transaction and updates MVCC state
func (tm *TransactionManager) CommitTransaction(txn *Transaction) (uint64, error) {
	if txn == nil {
		return 0, fmt.Errorf("transaction cannot be nil")
	}

	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// Verify transaction is tracked
	if _, exists := tm.activeTransactions[txn.logID]; !exists {
		return 0, fmt.Errorf("transaction not found or already completed")
	}

	// Commit the transaction
	version, err := txn.Commit()
	if err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update MVCC state for successful commit
	if mvccVersion := txn.GetMVCCVersion(); mvccVersion != nil {
		if err := tm.mvccManager.CommitTransaction(mvccVersion); err != nil {
			return version, fmt.Errorf("failed to commit MVCC transaction: %w", err)
		}
	}

	// Release snapshot
	if snapshot := txn.GetSnapshot(); snapshot != nil {
		tm.mvccManager.GetVersionTracker().ReleaseSnapshot(snapshot)
	}

	// Remove from active transactions
	delete(tm.activeTransactions, txn.logID)

	// Release write lock if this was a write transaction
	if txn.IsWriteTransaction() && tm.db.IsWriteLockHeld() {
		if err := tm.db.ReleaseWriteLock(); err != nil {
			return version, fmt.Errorf("failed to release write lock: %w", err)
		}
	}

	// Trigger garbage collection if needed
	tm.mvccManager.TriggerGarbageCollection()

	return version, nil
}

// RollbackTransaction rolls back a transaction and cleans up MVCC state
func (tm *TransactionManager) RollbackTransaction(txn *Transaction) error {
	if txn == nil {
		return fmt.Errorf("transaction cannot be nil")
	}

	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// Verify transaction is tracked
	if _, exists := tm.activeTransactions[txn.logID]; !exists {
		return fmt.Errorf("transaction not found or already completed")
	}

	// Rollback the transaction
	if err := txn.Rollback(); err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	// Clean up MVCC state
	if snapshot := txn.GetSnapshot(); snapshot != nil {
		tm.mvccManager.RollbackTransaction(txn.GetMVCCVersion(), snapshot)
	}

	// Remove from active transactions
	delete(tm.activeTransactions, txn.logID)

	// Release write lock if this was a write transaction
	if txn.IsWriteTransaction() && tm.db.IsWriteLockHeld() {
		if err := tm.db.ReleaseWriteLock(); err != nil {
			return fmt.Errorf("failed to release write lock: %w", err)
		}
	}

	return nil
}

// GetActiveTransactionCount returns the number of active transactions
func (tm *TransactionManager) GetActiveTransactionCount() int {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	return len(tm.activeTransactions)
}

// GetTransaction returns a transaction by ID
func (tm *TransactionManager) GetTransaction(txnID uint64) (*Transaction, bool) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()
	txn, exists := tm.activeTransactions[txnID]
	return txn, exists
}

// CloseAllTransactions closes all active transactions
func (tm *TransactionManager) CloseAllTransactions() error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	var errors []error

	for txnID, txn := range tm.activeTransactions {
		if err := txn.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close transaction %d: %w", txnID, err))
		}
	}

	// Clear all transactions
	tm.activeTransactions = make(map[uint64]*Transaction)

	if len(errors) > 0 {
		return fmt.Errorf("errors closing transactions: %v", errors)
	}

	return nil
}
