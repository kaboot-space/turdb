package transaction

import (
	"fmt"
	"time"
)

// Begin is a convenience method that begins a read transaction by default
func (t *Transaction) Begin() error {
	return t.SetStage(TransactionStageReading)
}

// BeginWrite promotes the transaction to write mode
func (t *Transaction) BeginWrite() error {
	if err := t.PromoteToWrite(); err != nil {
		return fmt.Errorf("failed to begin write transaction: %w", err)
	}
	return nil
}

// IsReadOnly returns whether the transaction is read-only
func (t *Transaction) IsReadOnly() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return !t.isWritable
}

// IsWriteTransaction returns whether the transaction is a write transaction
func (t *Transaction) IsWriteTransaction() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.isWritable
}

// GetTransactionID returns the transaction ID
func (t *Transaction) GetTransactionID() uint64 {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.logID
}

// CanCommit returns whether the transaction can be committed
func (t *Transaction) CanCommit() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.isAttached && t.isWritable && t.stage == TransactionStageWriting
}

// CanRollback returns whether the transaction can be rolled back
func (t *Transaction) CanRollback() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.isAttached && t.isWritable && (t.stage == TransactionStageWriting || t.stage == TransactionStageCommitting)
}

// TransactionOptions represents options for creating transactions
type TransactionOptions struct {
	ReadOnly       bool
	Timeout        time.Duration
	IsolationLevel IsolationLevel
}

// IsolationLevel represents transaction isolation levels
type IsolationLevel int

const (
	IsolationReadUncommitted IsolationLevel = iota
	IsolationReadCommitted
	IsolationRepeatableRead
	IsolationSerializable
)

// String returns string representation of isolation level
func (il IsolationLevel) String() string {
	switch il {
	case IsolationReadUncommitted:
		return "ReadUncommitted"
	case IsolationReadCommitted:
		return "ReadCommitted"
	case IsolationRepeatableRead:
		return "RepeatableRead"
	case IsolationSerializable:
		return "Serializable"
	default:
		return "Unknown"
	}
}

// BeginWithOptions begins a transaction with specific options
func (tm *TransactionManager) BeginWithOptions(opts *TransactionOptions) (*Transaction, error) {
	if opts == nil {
		opts = &TransactionOptions{
			ReadOnly:       false,
			Timeout:        30 * time.Second,
			IsolationLevel: IsolationReadCommitted,
		}
	}

	if opts.ReadOnly {
		return tm.BeginRead()
	}
	return tm.BeginWrite()
}

// ExecuteInTransaction executes a function within a transaction
func (tm *TransactionManager) ExecuteInTransaction(fn func(*Transaction) error, opts *TransactionOptions) error {
	txn, err := tm.BeginWithOptions(opts)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if r := recover(); r != nil {
			tm.RollbackTransaction(txn)
			panic(r)
		}
	}()

	if err := fn(txn); err != nil {
		if rollbackErr := tm.RollbackTransaction(txn); rollbackErr != nil {
			return fmt.Errorf("transaction error: %w, rollback error: %v", err, rollbackErr)
		}
		return err
	}

	if !opts.ReadOnly {
		if _, err := tm.CommitTransaction(txn); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	} else {
		// For read-only transactions, just close them
		if err := txn.Close(); err != nil {
			return fmt.Errorf("failed to close read transaction: %w", err)
		}
		tm.mutex.Lock()
		delete(tm.activeTransactions, txn.logID)
		tm.mutex.Unlock()
	}

	return nil
}

// GetMVCCManager returns the MVCC manager
func (tm *TransactionManager) GetMVCCManager() *MVCCManager {
	return tm.mvccManager
}

// SetIsolationLevel sets the isolation level for new transactions
func (tm *TransactionManager) SetIsolationLevel(level IsolationLevel) {
	tm.mvccManager.SetIsolationLevel(level)
}

// GetIsolationLevel returns the current isolation level
func (tm *TransactionManager) GetIsolationLevel() IsolationLevel {
	return tm.mvccManager.GetIsolationLevel()
}

// GetMVCCStats returns MVCC statistics
func (tm *TransactionManager) GetMVCCStats() map[string]interface{} {
	return tm.mvccManager.GetStats()
}
