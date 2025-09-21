package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TransactionContext manages transaction lifecycle with context support
type TransactionContext struct {
	ctx       context.Context
	cancel    context.CancelFunc
	txn       *Transaction
	manager   *TransactionManager
	timeout   time.Duration
	startTime time.Time
	mutex     sync.RWMutex
	completed bool
}

// NewTransactionContext creates a new transaction context
func NewTransactionContext(parent context.Context, manager *TransactionManager, opts *TransactionOptions) (*TransactionContext, error) {
	if parent == nil {
		parent = context.Background()
	}

	if opts == nil {
		opts = &TransactionOptions{
			ReadOnly:       false,
			Timeout:        30 * time.Second,
			IsolationLevel: IsolationReadCommitted,
		}
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(parent, opts.Timeout)

	// Begin transaction
	txn, err := manager.BeginWithOptions(opts)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	txnCtx := &TransactionContext{
		ctx:       ctx,
		cancel:    cancel,
		txn:       txn,
		manager:   manager,
		timeout:   opts.Timeout,
		startTime: time.Now(),
		completed: false,
	}

	// Start monitoring goroutine
	go txnCtx.monitor()

	return txnCtx, nil
}

// Context returns the underlying context
func (tc *TransactionContext) Context() context.Context {
	return tc.ctx
}

// Transaction returns the underlying transaction
func (tc *TransactionContext) Transaction() *Transaction {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return tc.txn
}

// Commit commits the transaction
func (tc *TransactionContext) Commit() (uint64, error) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	if tc.completed {
		return 0, fmt.Errorf("transaction already completed")
	}

	// Check if context is cancelled
	if err := tc.ctx.Err(); err != nil {
		tc.rollbackInternal()
		return 0, fmt.Errorf("transaction context cancelled: %w", err)
	}

	version, err := tc.manager.CommitTransaction(tc.txn)
	if err != nil {
		tc.rollbackInternal()
		return 0, err
	}

	tc.completed = true
	tc.cancel()
	return version, nil
}

// Rollback rolls back the transaction
func (tc *TransactionContext) Rollback() error {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	if tc.completed {
		return fmt.Errorf("transaction already completed")
	}

	err := tc.rollbackInternal()
	tc.completed = true
	tc.cancel()
	return err
}

// rollbackInternal performs the actual rollback without locking
func (tc *TransactionContext) rollbackInternal() error {
	return tc.manager.RollbackTransaction(tc.txn)
}

// Close closes the transaction context
func (tc *TransactionContext) Close() error {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	if tc.completed {
		return nil
	}

	// If it's a read-only transaction, just close it
	if tc.txn.IsReadOnly() {
		err := tc.txn.Close()
		tc.manager.mutex.Lock()
		delete(tc.manager.activeTransactions, tc.txn.logID)
		tc.manager.mutex.Unlock()
		tc.completed = true
		tc.cancel()
		return err
	}

	// For write transactions, rollback
	err := tc.rollbackInternal()
	tc.completed = true
	tc.cancel()
	return err
}

// IsCompleted returns whether the transaction is completed
func (tc *TransactionContext) IsCompleted() bool {
	tc.mutex.RLock()
	defer tc.mutex.RUnlock()
	return tc.completed
}

// GetElapsedTime returns the elapsed time since transaction start
func (tc *TransactionContext) GetElapsedTime() time.Duration {
	return time.Since(tc.startTime)
}

// GetRemainingTime returns the remaining time before timeout
func (tc *TransactionContext) GetRemainingTime() time.Duration {
	elapsed := tc.GetElapsedTime()
	if elapsed >= tc.timeout {
		return 0
	}
	return tc.timeout - elapsed
}

// monitor monitors the transaction context for cancellation or timeout
func (tc *TransactionContext) monitor() {
	<-tc.ctx.Done()

	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	if tc.completed {
		return
	}

	// Context was cancelled or timed out, rollback the transaction
	tc.rollbackInternal()
	tc.completed = true
}
