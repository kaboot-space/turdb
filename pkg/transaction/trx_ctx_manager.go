package transaction

import (
	"context"
	"fmt"
	"sync"
)

// TransactionContextManager manages multiple transaction contexts
type TransactionContextManager struct {
	manager  *TransactionManager
	contexts map[string]*TransactionContext
	mutex    sync.RWMutex
}

// NewTransactionContextManager creates a new transaction context manager
func NewTransactionContextManager(manager *TransactionManager) *TransactionContextManager {
	return &TransactionContextManager{
		manager:  manager,
		contexts: make(map[string]*TransactionContext),
	}
}

// BeginContext begins a new transaction context with a unique ID
func (tcm *TransactionContextManager) BeginContext(id string, parent context.Context, opts *TransactionOptions) (*TransactionContext, error) {
	tcm.mutex.Lock()
	defer tcm.mutex.Unlock()

	if _, exists := tcm.contexts[id]; exists {
		return nil, fmt.Errorf("transaction context with id %s already exists", id)
	}

	txnCtx, err := NewTransactionContext(parent, tcm.manager, opts)
	if err != nil {
		return nil, err
	}

	tcm.contexts[id] = txnCtx
	return txnCtx, nil
}

// GetContext returns a transaction context by ID
func (tcm *TransactionContextManager) GetContext(id string) (*TransactionContext, bool) {
	tcm.mutex.RLock()
	defer tcm.mutex.RUnlock()
	ctx, exists := tcm.contexts[id]
	return ctx, exists
}

// RemoveContext removes a transaction context by ID
func (tcm *TransactionContextManager) RemoveContext(id string) error {
	tcm.mutex.Lock()
	defer tcm.mutex.Unlock()

	ctx, exists := tcm.contexts[id]
	if !exists {
		return fmt.Errorf("transaction context with id %s not found", id)
	}

	if err := ctx.Close(); err != nil {
		return fmt.Errorf("failed to close transaction context: %w", err)
	}

	delete(tcm.contexts, id)
	return nil
}

// CloseAllContexts closes all active transaction contexts
func (tcm *TransactionContextManager) CloseAllContexts() error {
	tcm.mutex.Lock()
	defer tcm.mutex.Unlock()

	var errors []error

	for id, ctx := range tcm.contexts {
		if err := ctx.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close context %s: %w", id, err))
		}
	}

	// Clear all contexts
	tcm.contexts = make(map[string]*TransactionContext)

	if len(errors) > 0 {
		return fmt.Errorf("errors closing contexts: %v", errors)
	}

	return nil
}

// GetActiveContextCount returns the number of active transaction contexts
func (tcm *TransactionContextManager) GetActiveContextCount() int {
	tcm.mutex.RLock()
	defer tcm.mutex.RUnlock()
	return len(tcm.contexts)
}

// ExecuteInContext executes a function within a transaction context
func (tcm *TransactionContextManager) ExecuteInContext(id string, parent context.Context, opts *TransactionOptions, fn func(*TransactionContext) error) error {
	txnCtx, err := tcm.BeginContext(id, parent, opts)
	if err != nil {
		return fmt.Errorf("failed to begin transaction context: %w", err)
	}

	defer func() {
		tcm.RemoveContext(id)
		if r := recover(); r != nil {
			txnCtx.Rollback()
			panic(r)
		}
	}()

	if err := fn(txnCtx); err != nil {
		if rollbackErr := txnCtx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("transaction error: %w, rollback error: %v", err, rollbackErr)
		}
		return err
	}

	if !opts.ReadOnly {
		if _, err := txnCtx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	} else {
		if err := txnCtx.Close(); err != nil {
			return fmt.Errorf("failed to close read transaction: %w", err)
		}
	}

	return nil
}

// WithTransaction is a helper function to execute code within a transaction context
func WithTransaction(ctx context.Context, manager *TransactionManager, opts *TransactionOptions, fn func(*Transaction) error) error {
	txnCtx, err := NewTransactionContext(ctx, manager, opts)
	if err != nil {
		return fmt.Errorf("failed to create transaction context: %w", err)
	}

	defer func() {
		if r := recover(); r != nil {
			txnCtx.Rollback()
			panic(r)
		}
	}()

	if err := fn(txnCtx.Transaction()); err != nil {
		if rollbackErr := txnCtx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("transaction error: %w, rollback error: %v", err, rollbackErr)
		}
		return err
	}

	if !opts.ReadOnly {
		if _, err := txnCtx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	} else {
		if err := txnCtx.Close(); err != nil {
			return fmt.Errorf("failed to close read transaction: %w", err)
		}
	}

	return nil
}
