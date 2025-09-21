package transaction

import (
	"fmt"
	"sync"
)

// StateManager manages transaction state transitions
type StateManager struct {
	mutex sync.RWMutex
}

// NewStateManager creates a new state manager
func NewStateManager() *StateManager {
	return &StateManager{}
}

// ValidateTransition validates if a state transition is allowed
func (sm *StateManager) ValidateTransition(from, to TransactionStage) error {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	switch from {
	case TransactionStageReady:
		if to != TransactionStageReading && to != TransactionStageWriting {
			return fmt.Errorf("invalid transition from Ready to %v", to)
		}
	case TransactionStageReading:
		if to != TransactionStageReady && to != TransactionStageWriting {
			return fmt.Errorf("invalid transition from Reading to %v", to)
		}
	case TransactionStageWriting:
		if to != TransactionStageCommitting && to != TransactionStageRollingBack && to != TransactionStageReady {
			return fmt.Errorf("invalid transition from Writing to %v", to)
		}
	case TransactionStageCommitting:
		if to != TransactionStageReady {
			return fmt.Errorf("invalid transition from Committing to %v", to)
		}
	case TransactionStageRollingBack:
		if to != TransactionStageReady {
			return fmt.Errorf("invalid transition from RollingBack to %v", to)
		}
	default:
		return fmt.Errorf("unknown transaction stage: %v", from)
	}

	return nil
}

// ValidateAsyncTransition validates async state transitions
func (sm *StateManager) ValidateAsyncTransition(from, to AsyncState) error {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	switch from {
	case AsyncStateNone:
		if to != AsyncStateWaiting {
			return fmt.Errorf("invalid async transition from None to %v", to)
		}
	case AsyncStateWaiting:
		if to != AsyncStateCompleting && to != AsyncStateNone {
			return fmt.Errorf("invalid async transition from Waiting to %v", to)
		}
	case AsyncStateCompleting:
		if to != AsyncStateCompleted {
			return fmt.Errorf("invalid async transition from Completing to %v", to)
		}
	case AsyncStateCompleted:
		if to != AsyncStateNone {
			return fmt.Errorf("invalid async transition from Completed to %v", to)
		}
	default:
		return fmt.Errorf("unknown async state: %v", from)
	}

	return nil
}

// String returns string representation of TransactionStage
func (ts TransactionStage) String() string {
	switch ts {
	case TransactionStageReady:
		return "Ready"
	case TransactionStageReading:
		return "Reading"
	case TransactionStageWriting:
		return "Writing"
	case TransactionStageCommitting:
		return "Committing"
	case TransactionStageRollingBack:
		return "RollingBack"
	default:
		return "Unknown"
	}
}

// String returns string representation of AsyncState
func (as AsyncState) String() string {
	switch as {
	case AsyncStateNone:
		return "None"
	case AsyncStateWaiting:
		return "Waiting"
	case AsyncStateCompleting:
		return "Completing"
	case AsyncStateCompleted:
		return "Completed"
	default:
		return "Unknown"
	}
}

// IsValidStage checks if a transaction stage is valid
func IsValidStage(stage TransactionStage) bool {
	return stage >= TransactionStageReady && stage <= TransactionStageRollingBack
}

// IsValidAsyncState checks if an async state is valid
func IsValidAsyncState(state AsyncState) bool {
	return state >= AsyncStateNone && state <= AsyncStateCompleted
}

// CanRead returns whether the transaction can perform read operations
func (ts TransactionStage) CanRead() bool {
	return ts == TransactionStageReady || ts == TransactionStageReading || ts == TransactionStageWriting
}

// CanWrite returns whether the transaction can perform write operations
func (ts TransactionStage) CanWrite() bool {
	return ts == TransactionStageWriting
}

// IsActive returns whether the transaction is in an active state
func (ts TransactionStage) IsActive() bool {
	return ts != TransactionStageReady
}

// IsTerminal returns whether the transaction is in a terminal state
func (ts TransactionStage) IsTerminal() bool {
	return ts == TransactionStageCommitting || ts == TransactionStageRollingBack
}

// TransactionStateInfo provides detailed information about transaction state
type TransactionStateInfo struct {
	Stage      TransactionStage
	AsyncState AsyncState
	IsWritable bool
	IsAttached bool
	Version    uint64
	LogID      uint64
}

// NewTransactionStateInfo creates new transaction state info
func NewTransactionStateInfo(stage TransactionStage, asyncState AsyncState, isWritable, isAttached bool, version, logID uint64) *TransactionStateInfo {
	return &TransactionStateInfo{
		Stage:      stage,
		AsyncState: asyncState,
		IsWritable: isWritable,
		IsAttached: isAttached,
		Version:    version,
		LogID:      logID,
	}
}

// String returns string representation of transaction state info
func (tsi *TransactionStateInfo) String() string {
	return fmt.Sprintf("Stage: %s, AsyncState: %s, Writable: %t, Attached: %t, Version: %d, LogID: %d",
		tsi.Stage.String(), tsi.AsyncState.String(), tsi.IsWritable, tsi.IsAttached, tsi.Version, tsi.LogID)
}

// GetStateInfo returns current state information for a transaction
func (t *Transaction) GetStateInfo() *TransactionStateInfo {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return NewTransactionStateInfo(
		t.stage,
		t.asyncState,
		t.isWritable,
		t.isAttached,
		t.version,
		t.logID,
	)
}

// SetStage sets the transaction stage with validation
func (t *Transaction) SetStage(newStage TransactionStage) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Validate transition
	sm := NewStateManager()
	if err := sm.ValidateTransition(t.stage, newStage); err != nil {
		return err
	}

	t.stage = newStage
	return nil
}

// SetAsyncState sets the async state with validation
func (t *Transaction) SetAsyncState(newState AsyncState) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Validate transition
	sm := NewStateManager()
	if err := sm.ValidateAsyncTransition(t.asyncState, newState); err != nil {
		return err
	}

	t.asyncState = newState
	return nil
}
