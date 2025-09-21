package transaction

import (
	"fmt"
	"sync"
	"time"
)

// CheckpointManager manages checkpoints for the transaction log
type CheckpointManager struct {
	wal                   *TransactionLog
	mutex                 sync.RWMutex
	checkpointInterval    time.Duration
	lastCheckpoint        time.Time
	lastCheckpointVersion uint64

	// Checkpoint configuration
	minLogSize int64         // Minimum log size before checkpoint
	maxLogAge  time.Duration // Maximum age of log entries

	// Background checkpoint goroutine
	stopChan       chan struct{}
	checkpointChan chan struct{}
	running        bool
}

// CheckpointConfig contains configuration for checkpoint management
type CheckpointConfig struct {
	Interval       time.Duration
	MinLogSize     int64
	MaxLogAge      time.Duration
	AutoCheckpoint bool
}

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(wal *TransactionLog, config CheckpointConfig) *CheckpointManager {
	if config.Interval <= 0 {
		config.Interval = 5 * time.Minute
	}

	if config.MinLogSize <= 0 {
		config.MinLogSize = 1024 * 1024 // 1MB
	}

	if config.MaxLogAge <= 0 {
		config.MaxLogAge = 30 * time.Minute
	}

	cm := &CheckpointManager{
		wal:                wal,
		checkpointInterval: config.Interval,
		minLogSize:         config.MinLogSize,
		maxLogAge:          config.MaxLogAge,
		lastCheckpoint:     time.Now(),
		stopChan:           make(chan struct{}),
		checkpointChan:     make(chan struct{}, 1),
	}

	if config.AutoCheckpoint {
		cm.Start()
	}

	return cm
}

// Start starts the background checkpoint process
func (cm *CheckpointManager) Start() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if cm.running {
		return fmt.Errorf("checkpoint manager is already running")
	}

	cm.running = true
	go cm.checkpointLoop()

	return nil
}

// Stop stops the background checkpoint process
func (cm *CheckpointManager) Stop() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if !cm.running {
		return nil
	}

	cm.running = false
	close(cm.stopChan)

	return nil
}

// checkpointLoop runs the background checkpoint process
func (cm *CheckpointManager) checkpointLoop() {
	ticker := time.NewTicker(cm.checkpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopChan:
			return
		case <-ticker.C:
			cm.performCheckpointIfNeeded()
		case <-cm.checkpointChan:
			cm.performCheckpointIfNeeded()
		}
	}
}

// performCheckpointIfNeeded performs a checkpoint if conditions are met
func (cm *CheckpointManager) performCheckpointIfNeeded() {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if cm.shouldCheckpoint() {
		if err := cm.performCheckpoint(); err != nil {
			fmt.Printf("Checkpoint failed: %v\n", err)
		}
	}
}

// shouldCheckpoint determines if a checkpoint should be performed
func (cm *CheckpointManager) shouldCheckpoint() bool {
	// Check time-based condition
	if time.Since(cm.lastCheckpoint) >= cm.checkpointInterval {
		return true
	}

	// Check log size condition
	currentPosition := cm.wal.GetPosition()
	lastCheckpointPosition := cm.wal.GetLastCheckpoint()
	if currentPosition-lastCheckpointPosition >= cm.minLogSize {
		return true
	}

	// Check log age condition
	if time.Since(cm.lastCheckpoint) >= cm.maxLogAge {
		return true
	}

	return false
}

// performCheckpoint performs an actual checkpoint
func (cm *CheckpointManager) performCheckpoint() error {
	// Get current version from WAL or transaction system
	// This would typically come from the database or transaction manager
	currentVersion := cm.lastCheckpointVersion + 1

	// Create checkpoint in WAL
	if err := cm.wal.Checkpoint(currentVersion); err != nil {
		return fmt.Errorf("failed to create checkpoint: %w", err)
	}

	// Update checkpoint tracking
	cm.lastCheckpoint = time.Now()
	cm.lastCheckpointVersion = currentVersion

	return nil
}

// ForceCheckpoint forces an immediate checkpoint
func (cm *CheckpointManager) ForceCheckpoint() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	return cm.performCheckpoint()
}

// TriggerCheckpoint triggers a checkpoint in the background
func (cm *CheckpointManager) TriggerCheckpoint() {
	select {
	case cm.checkpointChan <- struct{}{}:
	default:
		// Channel is full, checkpoint already pending
	}
}

// GetLastCheckpointTime returns the time of the last checkpoint
func (cm *CheckpointManager) GetLastCheckpointTime() time.Time {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.lastCheckpoint
}

// GetLastCheckpointVersion returns the version of the last checkpoint
func (cm *CheckpointManager) GetLastCheckpointVersion() uint64 {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.lastCheckpointVersion
}

// SetCheckpointVersion sets the checkpoint version (used during recovery)
func (cm *CheckpointManager) SetCheckpointVersion(version uint64) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	cm.lastCheckpointVersion = version
}

// GetCheckpointStats returns checkpoint statistics
func (cm *CheckpointManager) GetCheckpointStats() CheckpointStats {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	return CheckpointStats{
		LastCheckpointTime:    cm.lastCheckpoint,
		LastCheckpointVersion: cm.lastCheckpointVersion,
		LogPosition:           cm.wal.GetPosition(),
		CheckpointPosition:    cm.wal.GetLastCheckpoint(),
		IsRunning:             cm.running,
	}
}

// CheckpointStats contains checkpoint statistics
type CheckpointStats struct {
	LastCheckpointTime    time.Time
	LastCheckpointVersion uint64
	LogPosition           int64
	CheckpointPosition    int64
	IsRunning             bool
}

// CompactLog compacts the log by removing entries before the last checkpoint
func (cm *CheckpointManager) CompactLog() error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	checkpointPosition := cm.wal.GetLastCheckpoint()
	if checkpointPosition <= 0 {
		return fmt.Errorf("no checkpoint found, cannot compact log")
	}

	// In a real implementation, this would:
	// 1. Create a new log file
	// 2. Copy entries after the checkpoint to the new file
	// 3. Replace the old log file with the new one
	// 4. Update internal state

	// For now, this is a placeholder
	fmt.Printf("Log compaction would remove entries before position %d\n", checkpointPosition)

	return nil
}
