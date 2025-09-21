package transaction

import (
	"fmt"
	"sync"
	"time"
)

// VersionID represents a unique version identifier
type VersionID struct {
	ID        uint64    // Unique version number
	Timestamp time.Time // When this version was created
	Salt      uint32    // Additional uniqueness factor
}

// NewVersionID creates a new version ID
func NewVersionID(id uint64) *VersionID {
	return &VersionID{
		ID:        id,
		Timestamp: time.Now(),
		Salt:      uint32(time.Now().UnixNano() & 0xFFFFFFFF),
	}
}

// String returns string representation of version ID
func (v *VersionID) String() string {
	return fmt.Sprintf("v%d@%d.%d", v.ID, v.Timestamp.Unix(), v.Salt)
}

// Compare compares two version IDs (-1: less, 0: equal, 1: greater)
func (v *VersionID) Compare(other *VersionID) int {
	if v.ID < other.ID {
		return -1
	}
	if v.ID > other.ID {
		return 1
	}
	return 0
}

// IsOlderThan checks if this version is older than another
func (v *VersionID) IsOlderThan(other *VersionID) bool {
	return v.Compare(other) < 0
}

// MVCCManager manages Multi-Version Concurrency Control
type MVCCManager struct {
	versionTracker *VersionTracker
	isolationLevel IsolationLevel
	mutex          sync.RWMutex
}

// Use IsolationLevel constants from operations.go
const (
	ReadUncommitted = IsolationReadUncommitted
	ReadCommitted   = IsolationReadCommitted
	RepeatableRead  = IsolationRepeatableRead
	Serializable    = IsolationSerializable
)

// NewMVCCManager creates a new MVCC manager
func NewMVCCManager() *MVCCManager {
	return &MVCCManager{
		versionTracker: NewVersionTracker(),
		isolationLevel: RepeatableRead, // Default to repeatable read
	}
}

// GetVersionTracker returns the version tracker
func (mm *MVCCManager) GetVersionTracker() *VersionTracker {
	return mm.versionTracker
}

// SetIsolationLevel sets the isolation level
func (mm *MVCCManager) SetIsolationLevel(level IsolationLevel) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()
	mm.isolationLevel = level
}

// GetIsolationLevel returns the current isolation level
func (mm *MVCCManager) GetIsolationLevel() IsolationLevel {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()
	return mm.isolationLevel
}

// BeginReadTransaction starts a new read transaction with snapshot isolation
func (mm *MVCCManager) BeginReadTransaction() *Snapshot {
	return mm.versionTracker.CreateSnapshot()
}

// BeginWriteTransaction starts a new write transaction
func (mm *MVCCManager) BeginWriteTransaction() (*VersionID, *Snapshot) {
	// Create new version for write transaction
	newVersion := mm.versionTracker.CreateNewVersion()

	// Create snapshot at previous version for consistent reads
	snapshot := mm.versionTracker.CreateSnapshot()

	return newVersion, snapshot
}

// CommitTransaction commits a write transaction
func (mm *MVCCManager) CommitTransaction(version *VersionID) error {
	// In a real implementation, this would:
	// 1. Validate no conflicts occurred
	// 2. Make the version visible to other transactions
	// 3. Update any dependent structures

	// For now, just ensure the version exists
	if mm.versionTracker.GetVersion(version.ID) == nil {
		return fmt.Errorf("version %d not found for commit", version.ID)
	}

	return nil
}

// RollbackTransaction rolls back a write transaction
func (mm *MVCCManager) RollbackTransaction(version *VersionID, snapshot *Snapshot) {
	// Release the snapshot
	if snapshot != nil {
		mm.versionTracker.ReleaseSnapshot(snapshot)
	}

	// In a real implementation, this would also:
	// 1. Discard any changes made in this version
	// 2. Clean up any temporary structures
	// 3. Release any locks held
}

// IsVersionVisible checks if a version is visible to a snapshot
func (mm *MVCCManager) IsVersionVisible(snapshot *Snapshot, version *VersionID) bool {
	if snapshot == nil || version == nil {
		return false
	}

	snapshotVersion := snapshot.GetVersion()

	switch mm.GetIsolationLevel() {
	case ReadUncommitted:
		return true // Can see all versions
	case ReadCommitted:
		// Can see committed versions up to snapshot time
		return version.Timestamp.Before(snapshot.GetReadTime()) || version.Timestamp.Equal(snapshot.GetReadTime())
	case RepeatableRead, Serializable:
		// Can only see versions that existed when snapshot was created
		return version.IsOlderThan(snapshotVersion) || version.ID == snapshotVersion.ID
	default:
		return false
	}
}

// GetStats returns MVCC statistics
func (mm *MVCCManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"current_version": mm.versionTracker.GetCurrentVersion(),
		"version_count":   mm.versionTracker.GetVersionCount(),
		"snapshot_count":  mm.versionTracker.GetSnapshotCount(),
		"oldest_snapshot": mm.versionTracker.GetOldestSnapshotVersion(),
		"isolation_level": mm.GetIsolationLevel().String(),
	}
}

// TriggerGarbageCollection manually triggers garbage collection
func (mm *MVCCManager) TriggerGarbageCollection() {
	mm.versionTracker.GarbageCollect()
}

// GetGarbageCollectionStats returns garbage collection statistics
func (mm *MVCCManager) GetGarbageCollectionStats() map[string]interface{} {
	vt := mm.versionTracker
	vt.mutex.RLock()
	defer vt.mutex.RUnlock()

	return map[string]interface{}{
		"total_versions":     len(vt.versions),
		"active_snapshots":   len(vt.snapshots),
		"gc_runs":            vt.gcRuns,
		"versions_collected": vt.versionsCollected,
		"oldest_snapshot":    vt.oldestSnapshot,
		"current_version":    vt.currentVersion,
		"last_gc":            vt.lastGC,
		"gc_threshold":       vt.gcThreshold,
		"max_versions":       vt.maxVersions,
	}
}
