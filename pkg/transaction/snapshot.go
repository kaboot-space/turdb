package transaction

import (
	"sync"
	"sync/atomic"
	"time"
)

// Snapshot represents a consistent view of the database at a specific version
type Snapshot struct {
	version  *VersionID
	readTime time.Time
	refCount int64
	isActive bool
	mutex    sync.RWMutex

	// Snapshot-specific data
	tableVersions map[uint64]*VersionID // Table-specific versions
	schemaVersion *VersionID            // Schema version at snapshot time
}

// NewSnapshot creates a new snapshot
func NewSnapshot(version *VersionID) *Snapshot {
	return &Snapshot{
		version:       version,
		readTime:      time.Now(),
		refCount:      1,
		isActive:      true,
		tableVersions: make(map[uint64]*VersionID),
		schemaVersion: version,
	}
}

// GetVersion returns the snapshot version
func (s *Snapshot) GetVersion() *VersionID {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.version
}

// GetReadTime returns when this snapshot was created
func (s *Snapshot) GetReadTime() time.Time {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.readTime
}

// AddRef increments the reference count
func (s *Snapshot) AddRef() {
	atomic.AddInt64(&s.refCount, 1)
}

// Release decrements the reference count
func (s *Snapshot) Release() int64 {
	return atomic.AddInt64(&s.refCount, -1)
}

// GetRefCount returns the current reference count
func (s *Snapshot) GetRefCount() int64 {
	return atomic.LoadInt64(&s.refCount)
}

// IsActive checks if the snapshot is still active
func (s *Snapshot) IsActive() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.isActive
}

// Invalidate marks the snapshot as inactive
func (s *Snapshot) Invalidate() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.isActive = false
}

// SetTableVersion sets the version for a specific table
func (s *Snapshot) SetTableVersion(tableID uint64, version *VersionID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.tableVersions[tableID] = version
}

// GetTableVersion gets the version for a specific table
func (s *Snapshot) GetTableVersion(tableID uint64) *VersionID {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if version, exists := s.tableVersions[tableID]; exists {
		return version
	}
	return s.version // Default to snapshot version
}
