package transaction

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// VersionTracker manages version history and garbage collection
type VersionTracker struct {
	currentVersion uint64
	versions       map[uint64]*VersionID
	snapshots      map[uint64]*Snapshot
	oldestSnapshot uint64
	mutex          sync.RWMutex

	// Garbage collection settings
	maxVersions       int
	gcThreshold       time.Duration
	lastGC            time.Time
	gcRuns            uint64
	versionsCollected uint64
}

// NewVersionTracker creates a new version tracker
func NewVersionTracker() *VersionTracker {
	return &VersionTracker{
		currentVersion: 0,
		versions:       make(map[uint64]*VersionID),
		snapshots:      make(map[uint64]*Snapshot),
		oldestSnapshot: 0,
		maxVersions:    1000,
		gcThreshold:    time.Hour,
		lastGC:         time.Now(),
	}
}

// GetCurrentVersion returns the current version number
func (vt *VersionTracker) GetCurrentVersion() uint64 {
	return atomic.LoadUint64(&vt.currentVersion)
}

// CreateNewVersion creates and returns a new version
func (vt *VersionTracker) CreateNewVersion() *VersionID {
	vt.mutex.Lock()
	defer vt.mutex.Unlock()

	newVersionNum := atomic.AddUint64(&vt.currentVersion, 1)
	version := NewVersionID(newVersionNum)
	vt.versions[newVersionNum] = version

	// Trigger garbage collection periodically
	if time.Since(vt.lastGC) > vt.gcThreshold {
		go vt.GarbageCollect()
	}

	return version
}

// GetVersion retrieves a specific version by ID
func (vt *VersionTracker) GetVersion(versionID uint64) *VersionID {
	vt.mutex.RLock()
	defer vt.mutex.RUnlock()
	return vt.versions[versionID]
}

// CreateSnapshot creates a new snapshot at the current version
func (vt *VersionTracker) CreateSnapshot() *Snapshot {
	vt.mutex.Lock()
	defer vt.mutex.Unlock()

	currentVer := vt.GetCurrentVersion()
	version := vt.versions[currentVer]
	if version == nil {
		version = NewVersionID(currentVer)
		vt.versions[currentVer] = version
	}

	snapshot := NewSnapshot(version)
	vt.snapshots[currentVer] = snapshot

	// Update oldest snapshot tracking
	if vt.oldestSnapshot == 0 || currentVer < vt.oldestSnapshot {
		vt.oldestSnapshot = currentVer
	}

	return snapshot
}

// CreateSnapshotAtVersion creates a snapshot at a specific version
func (vt *VersionTracker) CreateSnapshotAtVersion(versionID uint64) (*Snapshot, error) {
	vt.mutex.Lock()
	defer vt.mutex.Unlock()

	version := vt.versions[versionID]
	if version == nil {
		return nil, fmt.Errorf("version %d not found", versionID)
	}

	snapshot := NewSnapshot(version)
	vt.snapshots[versionID] = snapshot

	// Update oldest snapshot tracking
	vt.updateOldestSnapshot()

	return snapshot, nil
}

// ReleaseSnapshot releases a snapshot and updates tracking
func (vt *VersionTracker) ReleaseSnapshot(snapshot *Snapshot) {
	if snapshot == nil {
		return
	}

	refCount := snapshot.Release()
	if refCount <= 0 {
		vt.mutex.Lock()
		defer vt.mutex.Unlock()

		versionID := snapshot.GetVersion().ID
		delete(vt.snapshots, versionID)
		snapshot.Invalidate()

		// Update oldest snapshot
		vt.updateOldestSnapshot()
	}
}

// updateOldestSnapshot finds the oldest active snapshot
func (vt *VersionTracker) updateOldestSnapshot() {
	oldestVersion := uint64(0)
	for versionID := range vt.snapshots {
		if oldestVersion == 0 || versionID < oldestVersion {
			oldestVersion = versionID
		}
	}
	vt.oldestSnapshot = oldestVersion
}

// GetOldestSnapshotVersion returns the version of the oldest active snapshot
func (vt *VersionTracker) GetOldestSnapshotVersion() uint64 {
	vt.mutex.RLock()
	defer vt.mutex.RUnlock()
	return vt.oldestSnapshot
}

// garbageCollect removes old versions that are no longer needed
func (vt *VersionTracker) GarbageCollect() {
	vt.mutex.Lock()
	defer vt.mutex.Unlock()

	now := time.Now()

	// Only run GC if enough time has passed
	if now.Sub(vt.lastGC) < vt.gcThreshold {
		return
	}

	vt.lastGC = now
	vt.gcRuns++
	oldestSnapshot := vt.GetOldestSnapshotVersion()

	// Remove versions older than the oldest active snapshot
	collected := uint64(0)
	for versionID, version := range vt.versions {
		if versionID < oldestSnapshot && now.Sub(version.Timestamp) > vt.gcThreshold {
			delete(vt.versions, versionID)
			collected++
		}
	}
	vt.versionsCollected += collected

	// Limit total number of versions
	vt.limitVersions()
}

// limitVersions removes excess versions when we have too many
func (vt *VersionTracker) limitVersions() {
	if len(vt.versions) <= vt.maxVersions {
		return
	}

	// Keep the most recent versions
	versions := make([]uint64, 0, len(vt.versions))
	for versionID := range vt.versions {
		versions = append(versions, versionID)
	}

	// Sort versions (simple bubble sort for small arrays)
	for i := 0; i < len(versions)-1; i++ {
		for j := 0; j < len(versions)-i-1; j++ {
			if versions[j] > versions[j+1] {
				versions[j], versions[j+1] = versions[j+1], versions[j]
			}
		}
	}

	// Remove oldest versions
	toRemove := len(versions) - vt.maxVersions
	for i := 0; i < toRemove; i++ {
		delete(vt.versions, versions[i])
	}
}

// GetVersionCount returns the number of tracked versions
func (vt *VersionTracker) GetVersionCount() int {
	vt.mutex.RLock()
	defer vt.mutex.RUnlock()
	return len(vt.versions)
}

// GetSnapshotCount returns the number of active snapshots
func (vt *VersionTracker) GetSnapshotCount() int {
	vt.mutex.RLock()
	defer vt.mutex.RUnlock()
	return len(vt.snapshots)
}

// SetGCThreshold sets the garbage collection threshold
func (vt *VersionTracker) SetGCThreshold(threshold time.Duration) {
	vt.mutex.Lock()
	defer vt.mutex.Unlock()
	vt.gcThreshold = threshold
}

// SetMaxVersions sets the maximum number of versions to keep
func (vt *VersionTracker) SetMaxVersions(max int) {
	vt.mutex.Lock()
	defer vt.mutex.Unlock()
	vt.maxVersions = max
}
