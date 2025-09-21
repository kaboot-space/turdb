package test

import (
	"testing"
	"time"

	"github.com/turdb/tur/pkg/transaction"
)

func TestVersionID(t *testing.T) {
	t.Run("CreateVersionID", func(t *testing.T) {
		version := transaction.NewVersionID(1)
		if version.Timestamp.IsZero() {
			t.Error("Version timestamp should not be zero")
		}
		if version.Salt == 0 {
			t.Error("Version salt should not be zero")
		}
		if version.ID != 1 {
			t.Error("Version ID should be 1")
		}
	})

	t.Run("VersionComparison", func(t *testing.T) {
		v1 := transaction.NewVersionID(1)
		v2 := transaction.NewVersionID(2)

		if !v1.IsOlderThan(v2) {
			t.Error("v1 should be older than v2")
		}
		if v2.IsOlderThan(v1) {
			t.Error("v2 should not be older than v1")
		}
		if v1.Compare(v2) != -1 {
			t.Error("v1 should compare as less than v2")
		}
		if v2.Compare(v1) != 1 {
			t.Error("v2 should compare as greater than v1")
		}
	})

	t.Run("VersionString", func(t *testing.T) {
		version := transaction.NewVersionID(42)
		str := version.String()
		if str == "" {
			t.Error("Version string should not be empty")
		}
	})
}

func TestSnapshot(t *testing.T) {
	t.Run("CreateSnapshot", func(t *testing.T) {
		version := transaction.NewVersionID(1)
		snapshot := transaction.NewSnapshot(version)

		if snapshot.GetVersion().ID != version.ID {
			t.Error("Snapshot version should match input version")
		}
		if snapshot.GetRefCount() != 1 {
			t.Error("Initial ref count should be 1")
		}
	})

	t.Run("SnapshotRefCounting", func(t *testing.T) {
		version := transaction.NewVersionID(1)
		snapshot := transaction.NewSnapshot(version)

		snapshot.AddRef()
		if snapshot.GetRefCount() != 2 {
			t.Error("Ref count should be 2 after AddRef")
		}

		refCount := snapshot.Release()
		if refCount != 1 {
			t.Error("Ref count should be 1 after Release")
		}

		refCount = snapshot.Release()
		if refCount != 0 {
			t.Error("Ref count should be 0 after final Release")
		}
	})

	t.Run("SnapshotActive", func(t *testing.T) {
		version := transaction.NewVersionID(1)
		snapshot := transaction.NewSnapshot(version)

		if !snapshot.IsActive() {
			t.Error("New snapshot should be active")
		}

		snapshot.Invalidate()
		if snapshot.IsActive() {
			t.Error("Invalidated snapshot should not be active")
		}
	})
}

func TestVersionTracker(t *testing.T) {
	t.Run("CreateVersionTracker", func(t *testing.T) {
		tracker := transaction.NewVersionTracker()
		if tracker == nil {
			t.Fatal("Version tracker should not be nil")
		}
	})

	t.Run("CreateNewVersion", func(t *testing.T) {
		tracker := transaction.NewVersionTracker()
		version := tracker.CreateNewVersion()

		if version.Timestamp.IsZero() {
			t.Error("Created version should have non-zero timestamp")
		}
	})

	t.Run("CreateSnapshot", func(t *testing.T) {
		tracker := transaction.NewVersionTracker()
		snapshot := tracker.CreateSnapshot()

		if snapshot == nil {
			t.Fatal("Snapshot should not be nil")
		}
	})

	t.Run("CreateSnapshotAtVersion", func(t *testing.T) {
		tracker := transaction.NewVersionTracker()
		version := tracker.CreateNewVersion()
		snapshot, err := tracker.CreateSnapshotAtVersion(version.ID)

		if err != nil {
			t.Fatalf("CreateSnapshotAtVersion failed: %v", err)
		}
		if snapshot == nil {
			t.Fatal("Snapshot should not be nil")
		}
		if snapshot.GetVersion().ID != version.ID {
			t.Error("Snapshot version should match requested version")
		}
	})

	t.Run("ReleaseSnapshot", func(t *testing.T) {
		tracker := transaction.NewVersionTracker()
		snapshot := tracker.CreateSnapshot()

		tracker.ReleaseSnapshot(snapshot)
		// Snapshot should be released and cleaned up
	})

	t.Run("GetOldestSnapshotVersion", func(t *testing.T) {
		tracker := transaction.NewVersionTracker()
		
		// No snapshots initially
		oldest := tracker.GetOldestSnapshotVersion()
		if oldest != 0 {
			t.Error("Should return 0 when no snapshots exist")
		}

		// Create snapshots
		v1 := tracker.CreateNewVersion()
		time.Sleep(1 * time.Millisecond)
		v2 := tracker.CreateNewVersion()

		s1, _ := tracker.CreateSnapshotAtVersion(v1.ID)
		s2, _ := tracker.CreateSnapshotAtVersion(v2.ID)

		oldest = tracker.GetOldestSnapshotVersion()
		if oldest != v1.ID {
			t.Error("Oldest snapshot should be v1")
		}

		// Release oldest snapshot
		tracker.ReleaseSnapshot(s1)
		oldest = tracker.GetOldestSnapshotVersion()
		if oldest != v2.ID {
			t.Error("Oldest snapshot should now be v2")
		}

		tracker.ReleaseSnapshot(s2)
	})

	t.Run("GarbageCollection", func(t *testing.T) {
		tracker := transaction.NewVersionTracker()

		// Create multiple versions
		for i := 0; i < 5; i++ {
			tracker.CreateNewVersion()
			time.Sleep(1 * time.Millisecond)
		}

		// Create snapshot only for the latest version
		snapshot := tracker.CreateSnapshot()

		// Run garbage collection
		tracker.GarbageCollect()

		// Only the latest version should remain
		// (This test assumes GC removes versions older than active snapshots)

		tracker.ReleaseSnapshot(snapshot)
	})
}

func TestMVCCManager(t *testing.T) {
	t.Run("CreateMVCCManager", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		if manager == nil {
			t.Fatal("MVCC manager should not be nil")
		}
	})

	t.Run("IsolationLevels", func(t *testing.T) {
		manager := transaction.NewMVCCManager()

		// Test setting different isolation levels
		levels := []transaction.IsolationLevel{
			transaction.ReadUncommitted,
			transaction.ReadCommitted,
			transaction.RepeatableRead,
			transaction.Serializable,
		}

		for _, level := range levels {
			manager.SetIsolationLevel(level)
			if manager.GetIsolationLevel() != level {
				t.Errorf("Expected isolation level %v, got %v", level, manager.GetIsolationLevel())
			}
		}
	})

	t.Run("BeginReadTransaction", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		snapshot := manager.BeginReadTransaction()

		if snapshot == nil {
			t.Fatal("Read transaction snapshot should not be nil")
		}
		if snapshot.GetRefCount() != 1 {
			t.Error("Snapshot ref count should be 1")
		}

		manager.GetVersionTracker().ReleaseSnapshot(snapshot)
	})

	t.Run("BeginWriteTransaction", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		version, snapshot := manager.BeginWriteTransaction()

		if version.Timestamp.IsZero() {
			t.Error("Write transaction version should have non-zero timestamp")
		}
		if snapshot == nil {
			t.Fatal("Write transaction snapshot should not be nil")
		}

		manager.GetVersionTracker().ReleaseSnapshot(snapshot)
	})

	t.Run("CommitTransaction", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		version, snapshot := manager.BeginWriteTransaction()

		err := manager.CommitTransaction(version)
		if err != nil {
			t.Errorf("Commit transaction failed: %v", err)
		}

		manager.GetVersionTracker().ReleaseSnapshot(snapshot)
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		version, snapshot := manager.BeginWriteTransaction()

		manager.RollbackTransaction(version, snapshot)
		// Should not panic or error
	})

	t.Run("TriggerGarbageCollection", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		
		// Create some versions and snapshots
		v1, s1 := manager.BeginWriteTransaction()
		manager.CommitTransaction(v1)
		manager.GetVersionTracker().ReleaseSnapshot(s1)
		
		v2, s2 := manager.BeginWriteTransaction()
		manager.CommitTransaction(v2)
		manager.GetVersionTracker().ReleaseSnapshot(s2)

		// Trigger GC
		manager.TriggerGarbageCollection()
		// Should not panic or error
	})

	t.Run("GetGarbageCollectionStats", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		stats := manager.GetGarbageCollectionStats()

		if stats == nil {
			t.Fatal("GC stats should not be nil")
		}

		// Check for expected stat keys
		expectedKeys := []string{"total_versions", "active_snapshots", "gc_runs", "versions_collected"}
		for _, key := range expectedKeys {
			if _, exists := stats[key]; !exists {
				t.Errorf("Expected stat key '%s' not found", key)
			}
		}
	})

	t.Run("GetVersionTracker", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		tracker := manager.GetVersionTracker()

		if tracker == nil {
			t.Fatal("Version tracker should not be nil")
		}
	})
}

func TestMVCCIntegration(t *testing.T) {
	t.Run("MultipleReadTransactions", func(t *testing.T) {
		manager := transaction.NewMVCCManager()

		// Create multiple read transactions
		snapshots := make([]*transaction.Snapshot, 3)
		for i := 0; i < 3; i++ {
			snapshots[i] = manager.BeginReadTransaction()
		}

		// All should have different versions but same isolation
		for i := 0; i < 3; i++ {
			if snapshots[i] == nil {
				t.Errorf("Snapshot %d should not be nil", i)
			}
		}

		// Clean up
		for _, snapshot := range snapshots {
			manager.GetVersionTracker().ReleaseSnapshot(snapshot)
		}
	})

	t.Run("ReadWriteInteraction", func(t *testing.T) {
		manager := transaction.NewMVCCManager()

		// Start read transaction
		readSnapshot := manager.BeginReadTransaction()
		readVersion := readSnapshot.GetVersion()

		// Start and commit write transaction
		writeVersion, writeSnapshot := manager.BeginWriteTransaction()
		err := manager.CommitTransaction(writeVersion)
		if err != nil {
			t.Errorf("Write commit failed: %v", err)
		}

		// Read transaction should still see old version
		if readSnapshot.GetVersion().Compare(readVersion) != 0 {
			t.Error("Read transaction should maintain consistent view")
		}

		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot)
		manager.GetVersionTracker().ReleaseSnapshot(writeSnapshot)
	})

	t.Run("GarbageCollectionWithActiveSnapshots", func(t *testing.T) {
		manager := transaction.NewMVCCManager()

		// Create and commit multiple write transactions
		for i := 0; i < 5; i++ {
			version, snapshot := manager.BeginWriteTransaction()
			manager.CommitTransaction(version)
			manager.GetVersionTracker().ReleaseSnapshot(snapshot)
			time.Sleep(1 * time.Millisecond)
		}

		// Create read transaction (should prevent GC of current version)
		snapshot := manager.BeginReadTransaction()

		// Trigger GC
		manager.TriggerGarbageCollection()

		// Snapshot should still be valid
		if snapshot.GetRefCount() == 0 {
			t.Error("Active snapshot should not be garbage collected")
		}

		manager.GetVersionTracker().ReleaseSnapshot(snapshot)
	})
}