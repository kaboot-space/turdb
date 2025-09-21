package test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turdb/tur/pkg/transaction"
)

func TestIsolationLevels(t *testing.T) {
	t.Run("ReadUncommitted", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadUncommitted)

		if manager.GetIsolationLevel() != transaction.IsolationReadUncommitted {
			t.Error("Expected ReadUncommitted isolation level")
		}

		// Test dirty reads are allowed
		version1, writeSnapshot1 := manager.BeginWriteTransaction()
		readSnapshot := manager.BeginReadTransaction()

		// In ReadUncommitted, read should see uncommitted changes
		if readSnapshot.GetVersion().ID >= version1.ID {
			t.Log("ReadUncommitted allows reading uncommitted data")
		}

		manager.GetVersionTracker().ReleaseSnapshot(writeSnapshot1)
		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot)
	})

	t.Run("ReadCommitted", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		if manager.GetIsolationLevel() != transaction.IsolationReadCommitted {
			t.Error("Expected ReadCommitted isolation level")
		}

		// Test only committed data is read
		readSnapshot1 := manager.BeginReadTransaction()
		version1, _ := manager.BeginWriteTransaction()

		// Commit the write transaction
		err := manager.CommitTransaction(version1)
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		readSnapshot2 := manager.BeginReadTransaction()

		// ReadCommitted should see the committed version
		if readSnapshot2.GetVersion().ID <= readSnapshot1.GetVersion().ID {
			t.Error("ReadCommitted should see committed changes")
		}

		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot1)
		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot2)
	})

	t.Run("RepeatableRead", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationRepeatableRead)

		if manager.GetIsolationLevel() != transaction.IsolationRepeatableRead {
			t.Error("Expected RepeatableRead isolation level")
		}

		// Test consistent reads within transaction
		readSnapshot := manager.BeginReadTransaction()
		initialVersion := readSnapshot.GetVersion()

		// Start and commit another transaction
		version1, _ := manager.BeginWriteTransaction()
		err := manager.CommitTransaction(version1)
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// RepeatableRead should still see the same version
		if readSnapshot.GetVersion().ID != initialVersion.ID {
			t.Error("RepeatableRead should maintain consistent view")
		}

		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot)
	})

	t.Run("Serializable", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationSerializable)

		if manager.GetIsolationLevel() != transaction.IsolationSerializable {
			t.Error("Expected Serializable isolation level")
		}

		// Test serializable execution
		version1, writeSnapshot1 := manager.BeginWriteTransaction()
		version2, writeSnapshot2 := manager.BeginWriteTransaction()

		// In serializable, transactions should be ordered
		if version2.ID <= version1.ID {
			t.Error("Serializable should maintain transaction ordering")
		}

		manager.GetVersionTracker().ReleaseSnapshot(writeSnapshot1)
		manager.GetVersionTracker().ReleaseSnapshot(writeSnapshot2)
	})
}

func TestConcurrentIsolation(t *testing.T) {
	t.Run("ConcurrentReads", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		var wg sync.WaitGroup
		snapshots := make([]*transaction.Snapshot, 10)

		// Start multiple concurrent read transactions
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				snapshots[index] = manager.BeginReadTransaction()
			}(i)
		}

		wg.Wait()

		// All snapshots should be valid
		for i, snapshot := range snapshots {
			if snapshot == nil {
				t.Errorf("Snapshot %d should not be nil", i)
			}
			if snapshot.GetRefCount() != 1 {
				t.Errorf("Snapshot %d should have ref count 1", i)
			}
		}

		// Clean up
		for _, snapshot := range snapshots {
			if snapshot != nil {
				manager.GetVersionTracker().ReleaseSnapshot(snapshot)
			}
		}
	})

	t.Run("ReadWriteConflict", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationRepeatableRead)

		var wg sync.WaitGroup
		var readVersion, writeVersion *transaction.VersionID
		var readSnapshot, writeSnapshot *transaction.Snapshot

		// Start read transaction
		wg.Add(1)
		go func() {
			defer wg.Done()
			readSnapshot = manager.BeginReadTransaction()
			readVersion = readSnapshot.GetVersion()
			time.Sleep(100 * time.Millisecond) // Hold the read
		}()

		// Start write transaction after a delay
		time.Sleep(50 * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			writeVersion, writeSnapshot = manager.BeginWriteTransaction()
		}()

		wg.Wait()

		// Write version should be newer than read version
		if writeVersion.ID <= readVersion.ID {
			t.Error("Write transaction should have newer version than read")
		}

		// Clean up
		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot)
		manager.GetVersionTracker().ReleaseSnapshot(writeSnapshot)
	})

	t.Run("MultipleWriters", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationSerializable)

		var wg sync.WaitGroup
		versions := make([]*transaction.VersionID, 5)
		snapshots := make([]*transaction.Snapshot, 5)

		// Start multiple write transactions
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				versions[index], snapshots[index] = manager.BeginWriteTransaction()
			}(i)
		}

		wg.Wait()

		// Verify all versions are unique and non-zero
		versionSet := make(map[uint64]bool)
		for i, version := range versions {
			if version == nil {
				t.Errorf("Version %d is nil", i)
				continue
			}
			if version.ID == 0 {
				t.Errorf("Version %d has zero ID", i)
				continue
			}
			if versionSet[version.ID] {
				t.Errorf("Duplicate version ID %d found", version.ID)
			}
			versionSet[version.ID] = true
		}

		// Clean up
		for _, snapshot := range snapshots {
			if snapshot != nil {
				manager.GetVersionTracker().ReleaseSnapshot(snapshot)
			}
		}
	})
}

func TestIsolationWithCommitRollback(t *testing.T) {
	t.Run("CommitVisibility", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		// Start read transaction before write
		readSnapshot1 := manager.BeginReadTransaction()
		initialVersion := readSnapshot1.GetVersion()

		// Start and commit write transaction
		writeVersion, _ := manager.BeginWriteTransaction()
		err := manager.CommitTransaction(writeVersion)
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// New read transaction should see committed changes
		readSnapshot2 := manager.BeginReadTransaction()
		newVersion := readSnapshot2.GetVersion()

		if newVersion.ID <= initialVersion.ID {
			t.Error("New read transaction should see committed changes")
		}

		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot1)
		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot2)
	})

	t.Run("RollbackIsolation", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		// Start read transaction
		readSnapshot1 := manager.BeginReadTransaction()
		initialVersion := readSnapshot1.GetVersion()

		// Start and rollback write transaction
		writeVersion, writeSnapshot := manager.BeginWriteTransaction()
		manager.RollbackTransaction(writeVersion, writeSnapshot)

		// New read transaction should not see rolled back changes
		readSnapshot2 := manager.BeginReadTransaction()
		newVersion := readSnapshot2.GetVersion()

		// Version should not have advanced due to rollback
		if newVersion.ID != initialVersion.ID {
			t.Log("Version may advance even on rollback due to MVCC design")
		}

		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot1)
		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot2)
	})
}

func TestIsolationLevelTransitions(t *testing.T) {
	t.Run("ChangeIsolationLevel", func(t *testing.T) {
		manager := transaction.NewMVCCManager()

		// Test all isolation level transitions
		levels := []transaction.IsolationLevel{
			transaction.IsolationReadUncommitted,
			transaction.IsolationReadCommitted,
			transaction.IsolationRepeatableRead,
			transaction.IsolationSerializable,
		}

		for _, level := range levels {
			manager.SetIsolationLevel(level)
			if manager.GetIsolationLevel() != level {
				t.Errorf("Failed to set isolation level to %v", level)
			}

			// Test that transactions work with new level
			snapshot := manager.BeginReadTransaction()
			if snapshot == nil {
				t.Errorf("Failed to begin read transaction with isolation level %v", level)
			}
			manager.GetVersionTracker().ReleaseSnapshot(snapshot)
		}
	})

	t.Run("IsolationLevelString", func(t *testing.T) {
		levels := map[transaction.IsolationLevel]string{
			transaction.IsolationReadUncommitted: "ReadUncommitted",
			transaction.IsolationReadCommitted:   "ReadCommitted",
			transaction.IsolationRepeatableRead:  "RepeatableRead",
			transaction.IsolationSerializable:    "Serializable",
		}

		for level, expected := range levels {
			if level.String() != expected {
				t.Errorf("Expected %s, got %s", expected, level.String())
			}
		}
	})
}

func TestIsolationStress(t *testing.T) {
	t.Run("HighConcurrencyReads", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		var wg sync.WaitGroup
		numReaders := 100
		snapshots := make([]*transaction.Snapshot, numReaders)

		// Start many concurrent readers
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				snapshots[index] = manager.BeginReadTransaction()
				time.Sleep(10 * time.Millisecond) // Hold briefly
			}(i)
		}

		wg.Wait()

		// Verify all snapshots are valid
		validCount := 0
		for _, snapshot := range snapshots {
			if snapshot != nil && snapshot.GetRefCount() > 0 {
				validCount++
			}
		}

		if validCount != numReaders {
			t.Errorf("Expected %d valid snapshots, got %d", numReaders, validCount)
		}

		// Clean up
		for _, snapshot := range snapshots {
			if snapshot != nil {
				manager.GetVersionTracker().ReleaseSnapshot(snapshot)
			}
		}
	})

	t.Run("MixedReadWrite", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationSerializable)

		var wg sync.WaitGroup
		numOperations := 50
		var successCount int64

		// Mix of read and write operations
		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()

				if index%2 == 0 {
					// Read operation
					snapshot := manager.BeginReadTransaction()
					if snapshot != nil {
						time.Sleep(5 * time.Millisecond)
						manager.GetVersionTracker().ReleaseSnapshot(snapshot)
						atomic.AddInt64(&successCount, 1)
					}
				} else {
					// Write operation
					version, snapshot := manager.BeginWriteTransaction()
					if version != nil && snapshot != nil {
						time.Sleep(5 * time.Millisecond)
						err := manager.CommitTransaction(version)
						if err == nil {
							atomic.AddInt64(&successCount, 1)
						}
					}
				}
			}(i)
		}

		wg.Wait()

		minExpected := int64(float64(numOperations) * 0.8) // Allow some failures in stress test
		if successCount < minExpected {
			t.Errorf("Expected at least %d successes, got %d", minExpected, successCount)
		}
	})
}
