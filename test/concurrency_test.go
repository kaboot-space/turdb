package test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turdb/tur/pkg/transaction"
)

func TestConcurrentReads(t *testing.T) {
	t.Run("MultipleReadersNoConflict", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		var wg sync.WaitGroup
		numReaders := 20
		snapshots := make([]*transaction.Snapshot, numReaders)
		errors := make([]error, numReaders)

		// Start multiple concurrent read transactions
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				snapshots[index] = manager.BeginReadTransaction()
				if snapshots[index] == nil {
					errors[index] = fmt.Errorf("failed to begin read transaction")
					return
				}
				
				// Hold the snapshot for a short time
				time.Sleep(50 * time.Millisecond)
			}(i)
		}

		wg.Wait()

		// Verify all snapshots are valid
		successCount := 0
		for i, snapshot := range snapshots {
			if errors[i] == nil && snapshot != nil && snapshot.GetRefCount() > 0 {
				successCount++
			}
		}

		if successCount != numReaders {
			t.Errorf("Expected %d successful reads, got %d", numReaders, successCount)
		}

		// Clean up
		for _, snapshot := range snapshots {
			if snapshot != nil {
				manager.GetVersionTracker().ReleaseSnapshot(snapshot)
			}
		}
	})

	t.Run("ReadersWithVersionProgression", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		var wg sync.WaitGroup
		versions := make([]*transaction.VersionID, 10)

		// Start readers at different times to see version progression
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				time.Sleep(time.Duration(index*10) * time.Millisecond)
				
				snapshot := manager.BeginReadTransaction()
				if snapshot != nil {
					versions[index] = snapshot.GetVersion()
					manager.GetVersionTracker().ReleaseSnapshot(snapshot)
				}
			}(i)
		}

		wg.Wait()

		// Verify versions are non-decreasing (may be equal due to timing)
		for i := 1; i < len(versions); i++ {
			if versions[i] != nil && versions[i-1] != nil {
				if versions[i].ID < versions[i-1].ID {
					t.Errorf("Version should not decrease: %d < %d", versions[i].ID, versions[i-1].ID)
				}
			}
		}
	})
}

func TestConcurrentWrites(t *testing.T) {
	t.Run("MultipleWritersOrdered", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationSerializable)

		var wg sync.WaitGroup
		numWriters := 10
		versions := make([]*transaction.VersionID, numWriters)
		snapshots := make([]*transaction.Snapshot, numWriters)

		// Start multiple concurrent write transactions
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				versions[index], snapshots[index] = manager.BeginWriteTransaction()
			}(i)
		}

		wg.Wait()

		// Verify all versions are unique and ordered
		versionSet := make(map[uint64]bool)
		for i, version := range versions {
			if version == nil {
				t.Errorf("Version %d should not be nil", i)
				continue
			}
			
			if versionSet[version.ID] {
				t.Errorf("Duplicate version ID: %d", version.ID)
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

	t.Run("WriteCommitSequence", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		var wg sync.WaitGroup
		numWriters := 5
		commitErrors := make([]error, numWriters)

		// Start and commit write transactions
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				version, snapshot := manager.BeginWriteTransaction()
				if version == nil || snapshot == nil {
					commitErrors[index] = fmt.Errorf("failed to begin write transaction")
					return
				}
				
				// Hold the transaction briefly
				time.Sleep(20 * time.Millisecond)
				
				// Commit the transaction
				commitErrors[index] = manager.CommitTransaction(version)
				
				// Clean up snapshot
				if snapshot != nil {
					manager.GetVersionTracker().ReleaseSnapshot(snapshot)
				}
			}(i)
		}

		wg.Wait()

		// Verify all commits succeeded
		successCount := 0
		for i, err := range commitErrors {
			if err == nil {
				successCount++
			} else {
				t.Logf("Write %d failed: %v", i, err)
			}
		}

		if successCount != numWriters {
			t.Errorf("Expected %d successful commits, got %d", numWriters, successCount)
		}
	})
}

func TestReadWriteInteraction(t *testing.T) {
	t.Run("ReadDuringWrite", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		var wg sync.WaitGroup
		var readVersion, writeVersion *transaction.VersionID
		var readSnapshot *transaction.Snapshot

		// Start write transaction first
		wg.Add(1)
		go func() {
			defer wg.Done()
			writeVersion, _ = manager.BeginWriteTransaction()
			time.Sleep(100 * time.Millisecond) // Hold the write
		}()

		// Start read transaction after a delay
		time.Sleep(50 * time.Millisecond)
		wg.Add(1)
		go func() {
			defer wg.Done()
			readSnapshot = manager.BeginReadTransaction()
			readVersion = readSnapshot.GetVersion()
		}()

		wg.Wait()

		// Read should see a consistent version
		if readVersion == nil {
			t.Error("Read version should not be nil")
		}
		if writeVersion == nil {
			t.Error("Write version should not be nil")
		}

		// Clean up
		if readSnapshot != nil {
			manager.GetVersionTracker().ReleaseSnapshot(readSnapshot)
		}
	})

	t.Run("WriteAfterRead", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationRepeatableRead)

		// Start read transaction
		readSnapshot := manager.BeginReadTransaction()
		readVersion := readSnapshot.GetVersion()

		// Start write transaction
		writeVersion, writeSnapshot := manager.BeginWriteTransaction()

		// Write version should be newer than read version
		if writeVersion.ID <= readVersion.ID {
			t.Error("Write version should be newer than read version")
		}

		// Commit write transaction
		err := manager.CommitTransaction(writeVersion)
		if err != nil {
			t.Errorf("Failed to commit write transaction: %v", err)
		}

		// Read transaction should still see its original version (RepeatableRead)
		if readSnapshot.GetVersion().ID != readVersion.ID {
			t.Error("RepeatableRead should maintain consistent version")
		}

		// Clean up
		manager.GetVersionTracker().ReleaseSnapshot(readSnapshot)
		manager.GetVersionTracker().ReleaseSnapshot(writeSnapshot)
	})
}

func TestConcurrentGarbageCollection(t *testing.T) {
	t.Run("GCWithActiveSnapshots", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		var wg sync.WaitGroup
		snapshots := make([]*transaction.Snapshot, 5)

		// Create multiple snapshots
		for i := 0; i < 5; i++ {
			snapshots[i] = manager.BeginReadTransaction()
		}

		// Start concurrent operations
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Trigger garbage collection
			manager.TriggerGarbageCollection()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			// Create more transactions during GC
			for i := 0; i < 3; i++ {
				version, snapshot := manager.BeginWriteTransaction()
				if version != nil && snapshot != nil {
					time.Sleep(10 * time.Millisecond)
					manager.CommitTransaction(version)
					manager.GetVersionTracker().ReleaseSnapshot(snapshot)
				}
			}
		}()

		wg.Wait()

		// Verify snapshots are still valid
		for i, snapshot := range snapshots {
			if snapshot == nil || snapshot.GetRefCount() <= 0 {
				t.Errorf("Snapshot %d should still be valid after GC", i)
			}
		}

		// Clean up
		for _, snapshot := range snapshots {
			if snapshot != nil {
				manager.GetVersionTracker().ReleaseSnapshot(snapshot)
			}
		}
	})
}

func TestConcurrencyStress(t *testing.T) {
	t.Run("HighVolumeOperations", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationReadCommitted)

		var wg sync.WaitGroup
		var readCount, writeCount, commitCount int64
		numOperations := 100

		// Start mixed read/write operations
		for i := 0; i < numOperations; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				
				if index%3 == 0 {
					// Read operation
					snapshot := manager.BeginReadTransaction()
					if snapshot != nil {
						atomic.AddInt64(&readCount, 1)
						time.Sleep(5 * time.Millisecond)
						manager.GetVersionTracker().ReleaseSnapshot(snapshot)
					}
				} else {
					// Write operation
					version, snapshot := manager.BeginWriteTransaction()
					if version != nil && snapshot != nil {
						atomic.AddInt64(&writeCount, 1)
						time.Sleep(5 * time.Millisecond)
						
						err := manager.CommitTransaction(version)
						if err == nil {
							atomic.AddInt64(&commitCount, 1)
						}
						manager.GetVersionTracker().ReleaseSnapshot(snapshot)
					}
				}
			}(i)
		}

		wg.Wait()

		t.Logf("Operations completed - Reads: %d, Writes: %d, Commits: %d", 
			readCount, writeCount, commitCount)

		// Verify reasonable success rates
		expectedReads := int64(numOperations / 3)
		expectedWrites := int64(numOperations * 2 / 3)

		if readCount < expectedReads/2 {
			t.Errorf("Too few successful reads: %d < %d", readCount, expectedReads/2)
		}
		if writeCount < expectedWrites/2 {
			t.Errorf("Too few successful writes: %d < %d", writeCount, expectedWrites/2)
		}
	})

	t.Run("RapidTransactionCycles", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationSerializable)

		var wg sync.WaitGroup
		var successCount int64
		numCycles := 50

		// Rapid transaction creation and cleanup
		for i := 0; i < numCycles; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				
				// Quick read transaction
				readSnapshot := manager.BeginReadTransaction()
				if readSnapshot != nil {
					manager.GetVersionTracker().ReleaseSnapshot(readSnapshot)
					
					// Quick write transaction
					version, writeSnapshot := manager.BeginWriteTransaction()
					if version != nil && writeSnapshot != nil {
						err := manager.CommitTransaction(version)
						if err == nil {
							atomic.AddInt64(&successCount, 1)
						}
						manager.GetVersionTracker().ReleaseSnapshot(writeSnapshot)
					}
				}
			}()
		}

		wg.Wait()

		minExpected := int64(float64(numCycles) * 0.7) // Allow some failures
		if successCount < minExpected {
			t.Errorf("Expected at least %d successful cycles, got %d", minExpected, successCount)
		}
	})
}

func TestDeadlockPrevention(t *testing.T) {
	t.Run("NoDeadlockWithOrderedAccess", func(t *testing.T) {
		manager := transaction.NewMVCCManager()
		manager.SetIsolationLevel(transaction.IsolationSerializable)

		var wg sync.WaitGroup
		var completedCount int64
		timeout := time.After(5 * time.Second)
		done := make(chan bool)

		// Start operations that could potentially deadlock
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				
				// Ordered access to prevent deadlocks
				version, snapshot := manager.BeginWriteTransaction()
				if version != nil && snapshot != nil {
					time.Sleep(time.Duration(index*10) * time.Millisecond)
					
					err := manager.CommitTransaction(version)
					if err == nil {
						atomic.AddInt64(&completedCount, 1)
					}
					manager.GetVersionTracker().ReleaseSnapshot(snapshot)
				}
			}(i)
		}

		go func() {
			wg.Wait()
			done <- true
		}()

		// Wait for completion or timeout
		select {
		case <-done:
			t.Logf("All operations completed successfully: %d", completedCount)
		case <-timeout:
			t.Error("Operations timed out - possible deadlock")
		}
	})
}