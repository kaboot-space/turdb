package test

import (
	"testing"
	"time"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/storage"
	"github.com/turdb/tur/pkg/transaction"
)

// MockDBRef implements DBRef interface for testing
type MockDBRef struct {
	version       uint64
	writeLockHeld bool
}

func (m *MockDBRef) DoCommit(t *transaction.Transaction) (uint64, error) {
	m.version++
	return m.version, nil
}

func (m *MockDBRef) GetVersion() uint64 {
	return m.version
}

func (m *MockDBRef) GetVersionOfLatestSnapshot() uint64 {
	return m.version
}

func (m *MockDBRef) AcquireWriteLock() error {
	m.writeLockHeld = true
	return nil
}

func (m *MockDBRef) ReleaseWriteLock() error {
	m.writeLockHeld = false
	return nil
}

func (m *MockDBRef) IsWriteLockHeld() bool {
	return m.writeLockHeld
}

func TestTransactionCreation(t *testing.T) {
	t.Run("NewTransaction", func(t *testing.T) {
		db := &MockDBRef{version: 1}
		baseAlloc := core.NewAllocator(nil)
		alloc := core.NewSlabAlloc(baseAlloc)
		readLock := &transaction.ReadLockInfo{
			Version:   1,
			ReaderID:  1,
			Timestamp: time.Now().Unix(),
		}

		txn, err := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		if err != nil {
			t.Fatalf("Failed to create transaction: %v", err)
		}

		if txn == nil {
			t.Fatal("Transaction should not be nil")
		}

		if txn.GetVersion() == 0 {
			t.Error("Transaction should have a valid version")
		}
	})

	t.Run("NewTransactionWithNilDB", func(t *testing.T) {
		baseAlloc := core.NewAllocator(nil)
		alloc := core.NewSlabAlloc(baseAlloc)
		readLock := &transaction.ReadLockInfo{
			Version:   1,
			ReaderID:  1,
			Timestamp: time.Now().Unix(),
		}

		_, err := transaction.NewTransaction(nil, alloc, readLock, transaction.TransactionStageReady)
		if err == nil {
			t.Error("Should fail with nil database reference")
		}
	})

	t.Run("NewTransactionWithNilAlloc", func(t *testing.T) {
		db := &MockDBRef{version: 1}
		readLock := &transaction.ReadLockInfo{
			Version:   1,
			ReaderID:  1,
			Timestamp: time.Now().Unix(),
		}

		_, err := transaction.NewTransaction(db, nil, readLock, transaction.TransactionStageReady)
		if err == nil {
			t.Error("Should fail with nil allocator")
		}
	})
}

func TestTransactionStages(t *testing.T) {
	db := &MockDBRef{version: 1}
	baseAlloc := core.NewAllocator(nil)
	alloc := core.NewSlabAlloc(baseAlloc)
	readLock := &transaction.ReadLockInfo{
		Version:   1,
		ReaderID:  1,
		Timestamp: time.Now().Unix(),
	}

	t.Run("InitialStage", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		if txn.GetTransactionStage() != transaction.TransactionStageReady {
			t.Error("Initial transaction stage should be Ready")
		}
	})

	t.Run("PromoteToWrite", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReading)
		
		err := txn.PromoteToWrite()
		if err != nil {
			t.Errorf("Failed to promote to write: %v", err)
		}

		if txn.GetTransactionStage() != transaction.TransactionStageWriting {
			t.Error("Transaction stage should be Writing after promotion")
		}
	})
}

func TestTransactionCommitRollback(t *testing.T) {
	db := &MockDBRef{version: 1}
	baseAlloc := core.NewAllocator(nil)
	alloc := core.NewSlabAlloc(baseAlloc)
	readLock := &transaction.ReadLockInfo{
		Version:   1,
		ReaderID:  1,
		Timestamp: time.Now().Unix(),
	}

	t.Run("CommitReadTransaction", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReading)
		
		version, err := txn.Commit()
		if err != nil {
			t.Errorf("Failed to commit read transaction: %v", err)
		}

		if version == 0 {
			t.Error("Commit should return valid version")
		}
	})

	t.Run("CommitWriteTransaction", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReading)
		
		err := txn.PromoteToWrite()
		if err != nil {
			t.Fatalf("Failed to promote to write: %v", err)
		}

		version, err := txn.Commit()
		if err != nil {
			t.Errorf("Failed to commit write transaction: %v", err)
		}

		if version == 0 {
			t.Error("Commit should return valid version")
		}
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReading)
		
		err := txn.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback transaction: %v", err)
		}
	})
}

func TestTransactionMVCCIntegration(t *testing.T) {
	db := &MockDBRef{version: 1}
	baseAlloc := core.NewAllocator(nil)
	alloc := core.NewSlabAlloc(baseAlloc)
	readLock := &transaction.ReadLockInfo{
		Version:   1,
		ReaderID:  1,
		Timestamp: time.Now().Unix(),
	}

	t.Run("MVCCVersionTracking", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		// Set MVCC version
		version := transaction.NewVersionID(1)
		txn.SetMVCCVersion(version)

		retrievedVersion := txn.GetMVCCVersion()
		if retrievedVersion == nil {
			t.Fatal("MVCC version should not be nil")
		}

		if retrievedVersion.ID != version.ID {
			t.Error("Retrieved MVCC version should match set version")
		}
	})

	t.Run("SnapshotTracking", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		// Create and set snapshot
		version := transaction.NewVersionID(1)
		snapshot := transaction.NewSnapshot(version)
		txn.SetSnapshot(snapshot)

		retrievedSnapshot := txn.GetSnapshot()
		if retrievedSnapshot == nil {
			t.Fatal("Snapshot should not be nil")
		}

		if retrievedSnapshot.GetVersion().ID != version.ID {
			t.Error("Retrieved snapshot should match set snapshot")
		}
	})

	t.Run("VersionVisibility", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		// Set transaction snapshot
		snapshotVersion := transaction.NewVersionID(5)
		snapshot := transaction.NewSnapshot(snapshotVersion)
		txn.SetSnapshot(snapshot)

		// Test version visibility
		olderVersion := transaction.NewVersionID(3)
		newerVersion := transaction.NewVersionID(7)

		if !txn.IsVersionVisible(olderVersion) {
			t.Error("Older version should be visible")
		}

		if txn.IsVersionVisible(newerVersion) {
			t.Error("Newer version should not be visible")
		}
	})
}

func TestTransactionProperties(t *testing.T) {
	db := &MockDBRef{version: 1}
	baseAlloc := core.NewAllocator(nil)
	alloc := core.NewSlabAlloc(baseAlloc)
	readLock := &transaction.ReadLockInfo{
		Version:   1,
		ReaderID:  1,
		Timestamp: time.Now().Unix(),
	}

	t.Run("GetAllocator", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		retrievedAlloc := txn.GetAllocator()
		if retrievedAlloc != alloc {
			t.Error("Retrieved allocator should match original")
		}
	})

	t.Run("GetReadLockInfo", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		retrievedLock := txn.GetReadLockInfo()
		if retrievedLock != readLock {
			t.Error("Retrieved read lock should match original")
		}
	})

	t.Run("IsAttached", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		// Initially should be attached
		if !txn.IsAttached() {
			t.Error("Transaction should be attached initially")
		}
	})

	t.Run("HoldsWriteMutex", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReading)
		
		// Initially should not hold write mutex
		if txn.HoldsWriteMutex() {
			t.Error("Read transaction should not hold write mutex")
		}

		// After promotion should hold write mutex
		txn.PromoteToWrite()
		if !txn.HoldsWriteMutex() {
			t.Error("Write transaction should hold write mutex")
		}
	})

	t.Run("GetCommitSize", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		commitSize := txn.GetCommitSize()
		if commitSize < 0 {
			t.Error("Commit size should be non-negative")
		}
	})
}

func TestTransactionGroupIntegration(t *testing.T) {
	db := &MockDBRef{version: 1}
	baseAlloc := core.NewAllocator(nil)
	alloc := core.NewSlabAlloc(baseAlloc)
	readLock := &transaction.ReadLockInfo{
		Version:   1,
		ReaderID:  1,
		Timestamp: time.Now().Unix(),
	}

	t.Run("SetGetGroup", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		group := &storage.Group{}
		txn.SetGroup(group)

		retrievedGroup := txn.GetGroup()
		if retrievedGroup != group {
			t.Error("Retrieved group should match set group")
		}
	})
}

func TestTransactionAsyncOperations(t *testing.T) {
	db := &MockDBRef{version: 1}
	baseAlloc := core.NewAllocator(nil)
	alloc := core.NewSlabAlloc(baseAlloc)
	readLock := &transaction.ReadLockInfo{
		Version:   1,
		ReaderID:  1,
		Timestamp: time.Now().Unix(),
	}

	t.Run("PromoteToAsync", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageWriting)
		
		err := txn.PromoteToAsync()
		if err != nil {
			t.Errorf("Failed to promote to async: %v", err)
		}
	})

	t.Run("IsSynchronizing", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		// Initially should not be synchronizing
		if txn.IsSynchronizing() {
			t.Error("Transaction should not be synchronizing initially")
		}
	})

	t.Run("AsyncCompleteWrites", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageWriting)
		
		// First promote to async state
		err := txn.PromoteToAsync()
		if err != nil {
			t.Errorf("Failed to promote to async: %v", err)
		}
		
		err = txn.AsyncCompleteWrites()
		if err != nil {
			t.Errorf("Failed to complete async writes: %v", err)
		}
	})
}

func TestTransactionClose(t *testing.T) {
	db := &MockDBRef{version: 1}
	baseAlloc := core.NewAllocator(nil)
	alloc := core.NewSlabAlloc(baseAlloc)
	readLock := &transaction.ReadLockInfo{
		Version:   1,
		ReaderID:  1,
		Timestamp: time.Now().Unix(),
	}

	t.Run("CloseTransaction", func(t *testing.T) {
		txn, _ := transaction.NewTransaction(db, alloc, readLock, transaction.TransactionStageReady)
		
		err := txn.Close()
		if err != nil {
			t.Errorf("Failed to close transaction: %v", err)
		}

		// After close, should not be attached
		if txn.IsAttached() {
			t.Error("Transaction should not be attached after close")
		}
	})
}