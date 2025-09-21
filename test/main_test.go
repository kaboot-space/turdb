package test

import (
	"testing"
)

// TestMain sets up the test environment
func TestMain(m *testing.M) {
	// Setup code here if needed

	// Run tests
	m.Run()

	// Cleanup code here if needed
}

// TestAllFeatures runs a comprehensive test of all implemented features
func TestAllFeatures(t *testing.T) {
	// Run all test suites
	t.Run("BasicOperations", TestBasicTableOperations)
	t.Run("AllDataTypes", TestAllDataTypes)
	t.Run("CollectionTypes", TestCollectionTypes)
	t.Run("ColumnAttributes", TestColumnAttributes)
	t.Run("LinkTypes", TestLinkTypes)
	t.Run("Validation", TestValidation)
	t.Run("SchemaOperations", TestSchemaOperations)
	
	// MVCC and Transaction test suites
	t.Run("TransactionCreation", TestTransactionCreation)
	t.Run("IsolationLevels", TestIsolationLevels)
	t.Run("ConcurrentReads", TestConcurrentReads)
}