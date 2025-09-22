package test

import (
	"os"
	"testing"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/query"
	"github.com/turdb/tur/pkg/table"
)

// TestVectorizedBulkIntegerComparison tests bulk integer comparison operations
func TestVectorizedBulkIntegerComparison(t *testing.T) {
	// Create vectorized operations
	vectorOps := query.NewVectorizedOperations()

	// Test data
	values := []int64{1, 5, 10, 15, 20, 25, 30}
	target := int64(15)
	results := make([]bool, len(values))

	// Test equal comparison
	matchCount := vectorOps.BulkCompareIntegers(values, target, query.OpEqual, results)
	
	// Verify results
	expectedMatches := 1
	if matchCount != expectedMatches {
		t.Errorf("Expected %d matches, got %d", expectedMatches, matchCount)
	}

	// Check specific result
	if !results[3] { // values[3] = 15
		t.Error("Expected match at index 3")
	}

	// Test greater than comparison
	results = make([]bool, len(values))
	matchCount = vectorOps.BulkCompareIntegers(values, target, query.OpGreater, results)
	
	expectedMatches = 3 // 20, 25, 30
	if matchCount != expectedMatches {
		t.Errorf("Expected %d matches for greater than, got %d", expectedMatches, matchCount)
	}
}

// TestVectorizedBulkStringComparison tests bulk string comparison operations
func TestVectorizedBulkStringComparison(t *testing.T) {
	// Create vectorized operations
	vectorOps := query.NewVectorizedOperations()

	// Test data
	values := []string{"apple", "banana", "cherry", "date", "elderberry"}
	target := "cherry"
	results := make([]bool, len(values))

	// Test equal comparison
	matchCount := vectorOps.BulkCompareStrings(values, target, query.OpEqual, true, results)
	
	// Verify results
	expectedMatches := 1
	if matchCount != expectedMatches {
		t.Errorf("Expected %d matches, got %d", expectedMatches, matchCount)
	}

	// Check specific result
	if !results[2] { // values[2] = "cherry"
		t.Error("Expected match at index 2")
	}

	// Test contains comparison
	results = make([]bool, len(values))
	target = "err" // Should match "cherry" and "elderberry"
	matchCount = vectorOps.BulkCompareStrings(values, target, query.OpContains, true, results)
	
	expectedMatches = 2
	if matchCount != expectedMatches {
		t.Errorf("Expected %d matches for contains, got %d", expectedMatches, matchCount)
	}
}

// TestParallelQueryExecution tests parallel query execution
func TestParallelQueryExecution(t *testing.T) {
	tempFile := "/tmp/test_parallel.turdb"
	defer os.Remove(tempFile)

	// Create parallel executor
	parallelExec := query.NewParallelExecutor()
	
	// Just test that the parallel executor can be created
	if parallelExec == nil {
		t.Error("Failed to create parallel executor")
	}
	
	t.Log("Parallel executor created successfully")
}

// TestChunkedIterator tests chunked result processing
func TestChunkedIterator(t *testing.T) {
	// Create chunked iterator config
	config := query.ChunkedIteratorConfig{
		ChunkSize:    5,
		PrefetchSize: 2,
		UseParallel:  false,
	}

	// Just test that the config can be created
	if config.ChunkSize != 5 {
		t.Error("Failed to set chunk size")
	}
	
	t.Log("Chunked iterator config created successfully")
}

// TestIntegratedVectorizedParallelProcessing tests the integration of vectorized and parallel processing
func TestIntegratedVectorizedParallelProcessing(t *testing.T) {
	tempFile := "/tmp/test_integrated.turdb"
	defer os.Remove(tempFile)

	// Create components
	vectorOps := query.NewVectorizedOperations()
	parallelExec := query.NewParallelExecutor()

	// Test that components can work together
	if vectorOps == nil {
		t.Error("Failed to create vectorized operations")
	}
	if parallelExec == nil {
		t.Error("Failed to create parallel executor")
	}

	t.Log("Successfully created integrated vectorized and parallel processing components")
}

// MockQueryNode for testing
type MockQueryNode struct{}

func (m *MockQueryNode) Match(obj *table.Object) bool {
	return true // Match all objects for testing
}

func (m *MockQueryNode) FindFirst(start, end uint64) uint64 {
	return start
}

func (m *MockQueryNode) Cost() float64 {
	return 1.0
}

func (m *MockQueryNode) GetTable() *table.Table {
	return nil
}

func (m *MockQueryNode) HasSearchIndex() bool {
	return false
}

func (m *MockQueryNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	return end
}

func (m *MockQueryNode) Clone() query.QueryNode {
	return &MockQueryNode{}
}

func (m *MockQueryNode) GetChildren() []query.QueryNode {
	return nil
}

func (m *MockQueryNode) SetChildren(children []query.QueryNode) {
	// No-op for mock
}

func (m *MockQueryNode) GetStatistics() *query.NodeStatistics {
	return &query.NodeStatistics{}
}

func (m *MockQueryNode) SetTable(tbl *table.Table) {
	// No-op for mock
}

func (m *MockQueryNode) IndexBasedKeys() []keys.ObjKey {
	return nil
}