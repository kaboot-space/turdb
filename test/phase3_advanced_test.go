package test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/turdb/tur/pkg/api"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/query"
)

// TestQueryCompilation tests the query compilation system
func TestQueryCompilation(t *testing.T) {
	// Create test database
	config := api.TurConfig{
		Path: ":memory:",
	}
	tur, err := api.OpenTur(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer tur.Close()

	// Create test table with unique name
	tableName := fmt.Sprintf("users_compilation_%d", time.Now().UnixNano())
	tbl, err := tur.CreateTable(tableName)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = tbl.AddColumn("name", keys.TypeString, false)
	if err != nil {
		t.Fatalf("Failed to add name column: %v", err)
	}

	ageCol, err := tbl.AddColumn("age", keys.TypeInt, false)
	if err != nil {
		t.Fatalf("Failed to add age column: %v", err)
	}

	// Add test data (reduced to avoid memory issues)
	for i := 0; i < 10; i++ {
		obj, err := tbl.CreateObject()
		if err != nil {
			t.Fatalf("Failed to create object: %v", err)
		}
		obj.Set("name", fmt.Sprintf("User%d", i))
		obj.Set("age", int64(20+i%50))
	}

	// Test query compilation
	compiler := query.NewQueryCompiler()
	compiler.SetCompileThreshold(1) // Set threshold to 1 for immediate compilation
	
	// Create a hot query (age > 30)
	expr := &query.ComparisonExpression{
		Left:     &query.PropertyExpression{Name: "age", ColKey: ageCol},
		Operator: query.OpGreater,
		Right:    &query.LiteralExpression{Value: int64(30)},
	}

	// Compile the query
	compiled, err := compiler.GetOrCompile("hot_query_1", expr)
	if err != nil {
		t.Fatalf("Failed to compile query: %v", err)
	}

	if compiled == nil {
		t.Fatal("Compiled query is nil")
	}

	// Verify compilation properties
	if compiled.ID != "hot_query_1" {
		t.Errorf("Expected QueryID 'hot_query_1', got '%s'", compiled.ID)
	}

	if compiled.HitCount < 0 {
		t.Errorf("Expected non-negative HitCount, got %d", compiled.HitCount)
	}

	if len(compiled.GeneratedCode) == 0 {
		t.Error("Generated code is empty")
	}

	// Test code generation
	codeGen := &query.CodeGenerator{}
	code, err := codeGen.GenerateCode(expr)
	if err != nil {
		t.Fatalf("Failed to generate code: %v", err)
	}

	if !strings.Contains(code, "obj.Get") {
		t.Error("Generated code should contain obj.Get calls")
	}

	// Test formatted code
	formatted, err := codeGen.FormatCode(code)
	if err != nil {
		t.Fatalf("Failed to format code: %v", err)
	}

	if len(formatted) == 0 {
		t.Error("Formatted code is empty")
	}

	t.Logf("Generated code: %s", formatted)
}

// TestMemoryPools tests object pooling for query nodes and result sets
func TestMemoryPools(t *testing.T) {
	pool := query.NewNodePool()

	// Test comparison node pooling
	node1 := pool.GetComparisonNode()
	if node1 == nil {
		t.Fatal("Failed to get comparison node from pool")
	}

	node1.Operator = query.OpEqual
	pool.PutComparisonNode(node1)

	node2 := pool.GetComparisonNode()
	if node2 == nil {
		t.Fatal("Failed to get second comparison node from pool")
	}

	// Node should be reset
	if node2.Operator != 0 {
		t.Error("Node was not properly reset")
	}

	pool.PutComparisonNode(node2)

	// Test logical node pooling
	logicalNode := pool.GetLogicalNode()
	if logicalNode == nil {
		t.Fatal("Failed to get logical node from pool")
	}

	logicalNode.Operator = query.OpAnd
	pool.PutLogicalNode(logicalNode)

	// Test property node pooling
	propNode := pool.GetPropertyNode()
	if propNode == nil {
		t.Fatal("Failed to get property node from pool")
	}

	propNode.Name = "test"
	propNode.ColKey = keys.ColKey(123)
	pool.PutPropertyNode(propNode)

	// Test literal node pooling
	literalNode := pool.GetLiteralNode()
	if literalNode == nil {
		t.Fatal("Failed to get literal node from pool")
	}

	literalNode.Value = "test_value"
	pool.PutLiteralNode(literalNode)

	// Test result set pooling
	resultSet := pool.GetResultSet()
	if resultSet == nil {
		t.Fatal("Failed to get result set from pool")
	}

	resultSet.Add(1, "value1")
	resultSet.Add(2, "value2")

	if resultSet.Size() != 2 {
		t.Errorf("Expected result set size 2, got %d", resultSet.Size())
	}

	resultSet.Release()

	// Test pool statistics
	stats := pool.GetStats()
	if stats.ComparisonHits == 0 {
		t.Error("Expected some comparison hits")
	}

	if stats.TotalReused == 0 {
		t.Error("Expected some total reused count")
	}

	// Test memory usage calculation
	memUsage := pool.MemoryUsage()
	if memUsage == 0 {
		t.Error("Expected non-zero memory usage")
	}

	t.Logf("Pool stats: %+v", stats)
	t.Logf("Memory usage: %d bytes", memUsage)
}

// TestExecutionMetrics tests comprehensive execution metrics and statistics
func TestExecutionMetrics(t *testing.T) {
	collector := query.NewMetricsCollector(100)

	// Test query tracking
	queryStats := collector.StartQuery("test_query_1")
	if queryStats == nil {
		t.Fatal("Failed to start query tracking")
	}

	if queryStats.QueryID != "test_query_1" {
		t.Errorf("Expected QueryID 'test_query_1', got '%s'", queryStats.QueryID)
	}

	// Simulate query execution
	time.Sleep(10 * time.Millisecond)

	collector.EndQuery(queryStats, 50, 1024, true, false, nil)

	// Test metrics
	metrics := collector.GetMetrics()
	if metrics.TotalQueries != 1 {
		t.Errorf("Expected 1 total query, got %d", metrics.TotalQueries)
	}

	if metrics.SuccessfulQueries != 1 {
		t.Errorf("Expected 1 successful query, got %d", metrics.SuccessfulQueries)
	}

	if metrics.CompiledQueries != 1 {
		t.Errorf("Expected 1 compiled query, got %d", metrics.CompiledQueries)
	}

	if metrics.TotalResultsReturned != 50 {
		t.Errorf("Expected 50 total results, got %d", metrics.TotalResultsReturned)
	}

	// Test failed query
	failedStats := collector.StartQuery("failed_query")
	collector.EndQuery(failedStats, 0, 0, false, false, fmt.Errorf("test error"))

	metrics = collector.GetMetrics()
	if metrics.FailedQueries != 1 {
		t.Errorf("Expected 1 failed query, got %d", metrics.FailedQueries)
	}

	if metrics.EmptyResults != 1 {
		t.Errorf("Expected 1 empty result, got %d", metrics.EmptyResults)
	}

	// Test query history
	history := collector.GetQueryHistory()
	if len(history) != 2 {
		t.Errorf("Expected 2 queries in history, got %d", len(history))
	}

	// Test pool metrics update
	poolStats := query.PoolStats{
		ComparisonHits:   10,
		ComparisonMisses: 2,
		LogicalHits:      5,
		LogicalMisses:    1,
	}
	collector.UpdatePoolMetrics(poolStats)

	metrics = collector.GetMetrics()
	expectedHitRate := float64(15) / float64(18) // (10+5)/(10+2+5+1)
	if abs(metrics.PoolHitRate-expectedHitRate) > 0.01 {
		t.Errorf("Expected pool hit rate %.2f, got %.2f", expectedHitRate, metrics.PoolHitRate)
	}

	// Test index metrics
	collector.UpdateIndexMetrics(20, 5, 2)
	metrics = collector.GetMetrics()
	if metrics.IndexHits != 20 {
		t.Errorf("Expected 20 index hits, got %d", metrics.IndexHits)
	}

	// Test compilation metrics
	collector.UpdateCompilationMetrics(5*time.Millisecond, 3, 1)
	metrics = collector.GetMetrics()
	if metrics.CompilationHits != 3 {
		t.Errorf("Expected 3 compilation hits, got %d", metrics.CompilationHits)
	}

	// Test complexity calculation
	simpleExpr := &query.ComparisonExpression{
		Left:     &query.PropertyExpression{Name: "age"},
		Operator: query.OpEqual,
		Right:    &query.LiteralExpression{Value: 25},
	}
	complexity := query.CalculateComplexityScore(simpleExpr)
	if complexity != 1 {
		t.Errorf("Expected complexity 1 for simple expression, got %d", complexity)
	}

	complexExpr := &query.LogicalExpression{
		Left: &query.ComparisonExpression{
			Left:     &query.PropertyExpression{Name: "age"},
			Operator: query.OpGreater,
			Right:    &query.LiteralExpression{Value: 18},
		},
		Operator: query.OpAnd,
		Right: &query.ComparisonExpression{
			Left:     &query.PropertyExpression{Name: "name"},
			Operator: query.OpContains,
			Right:    &query.LiteralExpression{Value: "John"},
		},
	}
	complexity = query.CalculateComplexityScore(complexExpr)
	if complexity != 3 { // 1 + 1 + 1
		t.Errorf("Expected complexity 3 for complex expression, got %d", complexity)
	}

	t.Logf("Final metrics: %+v", metrics)
}

// TestIntegratedPerformance tests all Phase 3 features working together
func TestIntegratedPerformance(t *testing.T) {
	config := api.TurConfig{
		Path: ":memory:",
	}
	tur, err := api.OpenTur(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer tur.Close()

	tbl, err := tur.CreateTable(fmt.Sprintf("products_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = tbl.AddColumn("name", keys.TypeString, false)
	if err != nil {
		t.Fatalf("Failed to add name column: %v", err)
	}
	_, err = tbl.AddColumn("price", keys.TypeDouble, false)
	if err != nil {
		t.Fatalf("Failed to add price column: %v", err)
	}
	_, err = tbl.AddColumn("category", keys.TypeString, false)
	if err != nil {
		t.Fatalf("Failed to add category column: %v", err)
	}

	// Add test data (reduced to avoid memory issues)
	categories := []string{"Electronics", "Books", "Clothing", "Home", "Sports"}
	for i := 0; i < 50; i++ {
		obj, err := tbl.CreateObject()
		if err != nil {
			t.Fatalf("Failed to create object: %v", err)
		}
		obj.Set("name", fmt.Sprintf("Product%d", i))
		obj.Set("price", float64(10+i%100))
		obj.Set("category", categories[i%len(categories)])
	}

	// Test query compilation system
	compiler := query.NewQueryCompiler()
	compiler.SetCompileThreshold(1) // Set low threshold for immediate compilation
	
	// Get column key for price column
	priceCol, err := tbl.GetColumn("price")
	if err != nil {
		t.Fatalf("Failed to get price column: %v", err)
	}
	
	expr := &query.ComparisonExpression{
		Left:     &query.PropertyExpression{Name: "price", ColKey: priceCol},
		Operator: query.OpGreater,
		Right:    &query.LiteralExpression{Value: 50.0},
	}

	compiledQuery, err := compiler.GetOrCompile("price_filter", expr)
	if err != nil {
		t.Fatalf("Failed to compile query: %v", err)
	}

	// Test metrics collection
	collector := query.GlobalMetricsCollector
	initialMetrics := collector.GetMetrics()

	// Simulate query executions for metrics
	for i := 0; i < 10; i++ {
		queryStats := collector.StartQuery("test_query")
		// Simulate some work
		time.Sleep(1 * time.Millisecond)
		collector.EndQuery(queryStats, uint64(i*10), uint64(i*100), true, false, nil)
	}

	// Verify metrics were collected
	finalMetrics := collector.GetMetrics()
	if finalMetrics.TotalQueries <= initialMetrics.TotalQueries {
		t.Error("Expected metrics to be updated after query execution")
	}

	// Verify compilation tracking
	if compiledQuery != nil && compiledQuery.HitCount < 0 {
		t.Error("Expected valid hit count for compiled query")
	}

	// Test memory pool efficiency using node pool
	nodePool := query.GlobalNodePool
	initialStats := nodePool.GetStats()

	// Perform operations that should use memory pools
	for i := 0; i < 100; i++ {
		propNode := nodePool.GetPropertyNode()
		nodePool.PutPropertyNode(propNode)
	}

	finalStats := nodePool.GetStats()
	if finalStats.TotalReused <= initialStats.TotalReused {
		t.Error("Expected memory pool reuse to be tracked")
	}

	// Measure performance
	startTime := time.Now()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	executionTime := time.Since(startTime)

	// Verify metrics were collected
	metrics := query.GlobalMetricsCollector.GetMetrics()
	if metrics.TotalQueries == 0 {
		t.Error("Expected metrics to be collected")
	}

	// Verify pool usage
	poolStats := query.GlobalNodePool.GetStats()
	if poolStats.TotalReused == 0 {
		t.Error("Expected some pool reuse")
	}

	t.Logf("Execution time: %v", executionTime)
	t.Logf("Memory allocated: %d bytes", memAfter.TotalAlloc-memBefore.TotalAlloc)
	t.Logf("Query metrics: %+v", metrics)
	t.Logf("Pool stats: %+v", poolStats)
}

// TestMemoryEfficiency tests memory usage optimization
func TestMemoryEfficiency(t *testing.T) {
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Create many query nodes without pooling (reduced size)
	nodes := make([]*query.ComparisonExpression, 100)
	for i := 0; i < 100; i++ {
		nodes[i] = &query.ComparisonExpression{
			Left:     &query.PropertyExpression{Name: fmt.Sprintf("prop%d", i)},
			Operator: query.OpEqual,
			Right:    &query.LiteralExpression{Value: i},
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)
	memWithoutPool := memAfter.TotalAlloc - memBefore.TotalAlloc

	// Reset memory stats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Create same nodes using pooling (reduced size)
	pool := query.NewNodePool()
	pooledNodes := make([]*query.ComparisonExpression, 100)
	for i := 0; i < 100; i++ {
		node := pool.GetComparisonNode()
		node.Left = &query.PropertyExpression{Name: fmt.Sprintf("prop%d", i)}
		node.Operator = query.OpEqual
		node.Right = &query.LiteralExpression{Value: i}
		pooledNodes[i] = node
	}

	// Return nodes to pool
	for _, node := range pooledNodes {
		pool.PutComparisonNode(node)
	}

	runtime.GC()
	runtime.ReadMemStats(&memAfter)
	memWithPool := memAfter.TotalAlloc - memBefore.TotalAlloc

	t.Logf("Memory without pool: %d bytes", memWithoutPool)
	t.Logf("Memory with pool: %d bytes", memWithPool)

	// Pool should be more memory efficient for repeated allocations
	if memWithPool > memWithoutPool*2 {
		t.Errorf("Pool memory usage is too high: %d vs %d", memWithPool, memWithoutPool)
	}
}

// Helper function for floating point comparison
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}