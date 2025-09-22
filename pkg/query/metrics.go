package query

import (
	"sync"
	"sync/atomic"
	"time"
)

// ExecutionMetrics tracks comprehensive query execution statistics
type ExecutionMetrics struct {
	// Query execution counters
	TotalQueries        uint64
	SuccessfulQueries   uint64
	FailedQueries       uint64
	CachedQueries       uint64
	CompiledQueries     uint64
	
	// Timing statistics
	TotalExecutionTime  int64 // nanoseconds
	MinExecutionTime    int64
	MaxExecutionTime    int64
	AvgExecutionTime    int64
	
	// Memory statistics
	TotalMemoryUsed     uint64
	PeakMemoryUsed      uint64
	PoolHitRate         float64
	PoolMissRate        float64
	
	// Query complexity metrics
	SimpleQueries       uint64 // Single condition
	ComplexQueries      uint64 // Multiple conditions
	DeepQueries         uint64 // Nested expressions
	
	// Result set statistics
	TotalResultsReturned uint64
	EmptyResults         uint64
	LargeResults         uint64 // > 1000 results
	
	// Index usage statistics
	IndexHits           uint64
	IndexMisses         uint64
	FullTableScans      uint64
	
	// Compilation statistics
	CompilationTime     int64
	CompilationHits     uint64
	CompilationMisses   uint64
	
	mutex sync.RWMutex
	startTime time.Time
}

// QueryStats represents statistics for a single query execution
type QueryStats struct {
	QueryID         string
	StartTime       time.Time
	EndTime         time.Time
	ExecutionTime   time.Duration
	ResultCount     uint64
	MemoryUsed      uint64
	IndexesUsed     []string
	Compiled        bool
	FromCache       bool
	ErrorMessage    string
	ComplexityScore int
}

// ResourceUsage tracks system resource consumption
type ResourceUsage struct {
	CPUTime         time.Duration
	MemoryAllocated uint64
	MemoryFreed     uint64
	GCPauses        uint64
	GoroutineCount  int
}

// MetricsCollector manages query execution metrics
type MetricsCollector struct {
	metrics     *ExecutionMetrics
	queryStats  []QueryStats
	maxHistory  int
	enabled     bool
	mu          sync.RWMutex
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(maxHistory int) *MetricsCollector {
	return &MetricsCollector{
		metrics: &ExecutionMetrics{
			startTime:        time.Now(),
			MinExecutionTime: int64(^uint64(0) >> 1), // Max int64
		},
		queryStats: make([]QueryStats, 0, maxHistory),
		maxHistory: maxHistory,
		enabled:    true,
	}
}

// StartQuery begins tracking a query execution
func (mc *MetricsCollector) StartQuery(queryID string) *QueryStats {
	if !mc.enabled {
		return nil
	}
	
	return &QueryStats{
		QueryID:   queryID,
		StartTime: time.Now(),
	}
}

// EndQuery completes tracking a query execution
func (mc *MetricsCollector) EndQuery(stats *QueryStats, resultCount uint64, memoryUsed uint64, compiled bool, fromCache bool, err error) {
	if !mc.enabled || stats == nil {
		return
	}
	
	stats.EndTime = time.Now()
	stats.ExecutionTime = stats.EndTime.Sub(stats.StartTime)
	stats.ResultCount = resultCount
	stats.MemoryUsed = memoryUsed
	stats.Compiled = compiled
	stats.FromCache = fromCache
	
	if err != nil {
		stats.ErrorMessage = err.Error()
	}
	
	mc.updateMetrics(stats)
	mc.addQueryStats(stats)
}

// updateMetrics updates global execution metrics
func (mc *MetricsCollector) updateMetrics(stats *QueryStats) {
	mc.metrics.mutex.Lock()
	defer mc.metrics.mutex.Unlock()
	
	atomic.AddUint64(&mc.metrics.TotalQueries, 1)
	
	if stats.ErrorMessage == "" {
		atomic.AddUint64(&mc.metrics.SuccessfulQueries, 1)
	} else {
		atomic.AddUint64(&mc.metrics.FailedQueries, 1)
	}
	
	if stats.FromCache {
		atomic.AddUint64(&mc.metrics.CachedQueries, 1)
	}
	
	if stats.Compiled {
		atomic.AddUint64(&mc.metrics.CompiledQueries, 1)
	}
	
	// Update timing statistics
	execTimeNs := stats.ExecutionTime.Nanoseconds()
	atomic.AddInt64(&mc.metrics.TotalExecutionTime, execTimeNs)
	
	if execTimeNs < mc.metrics.MinExecutionTime {
		mc.metrics.MinExecutionTime = execTimeNs
	}
	
	if execTimeNs > mc.metrics.MaxExecutionTime {
		mc.metrics.MaxExecutionTime = execTimeNs
	}
	
	// Calculate average execution time
	totalQueries := atomic.LoadUint64(&mc.metrics.TotalQueries)
	if totalQueries > 0 {
		mc.metrics.AvgExecutionTime = mc.metrics.TotalExecutionTime / int64(totalQueries)
	}
	
	// Update memory statistics
	atomic.AddUint64(&mc.metrics.TotalMemoryUsed, stats.MemoryUsed)
	if stats.MemoryUsed > mc.metrics.PeakMemoryUsed {
		mc.metrics.PeakMemoryUsed = stats.MemoryUsed
	}
	
	// Update result statistics
	atomic.AddUint64(&mc.metrics.TotalResultsReturned, stats.ResultCount)
	
	if stats.ResultCount == 0 {
		atomic.AddUint64(&mc.metrics.EmptyResults, 1)
	} else if stats.ResultCount > 1000 {
		atomic.AddUint64(&mc.metrics.LargeResults, 1)
	}
	
	// Update complexity metrics based on complexity score
	switch {
	case stats.ComplexityScore <= 1:
		atomic.AddUint64(&mc.metrics.SimpleQueries, 1)
	case stats.ComplexityScore <= 5:
		atomic.AddUint64(&mc.metrics.ComplexQueries, 1)
	default:
		atomic.AddUint64(&mc.metrics.DeepQueries, 1)
	}
}

// addQueryStats adds query statistics to history
func (mc *MetricsCollector) addQueryStats(stats *QueryStats) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	if len(mc.queryStats) >= mc.maxHistory {
		// Remove oldest entry
		copy(mc.queryStats, mc.queryStats[1:])
		mc.queryStats = mc.queryStats[:len(mc.queryStats)-1]
	}
	
	mc.queryStats = append(mc.queryStats, *stats)
}

// GetMetrics returns current execution metrics
func (mc *MetricsCollector) GetMetrics() ExecutionMetrics {
	mc.metrics.mutex.RLock()
	defer mc.metrics.mutex.RUnlock()
	return *mc.metrics
}

// GetQueryHistory returns recent query statistics
func (mc *MetricsCollector) GetQueryHistory() []QueryStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	history := make([]QueryStats, len(mc.queryStats))
	copy(history, mc.queryStats)
	return history
}

// UpdatePoolMetrics updates memory pool statistics
func (mc *MetricsCollector) UpdatePoolMetrics(poolStats PoolStats) {
	mc.metrics.mutex.Lock()
	defer mc.metrics.mutex.Unlock()
	
	totalHits := poolStats.ComparisonHits + poolStats.LogicalHits + poolStats.PropertyHits + poolStats.LiteralHits + poolStats.ResultSetHits
	totalMisses := poolStats.ComparisonMisses + poolStats.LogicalMisses + poolStats.PropertyMisses + poolStats.LiteralMisses + poolStats.ResultSetMisses
	total := totalHits + totalMisses
	
	if total > 0 {
		mc.metrics.PoolHitRate = float64(totalHits) / float64(total)
		mc.metrics.PoolMissRate = float64(totalMisses) / float64(total)
	}
}

// UpdateIndexMetrics updates index usage statistics
func (mc *MetricsCollector) UpdateIndexMetrics(hits, misses, fullScans uint64) {
	atomic.AddUint64(&mc.metrics.IndexHits, hits)
	atomic.AddUint64(&mc.metrics.IndexMisses, misses)
	atomic.AddUint64(&mc.metrics.FullTableScans, fullScans)
}

// UpdateCompilationMetrics updates query compilation statistics
func (mc *MetricsCollector) UpdateCompilationMetrics(compilationTime time.Duration, hits, misses uint64) {
	atomic.AddInt64(&mc.metrics.CompilationTime, compilationTime.Nanoseconds())
	atomic.AddUint64(&mc.metrics.CompilationHits, hits)
	atomic.AddUint64(&mc.metrics.CompilationMisses, misses)
}

// Reset clears all metrics and statistics
func (mc *MetricsCollector) Reset() {
	mc.metrics.mutex.Lock()
	mc.mu.Lock()
	defer mc.metrics.mutex.Unlock()
	defer mc.mu.Unlock()
	
	mc.metrics = &ExecutionMetrics{
		startTime:        time.Now(),
		MinExecutionTime: int64(^uint64(0) >> 1), // Max int64
	}
	mc.queryStats = mc.queryStats[:0]
}

// Enable enables metrics collection
func (mc *MetricsCollector) Enable() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.enabled = true
}

// Disable disables metrics collection
func (mc *MetricsCollector) Disable() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.enabled = false
}

// IsEnabled returns whether metrics collection is enabled
func (mc *MetricsCollector) IsEnabled() bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.enabled
}

// CalculateComplexityScore calculates query complexity based on expression structure
func CalculateComplexityScore(expr QueryExpression) int {
	if expr == nil {
		return 0
	}
	
	switch e := expr.(type) {
	case *ComparisonExpression:
		return 1
	case *LogicalExpression:
		leftScore := CalculateComplexityScore(e.Left)
		rightScore := CalculateComplexityScore(e.Right)
		return 1 + leftScore + rightScore
	case *PropertyExpression:
		return 1
	case *LiteralExpression:
		return 0
	default:
		return 1
	}
}

// Global metrics collector instance
var GlobalMetricsCollector = NewMetricsCollector(1000)