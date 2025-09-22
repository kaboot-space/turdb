package query

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/table"
)

// ParallelExecutor handles parallel query execution
// Based on realm-core's parallel query processing patterns
type ParallelExecutor struct {
	numWorkers    int
	chunkSize     int64
	resultPool    sync.Pool
	workerPool    sync.Pool
	vectorizedOps *VectorizedOperations
}

// NewParallelExecutor creates a new parallel executor
func NewParallelExecutor() *ParallelExecutor {
	numCPU := runtime.NumCPU()
	return &ParallelExecutor{
		numWorkers:    numCPU,
		chunkSize:     1024, // Based on realm-core's optimal chunk size
		vectorizedOps: NewVectorizedOperations(),
		resultPool: sync.Pool{
			New: func() interface{} {
				return make([]keys.ObjKey, 0, 256)
			},
		},
		workerPool: sync.Pool{
			New: func() interface{} {
				return &ParallelWorker{}
			},
		},
	}
}

// ParallelWorker represents a worker goroutine for parallel execution
type ParallelWorker struct {
	id      int
	results []keys.ObjKey
	matches uint64
}

// WorkerTask represents a task for parallel execution
type WorkerTask struct {
	startRow uint64
	endRow   uint64
	node     QueryNode
	table    *table.Table
	results  chan<- []keys.ObjKey
	matches  *uint64
}

// ExecuteParallel executes a query node in parallel across multiple goroutines
// Based on realm-core's parallel query execution pattern
func (pe *ParallelExecutor) ExecuteParallel(
	ctx context.Context,
	node QueryNode,
	tbl *table.Table,
	startRow, endRow uint64,
) ([]keys.ObjKey, error) {
	totalRows := endRow - startRow
	if totalRows == 0 {
		return nil, nil
	}

	// Calculate optimal chunk size based on total rows and worker count
	chunkSize := pe.calculateChunkSize(totalRows)
	numChunks := (totalRows + chunkSize - 1) / chunkSize

	// Create result channel and worker pool
	resultChan := make(chan []keys.ObjKey, numChunks)
	var totalMatches uint64
	var wg sync.WaitGroup

	// Launch worker goroutines
	for chunk := uint64(0); chunk < numChunks; chunk++ {
		chunkStart := startRow + chunk*chunkSize
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > endRow {
			chunkEnd = endRow
		}

		wg.Add(1)
		go pe.executeChunk(ctx, &wg, WorkerTask{
			startRow: chunkStart,
			endRow:   chunkEnd,
			node:     node,
			table:    tbl,
			results:  resultChan,
			matches:  &totalMatches,
		})
	}

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results from all workers
	var allResults []keys.ObjKey
	for chunkResults := range resultChan {
		allResults = append(allResults, chunkResults...)
		// Return chunk results to pool
		pe.resultPool.Put(chunkResults[:0])
	}

	return allResults, nil
}

// executeChunk executes a single chunk of the query in a worker goroutine
func (pe *ParallelExecutor) executeChunk(
	ctx context.Context,
	wg *sync.WaitGroup,
	task WorkerTask,
) {
	defer wg.Done()

	// Get worker from pool
	worker := pe.workerPool.Get().(*ParallelWorker)
	defer pe.workerPool.Put(worker)

	// Get result slice from pool
	results := pe.resultPool.Get().([]keys.ObjKey)
	defer func() {
		// Send results to channel before returning to pool
		if len(results) > 0 {
			// Make a copy since we're returning the slice to pool
			resultsCopy := make([]keys.ObjKey, len(results))
			copy(resultsCopy, results)
			task.results <- resultsCopy
		}
	}()

	// Execute the chunk using vectorized operations when possible
	chunkMatches := pe.executeChunkVectorized(ctx, task, results)
	
	// Update total match count atomically
	atomic.AddUint64(task.matches, chunkMatches)
}

// executeChunkVectorized executes a chunk using vectorized operations when possible
func (pe *ParallelExecutor) executeChunkVectorized(
	ctx context.Context,
	task WorkerTask,
	results []keys.ObjKey,
) uint64 {
	var matches uint64

	// Check for cancellation
	select {
	case <-ctx.Done():
		return 0
	default:
	}

	// Try to use vectorized operations for comparison nodes
	if compNode, ok := task.node.(*ComparisonQueryNode); ok {
		matches = pe.executeVectorizedComparison(compNode, task, results)
	} else {
		// Fallback to standard node execution
		matches = pe.executeStandardChunk(task, results)
	}

	return matches
}

// executeVectorizedComparison executes comparison using vectorized operations
func (pe *ParallelExecutor) executeVectorizedComparison(
	node *ComparisonQueryNode,
	task WorkerTask,
	results []keys.ObjKey,
) uint64 {
	// Extract comparison details
	expr := node.expression
	if prop, ok := expr.Left.(*PropertyExpression); ok {
		return pe.executeVectorizedProperty(prop, expr, task, results)
	}

	// Fallback to standard execution
	return pe.executeStandardChunk(task, results)
}

// executeVectorizedProperty executes property comparison using vectorized operations
func (pe *ParallelExecutor) executeVectorizedProperty(
	prop *PropertyExpression,
	expr *ComparisonExpression,
	task WorkerTask,
	results []keys.ObjKey,
) uint64 {
	var matches uint64

	// Get column data for vectorized processing
	colKey := prop.ColKey

	// Try to get bulk data for vectorized operations
	if intValues, ok := pe.getBulkIntegerData(task.table, colKey, task.startRow, task.endRow); ok {
		if target, ok := expr.Right.(*LiteralExpression); ok {
			if targetInt, ok := target.Value.(int64); ok {
				// Use vectorized integer comparison
				resultFlags := make([]bool, len(intValues))
				matchCount := pe.vectorizedOps.BulkCompareIntegers(
					intValues, targetInt, expr.Operator, resultFlags,
				)

				// Collect matching row indices
				for i, match := range resultFlags {
					if match {
						objKey := keys.NewObjKey(task.startRow + uint64(i))
						results = append(results, objKey)
						matches++
					}
				}
				return uint64(matchCount)
			}
		}
	}

	// Try string vectorization
	if strValues, ok := pe.getBulkStringData(task.table, colKey, task.startRow, task.endRow); ok {
		if target, ok := expr.Right.(*LiteralExpression); ok {
			if targetStr, ok := target.Value.(string); ok {
				// Use vectorized string comparison
				resultFlags := make([]bool, len(strValues))
				matchCount := pe.vectorizedOps.BulkCompareStrings(
					strValues, targetStr, expr.Operator, true, resultFlags,
				)

				// Collect matching row indices
				for i, match := range resultFlags {
					if match {
						objKey := keys.NewObjKey(task.startRow + uint64(i))
						results = append(results, objKey)
						matches++
					}
				}
				return uint64(matchCount)
			}
		}
	}

	// Fallback to row-by-row processing
	return pe.executeStandardChunk(task, results)
}

// executeStandardChunk executes chunk using standard row-by-row processing
func (pe *ParallelExecutor) executeStandardChunk(
	task WorkerTask,
	results []keys.ObjKey,
) uint64 {
	var matches uint64

	for row := task.startRow; row < task.endRow; row++ {
		objKey := keys.NewObjKey(row)
		obj, err := task.table.GetObject(objKey)
		if err != nil {
			continue
		}

		if task.node.Match(obj) {
			results = append(results, objKey)
			matches++
		}
	}

	return matches
}

// getBulkIntegerData attempts to get bulk integer data for vectorized operations
func (pe *ParallelExecutor) getBulkIntegerData(
	tbl *table.Table,
	colKey keys.ColKey,
	startRow, endRow uint64,
) ([]int64, bool) {
	rowCount := endRow - startRow
	values := make([]int64, 0, rowCount)

	// Try to extract integer values in bulk
	for row := startRow; row < endRow; row++ {
		objKey := keys.NewObjKey(row)
		obj, err := tbl.GetObject(objKey)
		if err != nil {
			return nil, false
		}

		val, err := obj.Get(colKey)
		if err != nil {
			return nil, false
		}

		if intVal, ok := val.(int64); ok {
			values = append(values, intVal)
		} else {
			return nil, false
		}
	}

	return values, true
}

// getBulkStringData attempts to get bulk string data for vectorized operations
func (pe *ParallelExecutor) getBulkStringData(
	tbl *table.Table,
	colKey keys.ColKey,
	startRow, endRow uint64,
) ([]string, bool) {
	rowCount := endRow - startRow
	values := make([]string, 0, rowCount)

	// Try to extract string values in bulk
	for row := startRow; row < endRow; row++ {
		objKey := keys.NewObjKey(row)
		obj, err := tbl.GetObject(objKey)
		if err != nil {
			return nil, false
		}

		val, err := obj.Get(colKey)
		if err != nil {
			return nil, false
		}

		if strVal, ok := val.(string); ok {
			values = append(values, strVal)
		} else {
			return nil, false
		}
	}

	return values, true
}

// calculateChunkSize calculates optimal chunk size based on total rows
func (pe *ParallelExecutor) calculateChunkSize(totalRows uint64) uint64 {
	// Base chunk size from realm-core patterns
	baseChunkSize := uint64(pe.chunkSize)

	// Adjust based on number of workers and total rows
	optimalChunks := uint64(pe.numWorkers * 2) // 2x workers for better load balancing
	if totalRows < optimalChunks {
		return 1 // One row per chunk if very few rows
	}

	calculatedSize := totalRows / optimalChunks
	if calculatedSize < baseChunkSize {
		return calculatedSize
	}

	return baseChunkSize
}

// SetNumWorkers sets the number of worker goroutines
func (pe *ParallelExecutor) SetNumWorkers(numWorkers int) {
	if numWorkers > 0 {
		pe.numWorkers = numWorkers
	}
}

// SetChunkSize sets the chunk size for parallel processing
func (pe *ParallelExecutor) SetChunkSize(chunkSize int64) {
	if chunkSize > 0 {
		pe.chunkSize = chunkSize
	}
}