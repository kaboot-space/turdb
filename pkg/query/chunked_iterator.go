package query

import (
	"context"
	"errors"
	"sync"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/table"
)

// ChunkedIterator provides memory-efficient iteration over query results
// Based on realm-core's chunked result processing patterns
type ChunkedIterator struct {
	table         *table.Table
	node          QueryNode
	chunkSize     uint64
	currentChunk  uint64
	totalRows     uint64
	currentPos    uint64
	buffer        []keys.ObjKey
	bufferPos     int
	finished      bool
	mutex         sync.RWMutex
	parallelExec  *ParallelExecutor
	prefetchSize  int
	prefetchChan  chan []keys.ObjKey
	ctx           context.Context
	cancel        context.CancelFunc
}

// ChunkedIteratorConfig holds configuration for chunked iterator
type ChunkedIteratorConfig struct {
	ChunkSize    uint64
	PrefetchSize int
	UseParallel  bool
}

// DefaultChunkedIteratorConfig returns default configuration
func DefaultChunkedIteratorConfig() ChunkedIteratorConfig {
	return ChunkedIteratorConfig{
		ChunkSize:    1024, // Based on realm-core's optimal chunk size
		PrefetchSize: 2,    // Prefetch 2 chunks ahead
		UseParallel:  true,
	}
}

// NewChunkedIterator creates a new chunked iterator
func NewChunkedIterator(
	tbl *table.Table,
	node QueryNode,
	config ChunkedIteratorConfig,
) *ChunkedIterator {
	ctx, cancel := context.WithCancel(context.Background())
	
	iterator := &ChunkedIterator{
		table:        tbl,
		node:         node,
		chunkSize:    config.ChunkSize,
		totalRows:    tbl.GetObjectCount(),
		buffer:       make([]keys.ObjKey, 0, config.ChunkSize),
		parallelExec: NewParallelExecutor(),
		prefetchSize: config.PrefetchSize,
		prefetchChan: make(chan []keys.ObjKey, config.PrefetchSize),
		ctx:          ctx,
		cancel:       cancel,
	}

	// Start prefetching if enabled
	if config.UseParallel && config.PrefetchSize > 0 {
		go iterator.startPrefetching()
	}

	return iterator
}

// Next returns the next matching object key
func (ci *ChunkedIterator) Next() (keys.ObjKey, error) {
	ci.mutex.Lock()
	defer ci.mutex.Unlock()

	if ci.finished {
		return keys.ObjKey(0), errors.New("iterator finished")
	}

	// Check if we need to load next chunk
	if ci.bufferPos >= len(ci.buffer) {
		if err := ci.loadNextChunk(); err != nil {
			ci.finished = true
			return keys.ObjKey(0), err
		}
	}

	// Return next object from buffer
	if ci.bufferPos < len(ci.buffer) {
		objKey := ci.buffer[ci.bufferPos]
		ci.bufferPos++
		return objKey, nil
	}

	ci.finished = true
	return keys.ObjKey(0), errors.New("no more results")
}

// HasNext returns true if there are more results
func (ci *ChunkedIterator) HasNext() bool {
	ci.mutex.RLock()
	defer ci.mutex.RUnlock()

	if ci.finished {
		return false
	}

	// Check if current buffer has more items
	if ci.bufferPos < len(ci.buffer) {
		return true
	}

	// Check if there are more chunks to process
	return ci.currentPos < ci.totalRows
}

// loadNextChunk loads the next chunk of results
func (ci *ChunkedIterator) loadNextChunk() error {
	// Check if we have prefetched results
	select {
	case prefetchedChunk := <-ci.prefetchChan:
		ci.buffer = prefetchedChunk
		ci.bufferPos = 0
		return nil
	default:
		// No prefetched results, load synchronously
		return ci.loadChunkSync()
	}
}

// loadChunkSync loads a chunk synchronously
func (ci *ChunkedIterator) loadChunkSync() error {
	if ci.currentPos >= ci.totalRows {
		return errors.New("no more chunks")
	}

	chunkStart := ci.currentPos
	chunkEnd := chunkStart + ci.chunkSize
	if chunkEnd > ci.totalRows {
		chunkEnd = ci.totalRows
	}

	// Execute chunk using parallel executor
	results, err := ci.parallelExec.ExecuteParallel(
		ci.ctx,
		ci.node,
		ci.table,
		chunkStart,
		chunkEnd,
	)
	if err != nil {
		return err
	}

	ci.buffer = results
	ci.bufferPos = 0
	ci.currentPos = chunkEnd
	ci.currentChunk++

	return nil
}

// startPrefetching starts background prefetching of chunks
func (ci *ChunkedIterator) startPrefetching() {
	defer close(ci.prefetchChan)

	for {
		select {
		case <-ci.ctx.Done():
			return
		default:
			// Check if we need to prefetch more chunks
			ci.mutex.RLock()
			shouldPrefetch := ci.currentPos < ci.totalRows && !ci.finished
			currentPos := ci.currentPos
			ci.mutex.RUnlock()

			if !shouldPrefetch {
				return
			}

			// Calculate next chunk range
			chunkStart := currentPos
			chunkEnd := chunkStart + ci.chunkSize
			if chunkEnd > ci.totalRows {
				chunkEnd = ci.totalRows
			}

			// Execute chunk
			results, err := ci.parallelExec.ExecuteParallel(
				ci.ctx,
				ci.node,
				ci.table,
				chunkStart,
				chunkEnd,
			)
			if err != nil {
				return
			}

			// Try to send results to prefetch channel
			select {
			case ci.prefetchChan <- results:
				ci.mutex.Lock()
				ci.currentPos = chunkEnd
				ci.currentChunk++
				ci.mutex.Unlock()
			case <-ci.ctx.Done():
				return
			}
		}
	}
}

// Reset resets the iterator to the beginning
func (ci *ChunkedIterator) Reset() {
	ci.mutex.Lock()
	defer ci.mutex.Unlock()

	ci.currentChunk = 0
	ci.currentPos = 0
	ci.bufferPos = 0
	ci.buffer = ci.buffer[:0]
	ci.finished = false

	// Cancel and restart prefetching
	ci.cancel()
	ci.ctx, ci.cancel = context.WithCancel(context.Background())
	
	if ci.prefetchSize > 0 {
		go ci.startPrefetching()
	}
}

// Close closes the iterator and releases resources
func (ci *ChunkedIterator) Close() {
	ci.mutex.Lock()
	defer ci.mutex.Unlock()

	ci.finished = true
	ci.cancel()
}

// Count returns the total number of matching results
// This method processes all chunks to get an accurate count
func (ci *ChunkedIterator) Count() (uint64, error) {
	// Save current state
	originalPos := ci.currentPos
	originalChunk := ci.currentChunk
	originalBufferPos := ci.bufferPos
	originalBuffer := make([]keys.ObjKey, len(ci.buffer))
	copy(originalBuffer, ci.buffer)

	// Reset to beginning
	ci.Reset()

	var totalCount uint64
	for ci.HasNext() {
		_, err := ci.Next()
		if err != nil {
			break
		}
		totalCount++
	}

	// Restore original state
	ci.currentPos = originalPos
	ci.currentChunk = originalChunk
	ci.bufferPos = originalBufferPos
	ci.buffer = originalBuffer

	return totalCount, nil
}

// GetChunkStats returns statistics about chunk processing
func (ci *ChunkedIterator) GetChunkStats() ChunkStats {
	ci.mutex.RLock()
	defer ci.mutex.RUnlock()

	return ChunkStats{
		TotalChunks:    (ci.totalRows + ci.chunkSize - 1) / ci.chunkSize,
		ProcessedChunks: ci.currentChunk,
		ChunkSize:      ci.chunkSize,
		CurrentPos:     ci.currentPos,
		TotalRows:      ci.totalRows,
		BufferSize:     uint64(len(ci.buffer)),
		BufferPos:      uint64(ci.bufferPos),
	}
}

// ChunkStats holds statistics about chunk processing
type ChunkStats struct {
	TotalChunks     uint64
	ProcessedChunks uint64
	ChunkSize       uint64
	CurrentPos      uint64
	TotalRows       uint64
	BufferSize      uint64
	BufferPos       uint64
}

// ChunkedResultSet provides a high-level interface for chunked query results
type ChunkedResultSet struct {
	iterator *ChunkedIterator
	table    *table.Table
}

// NewChunkedResultSet creates a new chunked result set
func NewChunkedResultSet(
	tbl *table.Table,
	node QueryNode,
	config ChunkedIteratorConfig,
) *ChunkedResultSet {
	return &ChunkedResultSet{
		iterator: NewChunkedIterator(tbl, node, config),
		table:    tbl,
	}
}

// ForEach iterates over all results and calls the provided function
func (crs *ChunkedResultSet) ForEach(fn func(*table.Object) error) error {
	for crs.iterator.HasNext() {
		objKey, err := crs.iterator.Next()
		if err != nil {
			return err
		}

		obj, err := crs.table.GetObject(objKey)
		if err != nil {
			continue // Skip invalid objects
		}

		if err := fn(obj); err != nil {
			return err
		}
	}
	return nil
}

// Collect collects all results into a slice
func (crs *ChunkedResultSet) Collect() ([]*table.Object, error) {
	var results []*table.Object

	err := crs.ForEach(func(obj *table.Object) error {
		results = append(results, obj)
		return nil
	})

	return results, err
}

// Count returns the total number of results
func (crs *ChunkedResultSet) Count() (uint64, error) {
	return crs.iterator.Count()
}

// Close closes the result set
func (crs *ChunkedResultSet) Close() {
	crs.iterator.Close()
}