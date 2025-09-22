package query

import (
	"context"
	"sync"

	"github.com/turdb/tur/pkg/cluster"
	"github.com/turdb/tur/pkg/keys"
)

// StreamingResult represents a single result in the stream
type StreamingResult struct {
	Key   keys.ObjKey
	Index int64
	Error error
}

// ResultStream provides channel-based streaming of query results
type ResultStream struct {
	tree     *cluster.ClusterTree
	results  chan StreamingResult
	done     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	bufSize  int
	position int64
	size     int64
}

// NewResultStream creates a new streaming result set
func NewResultStream(tree *cluster.ClusterTree, bufferSize int) *ResultStream {
	if bufferSize <= 0 {
		bufferSize = 100 // Default buffer size
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	root, err := tree.GetRootCluster()
	size := int64(0)
	if err == nil && root != nil {
		size = int64(root.ObjectCount())
	}

	return &ResultStream{
		tree:    tree,
		results: make(chan StreamingResult, bufferSize),
		done:    make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
		bufSize: bufferSize,
		size:    size,
	}
}

// Start begins streaming results
func (rs *ResultStream) Start() <-chan StreamingResult {
	rs.wg.Add(1)
	go rs.streamResults()
	return rs.results
}

// streamResults streams results in a separate goroutine
func (rs *ResultStream) streamResults() {
	defer rs.wg.Done()
	defer close(rs.results)
	defer close(rs.done)

	root, err := rs.tree.GetRootCluster()
	if err != nil {
		select {
		case rs.results <- StreamingResult{Error: err}:
		case <-rs.ctx.Done():
		}
		return
	}

	if root == nil {
		return
	}

	objectCount := root.ObjectCount()
	for i := 0; i < objectCount; i++ {
		select {
		case <-rs.ctx.Done():
			return
		default:
		}

		objKey, err := root.GetObjectKey(i)
		result := StreamingResult{
			Key:   objKey,
			Index: int64(i),
			Error: err,
		}

		select {
		case rs.results <- result:
			rs.position = int64(i + 1)
		case <-rs.ctx.Done():
			return
		}
	}
}

// Stop stops the streaming
func (rs *ResultStream) Stop() {
	rs.cancel()
	rs.wg.Wait()
}

// Position returns the current streaming position
func (rs *ResultStream) Position() int64 {
	return rs.position
}

// Size returns the total number of results
func (rs *ResultStream) Size() int64 {
	return rs.size
}

// Done returns a channel that's closed when streaming is complete
func (rs *ResultStream) Done() <-chan struct{} {
	return rs.done
}

// BatchStream provides batched streaming of results
type BatchStream struct {
	tree      *cluster.ClusterTree
	batches   chan []StreamingResult
	done      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	batchSize int
	bufSize   int
}

// NewBatchStream creates a new batch streaming result set
func NewBatchStream(tree *cluster.ClusterTree, batchSize, bufferSize int) *BatchStream {
	if batchSize <= 0 {
		batchSize = 50 // Default batch size
	}
	if bufferSize <= 0 {
		bufferSize = 10 // Default buffer size for batches
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &BatchStream{
		tree:      tree,
		batches:   make(chan []StreamingResult, bufferSize),
		done:      make(chan struct{}),
		ctx:       ctx,
		cancel:    cancel,
		batchSize: batchSize,
		bufSize:   bufferSize,
	}
}

// Start begins streaming batches
func (bs *BatchStream) Start() <-chan []StreamingResult {
	bs.wg.Add(1)
	go bs.streamBatches()
	return bs.batches
}

// streamBatches streams results in batches
func (bs *BatchStream) streamBatches() {
	defer bs.wg.Done()
	defer close(bs.batches)
	defer close(bs.done)

	root, err := bs.tree.GetRootCluster()
	if err != nil {
		select {
		case bs.batches <- []StreamingResult{{Error: err}}:
		case <-bs.ctx.Done():
		}
		return
	}

	if root == nil {
		return
	}

	objectCount := root.ObjectCount()
	batch := make([]StreamingResult, 0, bs.batchSize)

	for i := 0; i < objectCount; i++ {
		select {
		case <-bs.ctx.Done():
			return
		default:
		}

		objKey, err := root.GetObjectKey(i)
		result := StreamingResult{
			Key:   objKey,
			Index: int64(i),
			Error: err,
		}

		batch = append(batch, result)

		// Send batch when it's full or we've reached the end
		if len(batch) >= bs.batchSize || i == objectCount-1 {
			select {
			case bs.batches <- batch:
				batch = make([]StreamingResult, 0, bs.batchSize)
			case <-bs.ctx.Done():
				return
			}
		}
	}
}

// Stop stops the batch streaming
func (bs *BatchStream) Stop() {
	bs.cancel()
	bs.wg.Wait()
}

// Done returns a channel that's closed when streaming is complete
func (bs *BatchStream) Done() <-chan struct{} {
	return bs.done
}

// StreamingQuery provides a high-level interface for streaming queries
type StreamingQuery struct {
	tree   *cluster.ClusterTree
	filter func(keys.ObjKey) bool
}

// NewStreamingQuery creates a new streaming query
func NewStreamingQuery(tree *cluster.ClusterTree) *StreamingQuery {
	return &StreamingQuery{
		tree: tree,
	}
}

// WithFilter adds a filter function to the streaming query
func (sq *StreamingQuery) WithFilter(filter func(keys.ObjKey) bool) *StreamingQuery {
	sq.filter = filter
	return sq
}

// Stream returns a filtered result stream
func (sq *StreamingQuery) Stream(bufferSize int) *ResultStream {
	stream := NewResultStream(sq.tree, bufferSize)
	
	if sq.filter != nil {
		// Create a filtered stream
		return sq.createFilteredStream(stream)
	}
	
	return stream
}

// createFilteredStream creates a stream with filtering applied
func (sq *StreamingQuery) createFilteredStream(originalStream *ResultStream) *ResultStream {
	ctx, cancel := context.WithCancel(context.Background())
	
	filteredStream := &ResultStream{
		tree:    sq.tree,
		results: make(chan StreamingResult, originalStream.bufSize),
		done:    make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
		bufSize: originalStream.bufSize,
	}

	filteredStream.wg.Add(1)
	go func() {
		defer filteredStream.wg.Done()
		defer close(filteredStream.results)
		defer close(filteredStream.done)

		originalResults := originalStream.Start()
		
		for result := range originalResults {
			select {
			case <-filteredStream.ctx.Done():
				originalStream.Stop()
				return
			default:
			}

			if result.Error != nil || sq.filter(result.Key) {
				select {
				case filteredStream.results <- result:
				case <-filteredStream.ctx.Done():
					originalStream.Stop()
					return
				}
			}
		}
	}()

	return filteredStream
}