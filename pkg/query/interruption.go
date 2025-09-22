package query

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/turdb/tur/pkg/cluster"
	"github.com/turdb/tur/pkg/keys"
)

// InterruptibleQuery provides query execution with cancellation support
type InterruptibleQuery struct {
	tree      *cluster.ClusterTree
	ctx       context.Context
	cancel    context.CancelFunc
	cancelled int32
	mu        sync.RWMutex
	timeout   time.Duration
}

// NewInterruptibleQuery creates a new interruptible query
func NewInterruptibleQuery(tree *cluster.ClusterTree) *InterruptibleQuery {
	ctx, cancel := context.WithCancel(context.Background())
	return &InterruptibleQuery{
		tree:   tree,
		ctx:    ctx,
		cancel: cancel,
	}
}

// NewInterruptibleQueryWithTimeout creates a new interruptible query with timeout
func NewInterruptibleQueryWithTimeout(tree *cluster.ClusterTree, timeout time.Duration) *InterruptibleQuery {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return &InterruptibleQuery{
		tree:    tree,
		ctx:     ctx,
		cancel:  cancel,
		timeout: timeout,
	}
}

// NewInterruptibleQueryWithContext creates a new interruptible query with custom context
func NewInterruptibleQueryWithContext(tree *cluster.ClusterTree, ctx context.Context) *InterruptibleQuery {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	return &InterruptibleQuery{
		tree:   tree,
		ctx:    ctxWithCancel,
		cancel: cancel,
	}
}

// Cancel cancels the query execution
func (iq *InterruptibleQuery) Cancel() {
	atomic.StoreInt32(&iq.cancelled, 1)
	iq.cancel()
}

// IsCancelled returns true if the query has been cancelled
func (iq *InterruptibleQuery) IsCancelled() bool {
	return atomic.LoadInt32(&iq.cancelled) == 1
}

// Context returns the query context
func (iq *InterruptibleQuery) Context() context.Context {
	return iq.ctx
}

// ExecuteWithInterruption executes a query with interruption support
func (iq *InterruptibleQuery) ExecuteWithInterruption(processor func(keys.ObjKey, int) error) error {
	if iq.IsCancelled() {
		return context.Canceled
	}

	root, err := iq.tree.GetRootCluster()
	if err != nil {
		return err
	}

	if root == nil {
		return nil
	}

	objectCount := root.ObjectCount()
	
	for i := 0; i < objectCount; i++ {
		select {
		case <-iq.ctx.Done():
			return iq.ctx.Err()
		default:
		}

		if iq.IsCancelled() {
			return context.Canceled
		}

		objKey, err := root.GetObjectKey(i)
		if err != nil {
			return err
		}

		if err := processor(objKey, i); err != nil {
			return err
		}
	}

	return nil
}

// InterruptibleIterator provides an iterator with cancellation support
type InterruptibleIterator struct {
	*Iterator
	query *InterruptibleQuery
}

// NewInterruptibleIterator creates a new interruptible iterator
func NewInterruptibleIterator(tree *cluster.ClusterTree) *InterruptibleIterator {
	query := NewInterruptibleQuery(tree)
	iterator := NewIterator(tree, 0)
	
	return &InterruptibleIterator{
		Iterator: iterator,
		query:    query,
	}
}

// NewInterruptibleIteratorWithContext creates a new interruptible iterator with context
func NewInterruptibleIteratorWithContext(tree *cluster.ClusterTree, ctx context.Context) *InterruptibleIterator {
	query := NewInterruptibleQueryWithContext(tree, ctx)
	iterator := NewIterator(tree, 0)
	
	return &InterruptibleIterator{
		Iterator: iterator,
		query:    query,
	}
}

// HasNext returns true if there are more elements and the query is not cancelled
func (ii *InterruptibleIterator) HasNext() bool {
	if ii.query.IsCancelled() {
		return false
	}

	select {
	case <-ii.query.ctx.Done():
		return false
	default:
		return ii.Iterator.HasNext()
	}
}

// Next returns the next element if available and not cancelled
func (ii *InterruptibleIterator) Next() (keys.ObjKey, error) {
	if ii.query.IsCancelled() {
		return keys.ObjKey(0), context.Canceled
	}

	select {
	case <-ii.query.ctx.Done():
		return keys.ObjKey(0), ii.query.ctx.Err()
	default:
		key, hasNext := ii.Iterator.Next()
		if !hasNext {
			return keys.ObjKey(0), nil
		}
		return key, nil
	}
}

// Cancel cancels the iterator
func (ii *InterruptibleIterator) Cancel() {
	ii.query.Cancel()
}

// IsCancelled returns true if the iterator has been cancelled
func (ii *InterruptibleIterator) IsCancelled() bool {
	return ii.query.IsCancelled()
}

// Context returns the iterator context
func (ii *InterruptibleIterator) Context() context.Context {
	return ii.query.Context()
}

// InterruptibleResultStream provides streaming with cancellation support
type InterruptibleResultStream struct {
	*ResultStream
	query *InterruptibleQuery
}

// NewInterruptibleResultStream creates a new interruptible result stream
func NewInterruptibleResultStream(tree *cluster.ClusterTree, bufferSize int) *InterruptibleResultStream {
	query := NewInterruptibleQuery(tree)
	stream := NewResultStream(tree, bufferSize)
	
	return &InterruptibleResultStream{
		ResultStream: stream,
		query:        query,
	}
}

// NewInterruptibleResultStreamWithContext creates a new interruptible result stream with context
func NewInterruptibleResultStreamWithContext(tree *cluster.ClusterTree, bufferSize int, ctx context.Context) *InterruptibleResultStream {
	query := NewInterruptibleQueryWithContext(tree, ctx)
	stream := NewResultStream(tree, bufferSize)
	
	return &InterruptibleResultStream{
		ResultStream: stream,
		query:        query,
	}
}

// Start begins streaming with interruption support
func (irs *InterruptibleResultStream) Start() <-chan StreamingResult {
	// Override the context in the result stream
	irs.ResultStream.ctx = irs.query.ctx
	irs.ResultStream.cancel = irs.query.cancel
	
	return irs.ResultStream.Start()
}

// Cancel cancels the streaming
func (irs *InterruptibleResultStream) Cancel() {
	irs.query.Cancel()
	irs.ResultStream.Stop()
}

// IsCancelled returns true if the stream has been cancelled
func (irs *InterruptibleResultStream) IsCancelled() bool {
	return irs.query.IsCancelled()
}

// Context returns the stream context
func (irs *InterruptibleResultStream) Context() context.Context {
	return irs.query.Context()
}

// QueryManager manages multiple interruptible queries
type QueryManager struct {
	queries map[string]*InterruptibleQuery
	mu      sync.RWMutex
}

// NewQueryManager creates a new query manager
func NewQueryManager() *QueryManager {
	return &QueryManager{
		queries: make(map[string]*InterruptibleQuery),
	}
}

// RegisterQuery registers a query with the manager
func (qm *QueryManager) RegisterQuery(id string, query *InterruptibleQuery) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.queries[id] = query
}

// CancelQuery cancels a specific query by ID
func (qm *QueryManager) CancelQuery(id string) bool {
	qm.mu.RLock()
	query, exists := qm.queries[id]
	qm.mu.RUnlock()
	
	if exists {
		query.Cancel()
		return true
	}
	return false
}

// CancelAllQueries cancels all registered queries
func (qm *QueryManager) CancelAllQueries() {
	qm.mu.RLock()
	queries := make([]*InterruptibleQuery, 0, len(qm.queries))
	for _, query := range qm.queries {
		queries = append(queries, query)
	}
	qm.mu.RUnlock()
	
	for _, query := range queries {
		query.Cancel()
	}
}

// RemoveQuery removes a query from the manager
func (qm *QueryManager) RemoveQuery(id string) {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	delete(qm.queries, id)
}

// GetQuery returns a query by ID
func (qm *QueryManager) GetQuery(id string) (*InterruptibleQuery, bool) {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	query, exists := qm.queries[id]
	return query, exists
}

// ActiveQueries returns the number of active queries
func (qm *QueryManager) ActiveQueries() int {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	return len(qm.queries)
}

// CleanupCancelledQueries removes cancelled queries from the manager
func (qm *QueryManager) CleanupCancelledQueries() {
	qm.mu.Lock()
	defer qm.mu.Unlock()
	
	for id, query := range qm.queries {
		if query.IsCancelled() {
			delete(qm.queries, id)
		}
	}
}