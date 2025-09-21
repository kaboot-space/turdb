package cluster

import (
	"errors"
	"sync"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// IteratorState represents the state for cluster tree traversal
type IteratorState struct {
	// Core iterator state
	currentState  *State          // Current state in traversal
	position      int             // Current position in the cluster
	depth         int             // Current depth in the tree
	direction     IteratorDir     // Traversal direction
	
	// Navigation state
	path          []*State        // Path from root to current position
	pathIndices   []int           // Indices at each level of the path
	
	// Traversal control
	traversalType TraversalType   // Type of traversal
	filter        IteratorFilter  // Optional filter function
	context       *IteratorContext // Iterator context
	
	// Caching and performance
	leafCache     map[storage.Ref]*Cluster // Cached leaf nodes
	cacheSize     int                      // Current cache size
	maxCacheSize  int                      // Maximum cache size
	
	// Synchronization
	mutex         sync.RWMutex    // Iterator mutex
	isValid       bool            // Iterator validity
	isAtEnd       bool            // End of iteration flag
}

// IteratorDir represents traversal direction
type IteratorDir int

const (
	IteratorDirForward IteratorDir = iota
	IteratorDirBackward
)

// TraversalType represents different types of tree traversal
type TraversalType int

const (
	TraversalInOrder TraversalType = iota
	TraversalPreOrder
	TraversalPostOrder
	TraversalLevelOrder
	TraversalDepthFirst
	TraversalBreadthFirst
)

// IteratorFilter is a function type for filtering during traversal
type IteratorFilter func(*State, interface{}) bool

// IteratorContext provides context for iterator operations
type IteratorContext struct {
	startKey    keys.ObjKey
	endKey      keys.ObjKey
	maxDepth    int
	userData    interface{}
	allocator   *core.Allocator
}

// NewIteratorState creates a new iterator state
func NewIteratorState(initialState *State, traversalType TraversalType) *IteratorState {
	return &IteratorState{
		currentState:  initialState,
		position:      0,
		depth:         0,
		direction:     IteratorDirForward,
		path:          make([]*State, 0, 16), // Pre-allocate for typical tree depth
		pathIndices:   make([]int, 0, 16),
		traversalType: traversalType,
		leafCache:     make(map[storage.Ref]*Cluster),
		maxCacheSize:  64, // Default cache size
		isValid:       true,
		isAtEnd:       false,
	}
}

// NewIteratorStateWithContext creates a new iterator state with context
func NewIteratorStateWithContext(initialState *State, traversalType TraversalType, context *IteratorContext) *IteratorState {
	iter := NewIteratorState(initialState, traversalType)
	iter.context = context
	return iter
}

// GetCurrentState returns the current state
func (is *IteratorState) GetCurrentState() *State {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.currentState
}

// SetCurrentState sets the current state
func (is *IteratorState) SetCurrentState(state *State) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.currentState = state
	if state != nil {
		is.depth = state.GetDepth()
	}
}

// GetPosition returns the current position
func (is *IteratorState) GetPosition() int {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.position
}

// SetPosition sets the current position
func (is *IteratorState) SetPosition(pos int) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.position = pos
}

// GetDepth returns the current depth
func (is *IteratorState) GetDepth() int {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.depth
}

// GetDirection returns the traversal direction
func (is *IteratorState) GetDirection() IteratorDir {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.direction
}

// SetDirection sets the traversal direction
func (is *IteratorState) SetDirection(dir IteratorDir) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.direction = dir
}

// GetTraversalType returns the traversal type
func (is *IteratorState) GetTraversalType() TraversalType {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.traversalType
}

// SetTraversalType sets the traversal type
func (is *IteratorState) SetTraversalType(tType TraversalType) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.traversalType = tType
}

// PushState pushes a state onto the path stack
func (is *IteratorState) PushState(state *State, index int) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.path = append(is.path, state)
	is.pathIndices = append(is.pathIndices, index)
	is.depth = len(is.path)
}

// PopState pops a state from the path stack
func (is *IteratorState) PopState() (*State, int, error) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	
	if len(is.path) == 0 {
		return nil, -1, errors.New("path stack is empty")
	}
	
	// Pop the last state and index
	lastIdx := len(is.path) - 1
	state := is.path[lastIdx]
	index := is.pathIndices[lastIdx]
	
	is.path = is.path[:lastIdx]
	is.pathIndices = is.pathIndices[:lastIdx]
	is.depth = len(is.path)
	
	return state, index, nil
}

// GetPath returns a copy of the current path
func (is *IteratorState) GetPath() []*State {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	
	path := make([]*State, len(is.path))
	copy(path, is.path)
	return path
}

// GetPathIndices returns a copy of the current path indices
func (is *IteratorState) GetPathIndices() []int {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	
	indices := make([]int, len(is.pathIndices))
	copy(indices, is.pathIndices)
	return indices
}

// ClearPath clears the path stack
func (is *IteratorState) ClearPath() {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.path = is.path[:0]
	is.pathIndices = is.pathIndices[:0]
	is.depth = 0
}

// SetFilter sets the iterator filter
func (is *IteratorState) SetFilter(filter IteratorFilter) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.filter = filter
}

// GetFilter returns the iterator filter
func (is *IteratorState) GetFilter() IteratorFilter {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.filter
}

// ApplyFilter applies the filter to the current state
func (is *IteratorState) ApplyFilter(data interface{}) bool {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	
	if is.filter == nil {
		return true
	}
	
	return is.filter(is.currentState, data)
}

// GetContext returns the iterator context
func (is *IteratorState) GetContext() *IteratorContext {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.context
}

// SetContext sets the iterator context
func (is *IteratorState) SetContext(context *IteratorContext) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.context = context
}

// CacheLeaf caches a leaf cluster
func (is *IteratorState) CacheLeaf(ref storage.Ref, cluster *Cluster) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	
	// Check cache size limit
	if is.cacheSize >= is.maxCacheSize {
		is.evictOldestCacheEntry()
	}
	
	is.leafCache[ref] = cluster
	is.cacheSize++
}

// GetCachedLeaf retrieves a cached leaf cluster
func (is *IteratorState) GetCachedLeaf(ref storage.Ref) (*Cluster, bool) {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	
	cluster, exists := is.leafCache[ref]
	return cluster, exists
}

// InvalidateLeafCache invalidates the leaf cache
func (is *IteratorState) InvalidateLeafCache() {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	
	// Clear the cache
	for k := range is.leafCache {
		delete(is.leafCache, k)
	}
	is.cacheSize = 0
}

// evictOldestCacheEntry evicts the oldest cache entry (simple FIFO)
func (is *IteratorState) evictOldestCacheEntry() {
	// Simple eviction strategy - remove first entry found
	// In a production system, this would use LRU or similar
	for k := range is.leafCache {
		delete(is.leafCache, k)
		is.cacheSize--
		break
	}
}

// SetMaxCacheSize sets the maximum cache size
func (is *IteratorState) SetMaxCacheSize(size int) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.maxCacheSize = size
	
	// Evict entries if current size exceeds new limit
	for is.cacheSize > is.maxCacheSize {
		is.evictOldestCacheEntry()
	}
}

// GetCacheSize returns the current cache size
func (is *IteratorState) GetCacheSize() int {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.cacheSize
}

// IsValid returns true if the iterator is valid
func (is *IteratorState) IsValid() bool {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.isValid && is.currentState != nil && is.currentState.IsValid()
}

// Invalidate marks the iterator as invalid
func (is *IteratorState) Invalidate() {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.isValid = false
}

// IsAtEnd returns true if the iterator is at the end
func (is *IteratorState) IsAtEnd() bool {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	return is.isAtEnd
}

// SetAtEnd sets the end flag
func (is *IteratorState) SetAtEnd(atEnd bool) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	is.isAtEnd = atEnd
}

// Reset resets the iterator to initial state
func (is *IteratorState) Reset(initialState *State) {
	is.mutex.Lock()
	defer is.mutex.Unlock()
	
	is.currentState = initialState
	is.position = 0
	is.depth = 0
	is.path = is.path[:0]
	is.pathIndices = is.pathIndices[:0]
	is.isValid = true
	is.isAtEnd = false
	
	// Clear cache
	for k := range is.leafCache {
		delete(is.leafCache, k)
	}
	is.cacheSize = 0
}

// Clone creates a copy of the iterator state
func (is *IteratorState) Clone() *IteratorState {
	is.mutex.RLock()
	defer is.mutex.RUnlock()
	
	clone := &IteratorState{
		currentState:  is.currentState,
		position:      is.position,
		depth:         is.depth,
		direction:     is.direction,
		traversalType: is.traversalType,
		filter:        is.filter,
		context:       is.context,
		maxCacheSize:  is.maxCacheSize,
		isValid:       is.isValid,
		isAtEnd:       is.isAtEnd,
	}
	
	// Copy path
	clone.path = make([]*State, len(is.path))
	copy(clone.path, is.path)
	
	// Copy path indices
	clone.pathIndices = make([]int, len(is.pathIndices))
	copy(clone.pathIndices, is.pathIndices)
	
	// Copy cache (shallow copy)
	clone.leafCache = make(map[storage.Ref]*Cluster)
	for k, v := range is.leafCache {
		clone.leafCache[k] = v
	}
	clone.cacheSize = is.cacheSize
	
	return clone
}

// ClusterIterator provides high-level iteration over cluster trees
type ClusterIterator struct {
	state   *IteratorState
	cluster *Cluster
}

// NewClusterIterator creates a new cluster iterator
func NewClusterIterator(cluster *Cluster, traversalType TraversalType) *ClusterIterator {
	// Create a MemRef from the cluster's storage reference
	memRef := NewMemRef(cluster.GetRef(), 0) // Size will be determined later
	initialState := NewState(cluster, memRef, 0)
	iterState := NewIteratorState(initialState, traversalType)
	
	return &ClusterIterator{
		state:   iterState,
		cluster: cluster,
	}
}

// Next advances the iterator to the next position
func (ci *ClusterIterator) Next() bool {
	if !ci.state.IsValid() || ci.state.IsAtEnd() {
		return false
	}
	
	// Implementation depends on traversal type
	switch ci.state.GetTraversalType() {
	case TraversalInOrder:
		return ci.nextInOrder()
	case TraversalPreOrder:
		return ci.nextPreOrder()
	case TraversalPostOrder:
		return ci.nextPostOrder()
	case TraversalLevelOrder:
		return ci.nextLevelOrder()
	default:
		return ci.nextInOrder() // Default to in-order
	}
}

// Previous moves the iterator to the previous position
func (ci *ClusterIterator) Previous() bool {
	if !ci.state.IsValid() {
		return false
	}
	
	// Set direction to backward and use appropriate traversal
	ci.state.SetDirection(IteratorDirBackward)
	return ci.Next()
}

// GetCurrent returns the current state
func (ci *ClusterIterator) GetCurrent() *State {
	return ci.state.GetCurrentState()
}

// GetValue returns the current value
func (ci *ClusterIterator) GetValue() (interface{}, error) {
	currentState := ci.state.GetCurrentState()
	if currentState == nil || !currentState.IsValid() {
		return nil, errors.New("invalid iterator state")
	}
	
	cluster := currentState.GetCluster()
	if cluster == nil {
		return nil, errors.New("no cluster associated with current state")
	}
	
	position := ci.state.GetPosition()
	// Note: This is a simplified version - in practice, you'd need a specific column key
	// For now, we'll return the object key at this position
	if position >= cluster.ObjectCount() {
		return nil, errors.New("position out of bounds")
	}
	
	return cluster.GetObjectKey(position)
}

// IsValid returns true if the iterator is valid
func (ci *ClusterIterator) IsValid() bool {
	return ci.state.IsValid()
}

// Reset resets the iterator to the beginning
func (ci *ClusterIterator) Reset() {
	memRef := NewMemRef(ci.cluster.GetRef(), 0)
	initialState := NewState(ci.cluster, memRef, 0)
	ci.state.Reset(initialState)
}

// Implementation of specific traversal methods
func (ci *ClusterIterator) nextInOrder() bool {
	// In-order traversal implementation
	// This is a simplified version - full implementation would handle tree structure
	currentState := ci.state.GetCurrentState()
	if currentState == nil {
		ci.state.SetAtEnd(true)
		return false
	}
	
	position := ci.state.GetPosition()
	cluster := currentState.GetCluster()
	
	if cluster == nil {
		ci.state.SetAtEnd(true)
		return false
	}
	
	// Move to next position
	if position+1 < cluster.ObjectCount() {
		ci.state.SetPosition(position + 1)
		return true
	}
	
	// End of current cluster
	ci.state.SetAtEnd(true)
	return false
}

func (ci *ClusterIterator) nextPreOrder() bool {
	// Pre-order traversal implementation
	return ci.nextInOrder() // Simplified
}

func (ci *ClusterIterator) nextPostOrder() bool {
	// Post-order traversal implementation
	return ci.nextInOrder() // Simplified
}

func (ci *ClusterIterator) nextLevelOrder() bool {
	// Level-order (breadth-first) traversal implementation
	return ci.nextInOrder() // Simplified
}