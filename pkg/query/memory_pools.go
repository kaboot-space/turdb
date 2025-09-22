package query

import (
	"sync"
	"unsafe"
)

// NodePool manages reusable query node objects to reduce allocations
type NodePool struct {
	comparisonNodes sync.Pool
	logicalNodes    sync.Pool
	propertyNodes   sync.Pool
	literalNodes    sync.Pool
	resultSets      sync.Pool
	mu              sync.RWMutex
	stats           PoolStats
}

// PoolStats tracks memory pool usage statistics
type PoolStats struct {
	ComparisonHits   uint64
	ComparisonMisses uint64
	LogicalHits      uint64
	LogicalMisses    uint64
	PropertyHits     uint64
	PropertyMisses   uint64
	LiteralHits      uint64
	LiteralMisses    uint64
	ResultSetHits    uint64
	ResultSetMisses  uint64
	TotalAllocated   uint64
	TotalReused      uint64
}

// ResultSetPool manages reusable result set objects
type ResultSetPool struct {
	pool  sync.Pool
	stats PoolStats
}

// PooledResultSet represents a reusable result set with capacity management
type PooledResultSet struct {
	Keys     []uint64
	Values   []interface{}
	Capacity int
	pool     *ResultSetPool
}

// NewNodePool creates a new node pool with optimized allocation patterns
func NewNodePool() *NodePool {
	np := &NodePool{}
	
	np.comparisonNodes.New = func() interface{} {
		np.stats.ComparisonMisses++
		return &ComparisonExpression{}
	}
	
	np.logicalNodes.New = func() interface{} {
		np.stats.LogicalMisses++
		return &LogicalExpression{}
	}
	
	np.propertyNodes.New = func() interface{} {
		np.stats.PropertyMisses++
		return &PropertyExpression{}
	}
	
	np.literalNodes.New = func() interface{} {
		np.stats.LiteralMisses++
		return &LiteralExpression{}
	}
	
	np.resultSets.New = func() interface{} {
		np.stats.ResultSetMisses++
		return &PooledResultSet{
			Keys:     make([]uint64, 0, 64),
			Values:   make([]interface{}, 0, 64),
			Capacity: 64,
		}
	}
	
	return np
}

// GetComparisonNode retrieves a reusable comparison node
func (np *NodePool) GetComparisonNode() *ComparisonExpression {
	np.mu.Lock()
	np.stats.ComparisonHits++
	np.stats.TotalReused++
	np.mu.Unlock()
	
	node := np.comparisonNodes.Get().(*ComparisonExpression)
	node.reset()
	return node
}

// PutComparisonNode returns a comparison node to the pool
func (np *NodePool) PutComparisonNode(node *ComparisonExpression) {
	if node != nil {
		node.reset()
		np.comparisonNodes.Put(node)
	}
}

// GetLogicalNode retrieves a reusable logical node
func (np *NodePool) GetLogicalNode() *LogicalExpression {
	np.mu.Lock()
	np.stats.LogicalHits++
	np.stats.TotalReused++
	np.mu.Unlock()
	
	node := np.logicalNodes.Get().(*LogicalExpression)
	node.reset()
	return node
}

// PutLogicalNode returns a logical node to the pool
func (np *NodePool) PutLogicalNode(node *LogicalExpression) {
	if node != nil {
		node.reset()
		np.logicalNodes.Put(node)
	}
}

// GetPropertyNode retrieves a reusable property node
func (np *NodePool) GetPropertyNode() *PropertyExpression {
	np.mu.Lock()
	np.stats.PropertyHits++
	np.stats.TotalReused++
	np.mu.Unlock()
	
	node := np.propertyNodes.Get().(*PropertyExpression)
	node.reset()
	return node
}

// PutPropertyNode returns a property node to the pool
func (np *NodePool) PutPropertyNode(node *PropertyExpression) {
	if node != nil {
		node.reset()
		np.propertyNodes.Put(node)
	}
}

// GetLiteralNode retrieves a reusable literal node
func (np *NodePool) GetLiteralNode() *LiteralExpression {
	np.mu.Lock()
	np.stats.LiteralHits++
	np.stats.TotalReused++
	np.mu.Unlock()
	
	node := np.literalNodes.Get().(*LiteralExpression)
	node.reset()
	return node
}

// PutLiteralNode returns a literal node to the pool
func (np *NodePool) PutLiteralNode(node *LiteralExpression) {
	if node != nil {
		node.reset()
		np.literalNodes.Put(node)
	}
}

// GetResultSet retrieves a reusable result set
func (np *NodePool) GetResultSet() *PooledResultSet {
	np.mu.Lock()
	np.stats.ResultSetHits++
	np.stats.TotalReused++
	np.mu.Unlock()
	
	rs := np.resultSets.Get().(*PooledResultSet)
	rs.reset()
	rs.pool = &ResultSetPool{pool: np.resultSets}
	return rs
}

// PutResultSet returns a result set to the pool
func (np *NodePool) PutResultSet(rs *PooledResultSet) {
	if rs != nil {
		rs.reset()
		np.resultSets.Put(rs)
	}
}

// GetStats returns current pool statistics
func (np *NodePool) GetStats() PoolStats {
	np.mu.RLock()
	defer np.mu.RUnlock()
	return np.stats
}

// Reset clears all pool statistics
func (np *NodePool) Reset() {
	np.mu.Lock()
	defer np.mu.Unlock()
	np.stats = PoolStats{}
}

// reset methods for pooled objects
func (ce *ComparisonExpression) reset() {
	ce.Left = nil
	ce.Right = nil
	ce.Operator = 0
}

func (le *LogicalExpression) reset() {
	le.Left = nil
	le.Right = nil
	le.Operator = 0
}

func (pe *PropertyExpression) reset() {
	pe.Name = ""
	pe.ColKey = 0
}

func (le *LiteralExpression) reset() {
	le.Value = nil
}

// reset clears the result set for reuse
func (rs *PooledResultSet) reset() {
	rs.Keys = rs.Keys[:0]
	rs.Values = rs.Values[:0]
}

// Grow expands the result set capacity if needed
func (rs *PooledResultSet) Grow(size int) {
	if size > rs.Capacity {
		newCap := rs.Capacity * 2
		if newCap < size {
			newCap = size
		}
		rs.Keys = make([]uint64, 0, newCap)
		rs.Values = make([]interface{}, 0, newCap)
		rs.Capacity = newCap
	}
}

// Add appends a key-value pair to the result set
func (rs *PooledResultSet) Add(key uint64, value interface{}) {
	rs.Keys = append(rs.Keys, key)
	rs.Values = append(rs.Values, value)
}

// Size returns the current number of items in the result set
func (rs *PooledResultSet) Size() int {
	return len(rs.Keys)
}

// Release returns the result set to its pool
func (rs *PooledResultSet) Release() {
	if rs.pool != nil {
		rs.pool.pool.Put(rs)
	}
}

// MemoryUsage returns approximate memory usage in bytes
func (np *NodePool) MemoryUsage() uintptr {
	np.mu.RLock()
	defer np.mu.RUnlock()
	
	var total uintptr
	total += uintptr(np.stats.ComparisonHits+np.stats.ComparisonMisses) * unsafe.Sizeof(ComparisonExpression{})
	total += uintptr(np.stats.LogicalHits+np.stats.LogicalMisses) * unsafe.Sizeof(LogicalExpression{})
	total += uintptr(np.stats.PropertyHits+np.stats.PropertyMisses) * unsafe.Sizeof(PropertyExpression{})
	total += uintptr(np.stats.LiteralHits+np.stats.LiteralMisses) * unsafe.Sizeof(LiteralExpression{})
	total += uintptr(np.stats.ResultSetHits+np.stats.ResultSetMisses) * unsafe.Sizeof(PooledResultSet{})
	
	return total
}

// Global node pool instance
var GlobalNodePool = NewNodePool()