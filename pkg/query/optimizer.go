package query

import (
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"time"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/table"
)

// Constants from realm-core query optimization
const (
	// Bitwidth time unit for cost normalization (realm-core value: 64)
	BitwidthTimeUnit = 64.0

	// Statistical probing constants from realm-core
	Findlocals   = 64  // Maximum matches to find before probing other conditions
	ProbeMatches = 4   // Minimum matches required for statistical calculations
	Bestdist     = 512 // Optimal average match distance threshold

	// Node type dT values from realm-core
	DT_IndexedAccess = 0.0   // IndexEvaluator - instant lookup
	DT_IntegerNode   = 0.25  // Fast leaf access for integers
	DT_StringNode    = 10.0  // String comparison operations
	DT_BinaryNode    = 50.0  // Binary data operations
	DT_MixedNode     = 75.0  // Mixed type operations
	DT_ComplexNode   = 100.0 // Most expensive operations
)

// QueryNode interface defines the contract for query execution nodes
// Based on realm-core's query node architecture
type QueryNode interface {
	Cost() float64
	HasSearchIndex() bool
	AggregateLocal(start, end, localMatches uint64) uint64
	FindFirst(start, end uint64) uint64
	Match(obj *table.Object) bool
	Clone() QueryNode
	GetChildren() []QueryNode
	SetChildren(children []QueryNode)
	GetStatistics() *NodeStatistics
	SetTable(tbl *table.Table)     // Add SetTable to interface
	GetTable() *table.Table        // Add GetTable to interface
	IndexBasedKeys() []keys.ObjKey // Add IndexBasedKeys to interface - realm-core's index_based_keys()
}

// QueryOptimizer handles cost-based query optimization
// Based on realm-core Query::find_best_node and cost calculation
type QueryOptimizer struct {
	table       *table.Table
	planCache   *PlanCache
	indexOpt    *IndexOptimizer
	cacheMutex  sync.RWMutex
	cacheSize   int
	maxCacheAge time.Duration
}

// QueryPlan represents an optimized query execution plan
type QueryPlan struct {
	rootNode   QueryNode
	totalCost  float64
	createdAt  time.Time
	hitCount   uint64
	indexUsage map[keys.ColKey]bool
}

// GetTotalCost returns the total cost of the query plan
func (qp *QueryPlan) GetTotalCost() float64 {
	return qp.totalCost
}

// GetRootNode returns the root node of the query plan
func (qp *QueryPlan) GetRootNode() QueryNode {
	return qp.rootNode
}

// NodeStatistics tracks performance metrics for query nodes
// Based on realm-core's statistical probing (m_dD, m_dT)
type NodeStatistics struct {
	matchDistance float64 // m_dD - average distance between matches
	timePerMatch  float64 // m_dT - time per match evaluation
	probeCount    uint64  // number of probes performed
	matchCount    uint64  // number of matches found
}

// BaseQueryNode provides common functionality for all query nodes
type BaseQueryNode struct {
	stats    NodeStatistics
	children []QueryNode
	hasIndex bool
	table    *table.Table // m_table equivalent from realm-core ParentNode
}

// Cost returns the estimated cost of executing this node
// Based on realm-core formula: cost = 8 * bitwidth_time_unit / m_dD + m_dT
func (bn *BaseQueryNode) Cost() float64 {
	// Use realm-core cost calculation formula
	// cost = 8 * bitwidth_time_unit / m_dD + m_dT
	if bn.stats.matchDistance <= 0 {
		// Avoid division by zero - use high cost for unknown performance
		return BitwidthTimeUnit*8 + bn.stats.timePerMatch
	}

	cost := (8.0 * BitwidthTimeUnit / bn.stats.matchDistance) + bn.stats.timePerMatch
	return cost
}

// GetStatistics returns the node's performance statistics
func (bn *BaseQueryNode) GetStatistics() *NodeStatistics {
	return &bn.stats
}

// HasSearchIndex returns true if this node can use a search index
func (bn *BaseQueryNode) HasSearchIndex() bool {
	return bn.hasIndex
}

// GetChildren returns the child nodes
func (bn *BaseQueryNode) GetChildren() []QueryNode {
	return bn.children
}

// SetChildren sets the child nodes
func (bn *BaseQueryNode) SetChildren(children []QueryNode) {
	bn.children = children
}

// SetTable sets the table reference for this node (equivalent to realm-core set_table)
func (bn *BaseQueryNode) SetTable(tbl *table.Table) {
	if tbl == bn.table {
		return
	}

	bn.table = tbl
	// Propagate table to children (following realm-core pattern)
	for _, child := range bn.children {
		child.SetTable(tbl)
	}
}

// GetTable returns the table reference for this node
func (bn *BaseQueryNode) GetTable() *table.Table {
	return bn.table
}

// AggregateLocal must be implemented by concrete node types
func (bn *BaseQueryNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	// Default implementation - should be overridden by concrete types
	return 0
}

// FindFirst must be implemented by concrete node types
func (bn *BaseQueryNode) FindFirst(start, end uint64) uint64 {
	// Default implementation - should be overridden by concrete types
	return end
}

// Match must be implemented by concrete node types
func (bn *BaseQueryNode) Match(obj *table.Object) bool {
	// Default implementation - should be overridden by concrete types
	return false
}

// Clone must be implemented by concrete node types
func (bn *BaseQueryNode) Clone() QueryNode {
	// Default implementation - should be overridden by concrete types
	return &BaseQueryNode{
		stats:    bn.stats,
		children: bn.children,
		hasIndex: bn.hasIndex,
	}
}

// IndexBasedKeys returns object keys from index, nil if no index available
// Based on realm-core's index_based_keys() method
func (bn *BaseQueryNode) IndexBasedKeys() []keys.ObjKey {
	// Default implementation - should be overridden by nodes with indexes
	return nil
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer(tbl *table.Table) *QueryOptimizer {
	return &QueryOptimizer{
		table:       tbl,
		planCache:   NewPlanCache(1000, 30*time.Minute),
		indexOpt:    NewIndexOptimizer(tbl),
		cacheSize:   1000,
		maxCacheAge: 30 * time.Minute,
	}
}

// OptimizeQuery optimizes a query expression and returns the best execution plan
func (qo *QueryOptimizer) OptimizeQuery(expr QueryExpression) (*QueryPlan, error) {
	// Generate cache key
	tableHash := qo.getTableSchemaHash()
	cacheKey := qo.planCache.GenerateKey(expr, tableHash)

	// Check cache first
	if cachedPlan, found := qo.planCache.Get(cacheKey, tableHash); found {
		return cachedPlan, nil
	}

	// Create root node from expression
	rootNode, err := qo.createQueryNode(expr)
	if err != nil {
		return nil, fmt.Errorf("failed to create query node: %w", err)
	}

	// Find the best execution plan
	bestNode := qo.findBestNode(rootNode)

	// Create execution plan
	plan := &QueryPlan{
		rootNode:   bestNode,
		totalCost:  bestNode.Cost(),
		indexUsage: make(map[keys.ColKey]bool),
	}

	// Collect index usage information
	qo.collectIndexUsage(bestNode, plan.indexUsage)

	// Cache the plan
	qo.planCache.Put(cacheKey, plan, tableHash)

	return plan, nil
}

// getTableSchemaHash generates a hash of the table schema for cache invalidation
func (qo *QueryOptimizer) getTableSchemaHash() uint64 {
	h := fnv.New64a()
	// Use table name and column count as a simple schema hash
	h.Write([]byte(qo.table.GetName()))
	return h.Sum64()
}

// createQueryNode creates a QueryNode from a QueryExpression
func (qo *QueryOptimizer) createQueryNode(expr QueryExpression) (QueryNode, error) {
	switch e := expr.(type) {
	case *ComparisonExpression:
		return qo.buildComparisonNode(e)
	case *LogicalExpression:
		leftNode, err := qo.createQueryNode(e.Left)
		if err != nil {
			return nil, err
		}
		rightNode, err := qo.createQueryNode(e.Right)
		if err != nil {
			return nil, err
		}
		return qo.buildLogicalNode(e, leftNode, rightNode), nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// collectIndexUsage collects index usage information from query nodes
func (qo *QueryOptimizer) collectIndexUsage(node QueryNode, usage map[keys.ColKey]bool) {
	if node.HasSearchIndex() {
		// Mark that this node uses an index (simplified implementation)
		usage[keys.ColKey(0)] = true
	}

	for _, child := range node.GetChildren() {
		qo.collectIndexUsage(child, usage)
	}
}

// findBestNode finds the child node with the lowest cost
// Based on realm-core's Query::find_best_node implementation (query.cpp:1166-1174)
func (qo *QueryOptimizer) findBestNode(node QueryNode) QueryNode {
	children := node.GetChildren()
	if len(children) == 0 {
		return node
	}

	// Exact implementation of realm-core's find_best_node:
	// auto score_compare = [](const ParentNode* a, const ParentNode* b) {
	//     return a->cost() < b->cost();
	// };
	// size_t best = std::distance(pn->m_children.begin(),
	//                             std::min_element(pn->m_children.begin(), pn->m_children.end(), score_compare));

	bestIndex := 0
	bestCost := children[0].Cost()

	// Apply exact score_compare logic from realm-core
	for i := 1; i < len(children); i++ {
		if children[i].Cost() < bestCost {
			bestCost = children[i].Cost()
			bestIndex = i
		}
	}

	return children[bestIndex]
}

func (qo *QueryOptimizer) calculateTotalCost(nodes []QueryNode) float64 {
	totalCost := 0.0
	for _, node := range nodes {
		totalCost += node.Cost()
	}
	return totalCost
}

func (qo *QueryOptimizer) analyzeIndexUsage(nodes []QueryNode) map[keys.ColKey]bool {
	indexUsage := make(map[keys.ColKey]bool)
	for _, node := range nodes {
		if node.HasSearchIndex() {
			// Simplified: mark that an index is used
			indexUsage[keys.ColKey(0)] = true
		}
	}
	return indexUsage
}

func (qo *QueryOptimizer) buildQueryNodes(expr QueryExpression) ([]QueryNode, error) {
	node, err := qo.createQueryNode(expr)
	if err != nil {
		return nil, err
	}
	return []QueryNode{node}, nil
}

func (qo *QueryOptimizer) buildComparisonNode(expr *ComparisonExpression) (QueryNode, error) {
	// Check if we can use an index for this comparison
	if propExpr, ok := expr.Left.(*PropertyExpression); ok {
		colKey := propExpr.ColKey

		// Check if column has index - prefer IndexEvaluatorNode for indexed access
		if qo.HasIndex(propExpr.Name) {
			return NewIndexEvaluatorNode(expr, colKey, qo.table), nil
		}

		// Determine column type and create appropriate specialized node
		colType, err := qo.table.GetColumnType(colKey)
		if err == nil {
			switch colType {
			case keys.TypeString:
				return NewStringQueryNode(expr, colKey, qo.table), nil
			case keys.TypeInt, keys.TypeFloat, keys.TypeDouble:
				return NewIntegerQueryNode(expr, colKey, qo.table), nil
			case keys.TypeBinary:
				return NewBinaryQueryNode(expr, colKey, qo.table), nil
			case keys.TypeTimestamp:
				return NewTimestampQueryNode(expr, colKey, qo.table), nil
			}
		}
	}

	// Fallback to generic comparison node for unknown types
	return &ComparisonQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats: NodeStatistics{
				timePerMatch: DT_ComplexNode, // Higher cost for unknown types
			},
			table: qo.table,
		},
		expression: expr,
	}, nil
}

func (qo *QueryOptimizer) buildLogicalNode(expr *LogicalExpression, leftNode, rightNode QueryNode) QueryNode {
	return &LogicalQueryNode{
		BaseQueryNode: BaseQueryNode{
			children: []QueryNode{leftNode, rightNode},
			hasIndex: leftNode.HasSearchIndex() || rightNode.HasSearchIndex(),
			table:    qo.table, // Set table reference
		},
		operator: expr.Operator,
	}
}

// ClearCache clears the query plan cache
func (qo *QueryOptimizer) ClearCache() {
	qo.planCache.Clear()
}

// GetCacheStats returns cache statistics
func (qo *QueryOptimizer) GetCacheStats() *CacheStats {
	return qo.planCache.GetStats()
}

// OptimizeWithCostBasedSelection optimizes node selection based on cost analysis
// Based on realm-core Query::find_best_node implementation
func (qo *QueryOptimizer) OptimizeWithCostBasedSelection(node QueryNode, start, end uint64) QueryNode {
	if node == nil {
		return nil
	}

	// Perform statistical probing similar to realm-core
	qo.performStatisticalProbing(node, start, end)

	// Get current statistics

	// Calculate cost based on realm-core formula: cost = 8 * bitwidth_time_unit / dD + dT
	cost := node.Cost() // Use the new Cost() method with proper formula

	// If cost is too high, try to find a better alternative
	if cost > 1000.0 { // Threshold similar to realm-core
		// Try to use index if available
		if node.HasSearchIndex() {
			return node // Keep indexed node
		}

		// For non-indexed nodes, try to optimize children
		children := node.GetChildren()
		if len(children) > 0 {
			optimizedChildren := make([]QueryNode, len(children))
			for i, child := range children {
				optimizedChildren[i] = qo.OptimizeWithCostBasedSelection(child, start, end)
			}
			node.SetChildren(optimizedChildren)
		}
	}

	return node
}

// getNodeTimePerMatch calculates time per match for a node
func (qo *QueryOptimizer) getNodeTimePerMatch(node QueryNode) float64 {
	// If node has search index, use indexed access dT
	if node.HasSearchIndex() {
		return DT_IndexedAccess
	}

	// For now, determine node type based on structure
	// This will be improved when we add specialized node types
	switch node.(type) {
	case *ComparisonQueryNode:
		// Assume integer operations for comparison nodes (can be refined)
		return DT_IntegerNode
	case *LogicalQueryNode:
		// Logical operations are typically more complex
		return DT_StringNode
	case *CombinedConditionNode:
		// Combined conditions are optimized for efficiency
		return DT_IntegerNode
	default:
		// Unknown node types get complex operation cost
		return DT_ComplexNode
	}
}

// AggregateInternal performs internal aggregation similar to realm-core
func (qo *QueryOptimizer) AggregateInternal(node QueryNode, start, end uint64, limit uint64) uint64 {
	if node == nil {
		return 0
	}

	// Use the node's AggregateLocal method
	matches := node.AggregateLocal(start, end, limit)

	// Update statistics based on the aggregation results
	stats := node.GetStatistics()
	stats.matchCount += matches
	stats.probeCount += (end - start)

	// Calculate match distance (average distance between matches)
	if matches > 0 {
		stats.matchDistance = float64(end-start) / float64(matches)
	}

	return matches
}

// verifyAllConditions verifies all conditions in a query node
func (qo *QueryOptimizer) verifyAllConditions(node QueryNode, position uint64) bool {
	if node == nil {
		return true
	}

	// This would verify that all conditions are met at the given position
	// For now, we'll use a simplified implementation
	return true
}

// OptimizeQueryPlan optimizes an entire query plan
func (qo *QueryOptimizer) OptimizeQueryPlan(plan *QueryPlan, tableSize uint64) *QueryPlan {
	if plan == nil || plan.rootNode == nil {
		return plan
	}

	// Optimize the root node
	optimizedRoot := qo.OptimizeWithCostBasedSelection(plan.rootNode, 0, tableSize)

	// Create new optimized plan
	optimizedPlan := &QueryPlan{
		rootNode:   optimizedRoot,
		totalCost:  optimizedRoot.Cost(),
		createdAt:  time.Now(),
		hitCount:   0,
		indexUsage: make(map[keys.ColKey]bool),
	}

	// Collect index usage from optimized plan
	qo.collectIndexUsage(optimizedRoot, optimizedPlan.indexUsage)

	return optimizedPlan
}

// performStatisticalProbing performs statistical probing on a node
func (qo *QueryOptimizer) performStatisticalProbing(node QueryNode, start, end uint64) {
	if node == nil {
		return
	}

	// Use realm-core Findlocals constant for sample size
	sampleSize := uint64(Findlocals)
	if end-start < sampleSize {
		sampleSize = end - start
	}

	if sampleSize > 0 {
		// Perform aggregation to get matches in sample
		matches := node.AggregateLocal(start, start+sampleSize, sampleSize)

		// Only update statistics if we have minimum required matches (ProbeMatches)
		if matches >= ProbeMatches {
			stats := node.GetStatistics()

			// Calculate m_dD using realm-core formula: double(pos - start) / (matches + 1.1)
			stats.matchDistance = float64(sampleSize) / (float64(matches) + 1.1)

			// Set appropriate dT based on node type and index availability
			if node.HasSearchIndex() {
				stats.timePerMatch = DT_IndexedAccess
			} else {
				// Use node-specific dT value (will be overridden by specialized nodes)
				stats.timePerMatch = qo.getNodeTimePerMatch(node)
			}

			stats.probeCount += sampleSize
			stats.matchCount += matches
		}
	}
}

// CreateIndexedQueryNode creates a query node that uses an index
func (qo *QueryOptimizer) CreateIndexedQueryNode(expr *ComparisonExpression, colKey keys.ColKey) QueryNode {
	// Check if we have an index for this column
	if !qo.HasIndex("") { // We'll need to pass the column name
		return nil
	}

	// Create a base node with index support
	node := &BaseQueryNode{
		hasIndex: true,
		stats: NodeStatistics{
			matchDistance: 1.0, // Indexed access is typically very selective
			timePerMatch:  0.1, // Much faster with index
		},
	}

	return node
}

// OptimizeLogicalExpression optimizes logical expressions
func (qo *QueryOptimizer) OptimizeLogicalExpression(expr *LogicalExpression) QueryNode {
	if expr == nil {
		return nil
	}

	// Create nodes for left and right expressions
	leftNode, _ := qo.createQueryNode(expr.Left)
	rightNode, _ := qo.createQueryNode(expr.Right)

	// Create logical node
	logicalNode := qo.buildLogicalNode(expr, leftNode, rightNode)

	// Calculate combined statistics
	if leftNode != nil && rightNode != nil {
		leftStats := leftNode.GetStatistics()
		rightStats := rightNode.GetStatistics()

		// Calculate combined distance and time based on logical operator
		distance := qo.calculateLogicalDistance(expr.Operator, leftNode, rightNode)
		timePerMatch := qo.calculateLogicalTime(expr.Operator, leftNode, rightNode)

		// Update logical node statistics
		if baseNode, ok := logicalNode.(*BaseQueryNode); ok {
			baseNode.stats.matchDistance = distance
			baseNode.stats.timePerMatch = timePerMatch
			baseNode.stats.matchCount = leftStats.matchCount + rightStats.matchCount
			baseNode.stats.probeCount = leftStats.probeCount + rightStats.probeCount
		}
	}

	return logicalNode
}

// calculateLogicalDistance calculates the match distance for logical operations
func (qo *QueryOptimizer) calculateLogicalDistance(op LogicalOp, left, right QueryNode) float64 {
	leftStats := left.GetStatistics()
	rightStats := right.GetStatistics()

	switch op {
	case OpAnd:
		// AND operation: distance is the maximum of both
		return math.Max(leftStats.matchDistance, rightStats.matchDistance)
	case OpOr:
		// OR operation: distance is the minimum of both
		return math.Min(leftStats.matchDistance, rightStats.matchDistance)
	case OpNot:
		// NOT operation: inverse of the distance
		return 1.0 / leftStats.matchDistance
	default:
		return leftStats.matchDistance
	}
}

// calculateLogicalTime calculates the time per match for logical operations
func (qo *QueryOptimizer) calculateLogicalTime(op LogicalOp, left, right QueryNode) float64 {
	leftStats := left.GetStatistics()
	rightStats := right.GetStatistics()

	switch op {
	case OpAnd:
		// AND operation: sum of both times
		return leftStats.timePerMatch + rightStats.timePerMatch
	case OpOr:
		// OR operation: average of both times
		return (leftStats.timePerMatch + rightStats.timePerMatch) / 2.0
	case OpNot:
		// NOT operation: same as original
		return leftStats.timePerMatch
	default:
		return leftStats.timePerMatch
	}
}

// GetOptimizationMetrics returns optimization metrics
func (qo *QueryOptimizer) GetOptimizationMetrics() *OptimizationMetrics {
	cacheStats := qo.planCache.GetStats()

	return &OptimizationMetrics{
		CacheHitRate:     float64(cacheStats.HitCount) / float64(cacheStats.HitCount+cacheStats.MissCount),
		CacheSize:        cacheStats.Size,
		TotalOptimized:   cacheStats.HitCount + cacheStats.MissCount,
		IndexUtilization: qo.calculateIndexUtilization(),
		AvgOptimizeTime:  qo.calculateAvgOptimizeTime(),
	}
}

// OptimizationMetrics contains optimization performance metrics
type OptimizationMetrics struct {
	CacheHitRate     float64
	CacheSize        int
	TotalOptimized   uint64
	IndexUtilization float64
	AvgOptimizeTime  time.Duration
}

// calculateIndexUtilization calculates how well indexes are being utilized
func (qo *QueryOptimizer) calculateIndexUtilization() float64 {
	if qo.table == nil {
		return 0.0
	}

	indexedColumns := qo.table.GetIndexedColumns()
	totalColumns := len(qo.table.GetColumns())

	if totalColumns == 0 {
		return 0.0
	}

	return float64(len(indexedColumns)) / float64(totalColumns)
}

// calculateAvgOptimizeTime calculates average optimization time
func (qo *QueryOptimizer) calculateAvgOptimizeTime() time.Duration {
	// Simplified implementation - would track actual timing in production
	return time.Microsecond * 100
}

// HasIndex checks if a column has an index
func (qo *QueryOptimizer) HasIndex(columnName string) bool {
	if qo.table == nil {
		return false
	}

	colKey, exists := qo.table.GetColumn(columnName)
	if !exists {
		return false
	}

	indexedColumns := qo.table.GetIndexedColumns()
	for _, indexedCol := range indexedColumns {
		if indexedCol == colKey {
			return true
		}
	}
	return false
}

// GetColumnCount returns the number of columns in the table
func (qo *QueryOptimizer) GetColumnCount() int {
	if qo.table == nil {
		return 0
	}
	return len(qo.table.GetColumns())
}

// verifyAllConditions verifies all conditions in a query plan based on realm-core pattern
func (qo *QueryOptimizer) verifyAllConditionsForPlan(plan *QueryPlan, conditions []QueryExpression) bool {
	if plan == nil || len(conditions) == 0 {
		return true
	}

	// Check if table has required indexes for conditions
	for _, condition := range conditions {
		if compExpr, ok := condition.(*ComparisonExpression); ok {
			if propExpr, ok := compExpr.Left.(*PropertyExpression); ok {
				// Use the corrected HasIndex method with proper field name
				if !qo.HasIndex(propExpr.Name) {
					// Log warning but don't fail - we can still execute without index
					continue
				}
			}
		}
	}

	// Verify node statistics are reasonable
	if plan.rootNode != nil {
		stats := plan.rootNode.GetStatistics()
		if stats.matchDistance < 0 || stats.timePerMatch < 0 {
			return false
		}
	}

	return true
}
