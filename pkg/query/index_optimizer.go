package query

import (
	"time"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/table"
)

// IndexOptimizer handles index-based query optimization
type IndexOptimizer struct {
	table      *table.Table
	indexCache map[keys.ColKey]*IndexInfo
	statistics *IndexStatistics
}

// IndexInfo contains information about a column index
type IndexInfo struct {
	ColKey      keys.ColKey
	IsUnique    bool
	Cardinality uint64
	Cost        float64
	LastUsed    int64
}

// IndexStatistics tracks index usage statistics
type IndexStatistics struct {
	HitCount     uint64
	MissCount    uint64
	TotalQueries uint64
}

// NewIndexOptimizer creates a new index optimizer
func NewIndexOptimizer(tbl *table.Table) *IndexOptimizer {
	return &IndexOptimizer{
		table:      tbl,
		indexCache: make(map[keys.ColKey]*IndexInfo),
		statistics: &IndexStatistics{},
	}
}

// OptimizeComparison optimizes comparison expressions using indexes
func (io *IndexOptimizer) OptimizeComparison(expr *ComparisonExpression) QueryNode {
	// Extract property from comparison
	if prop, ok := expr.Left.(*PropertyExpression); ok {
		return io.optimizePropertyComparison(prop, expr)
	}

	// Fallback to non-indexed comparison
	return &ComparisonQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats: NodeStatistics{
				matchDistance: 100.0, // High cost for non-indexed
				timePerMatch:  1.0,
				probeCount:    0,
				matchCount:    0,
			},
			hasIndex: false,
		},
		expression: expr,
	}
}

// optimizePropertyComparison optimizes property-based comparisons
func (io *IndexOptimizer) optimizePropertyComparison(prop *PropertyExpression, expr *ComparisonExpression) QueryNode {
	// Check if column has index
	indexedCols := io.table.GetIndexedColumns()
	hasIndex := false

	for _, colKey := range indexedCols {
		if colKey == prop.ColKey {
			hasIndex = true
			break
		}
	}

	if hasIndex {
		// Get or create index info
		indexInfo := io.getIndexInfo(prop.ColKey)

		// Calculate cost based on index selectivity
		cost := io.calculateIndexCost(indexInfo, expr.Operator)

		return &ComparisonQueryNode{
			BaseQueryNode: BaseQueryNode{
				stats: NodeStatistics{
					matchDistance: cost,
					timePerMatch:  0.1, // Fast index lookup
					probeCount:    0,
					matchCount:    0,
				},
				hasIndex: true,
			},
			expression: expr,
		}
	}

	// No index available - use table scan
	return &ComparisonQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats: NodeStatistics{
				matchDistance: 1000.0, // High cost for table scan
				timePerMatch:  2.0,    // Slower table scan
				probeCount:    0,
				matchCount:    0,
			},
			hasIndex: false,
		},
		expression: expr,
	}
}

// getIndexInfo retrieves or creates index information
func (io *IndexOptimizer) getIndexInfo(colKey keys.ColKey) *IndexInfo {
	if info, exists := io.indexCache[colKey]; exists {
		return info
	}

	// Create new index info
	info := &IndexInfo{
		ColKey:      colKey,
		IsUnique:    io.isUniqueColumn(colKey),
		Cardinality: io.estimateCardinality(colKey),
		Cost:        1.0,
		LastUsed:    0,
	}

	io.indexCache[colKey] = info
	return info
}

// isUniqueColumn checks if a column has unique constraint
func (io *IndexOptimizer) isUniqueColumn(colKey keys.ColKey) bool {
	uniqueCols := io.table.GetUniqueColumns()
	for _, uniqueCol := range uniqueCols {
		if uniqueCol == colKey {
			return true
		}
	}
	return false
}

// estimateCardinality estimates the cardinality of a column
func (io *IndexOptimizer) estimateCardinality(colKey keys.ColKey) uint64 {
	// Check if we have cached cardinality information
	if info, exists := io.indexCache[colKey]; exists && info.Cardinality > 0 {
		return info.Cardinality
	}

	tableSize := io.table.GetObjectCount()
	if tableSize == 0 {
		return 0
	}

	// For unique columns, cardinality equals table size
	if io.isUniqueColumn(colKey) {
		return tableSize
	}

	// For non-unique columns, estimate based on column type and table size
	// This follows a heuristic approach:
	// - String columns: assume 50% uniqueness
	// - Numeric columns: assume 30% uniqueness
	// - Boolean columns: cardinality is 2
	// - Other types: assume 20% uniqueness

	var estimatedCardinality uint64

	// Get column type from table metadata
	colType, err := io.table.GetColumnType(colKey)
	if err != nil {
		// Fallback to default cardinality if column type cannot be determined
		return tableSize / 5 // 20% uniqueness
	}
	switch colType {
	case keys.TypeString:
		estimatedCardinality = tableSize / 2
	case keys.TypeInt, keys.TypeFloat, keys.TypeDouble:
		estimatedCardinality = tableSize * 3 / 10
	case keys.TypeBool:
		estimatedCardinality = 2
	default:
		estimatedCardinality = tableSize / 5
	}

	// Ensure minimum cardinality of 1
	if estimatedCardinality == 0 {
		estimatedCardinality = 1
	}

	// Cache the estimated cardinality
	if info, exists := io.indexCache[colKey]; exists {
		info.Cardinality = estimatedCardinality
	} else {
		io.indexCache[colKey] = &IndexInfo{
			ColKey:      colKey,
			Cardinality: estimatedCardinality,
			LastUsed:    getCurrentTimestamp(),
		}
	}

	return estimatedCardinality
}

// calculateIndexCost calculates the cost of using an index for a comparison
func (io *IndexOptimizer) calculateIndexCost(info *IndexInfo, op ComparisonOp) float64 {
	baseCost := 1.0

	// Adjust cost based on operator selectivity
	switch op {
	case OpEqual:
		if info.IsUnique {
			return baseCost * 0.1 // Very selective
		}
		return baseCost * 0.5 // Moderately selective

	case OpNotEqual:
		return baseCost * 10.0 // Not very selective

	case OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
		return baseCost * 2.0 // Range queries

	case OpContains, OpBeginsWith, OpEndsWith:
		return baseCost * 5.0 // String operations

	case OpIn:
		return baseCost * 1.5 // IN operations

	case OpBetween:
		return baseCost * 3.0 // Range operations

	default:
		return baseCost * 10.0 // Unknown operations
	}
}

// OptimizeLogical optimizes logical expressions using index information
func (io *IndexOptimizer) OptimizeLogical(expr *LogicalExpression) QueryNode {
	leftNode := io.optimizeExpression(expr.Left)
	rightNode := io.optimizeExpression(expr.Right)

	// Create logical node with optimized children
	logicalNode := &LogicalQueryNode{
		BaseQueryNode: BaseQueryNode{
			children: []QueryNode{leftNode, rightNode},
			hasIndex: leftNode.HasSearchIndex() || rightNode.HasSearchIndex(),
		},
		operator: expr.Operator,
	}

	// Calculate combined cost
	leftCost := leftNode.Cost()
	rightCost := rightNode.Cost()

	switch expr.Operator {
	case OpAnd:
		// AND: execute cheaper condition first
		if leftCost > rightCost {
			logicalNode.children = []QueryNode{rightNode, leftNode}
		}
		logicalNode.stats.matchDistance = (leftCost + rightCost) * 0.5

	case OpOr:
		// OR: combined cost is higher
		logicalNode.stats.matchDistance = leftCost + rightCost

	case OpNot:
		// NOT: slightly higher cost than base condition
		logicalNode.stats.matchDistance = leftCost * 1.2
	}

	return logicalNode
}

// optimizeExpression optimizes any query expression
func (io *IndexOptimizer) optimizeExpression(expr QueryExpression) QueryNode {
	switch e := expr.(type) {
	case *ComparisonExpression:
		return io.OptimizeComparison(e)
	case *LogicalExpression:
		return io.OptimizeLogical(e)
	default:
		// Unknown expression type - create basic node
		return &ComparisonQueryNode{
			BaseQueryNode: BaseQueryNode{
				stats: NodeStatistics{
					matchDistance: 100.0,
					timePerMatch:  1.0,
				},
				hasIndex: false,
			},
		}
	}
}

// UpdateStatistics updates index usage statistics
func (io *IndexOptimizer) UpdateStatistics(colKey keys.ColKey, hit bool) {
	io.statistics.TotalQueries++

	if hit {
		io.statistics.HitCount++
	} else {
		io.statistics.MissCount++
	}

	// Update index-specific statistics
	if info, exists := io.indexCache[colKey]; exists {
		info.LastUsed = getCurrentTimestamp()
	}
}

// GetStatistics returns current index statistics
func (io *IndexOptimizer) GetStatistics() *IndexStatistics {
	return io.statistics
}

// getCurrentTimestamp returns current timestamp in milliseconds since Unix epoch
func getCurrentTimestamp() int64 {
	return time.Now().UnixMilli()
}

// ClearCache clears the index information cache
func (io *IndexOptimizer) ClearCache() {
	io.indexCache = make(map[keys.ColKey]*IndexInfo)
}

// GetIndexInfo returns index information for a column
func (io *IndexOptimizer) GetIndexInfo(colKey keys.ColKey) (*IndexInfo, bool) {
	info, exists := io.indexCache[colKey]
	return info, exists
}

// Constants from realm-core query_engine.hpp
const (
	// Threshold for when conditions overwhelm index performance
	ThresholdConditionsOverwhelmingIndex = 100
)

// CanCombineConditions determines if conditions should be combined based on realm-core logic
// Based on realm-core's consume_condition logic in query_engine.hpp
func (io *IndexOptimizer) CanCombineConditions(colKey keys.ColKey, numConditions int, ignoreIndexes bool) bool {
	// Get index info for the column
	indexInfo := io.getIndexInfo(colKey)
	hasSearchIndex := indexInfo != nil

	// Realm-core logic: combine conditions if:
	// 1. No search index available, OR
	// 2. Indexes are being ignored, OR
	// 3. For non-string columns with many conditions (overwhelms index performance)

	if hasSearchIndex && !ignoreIndexes {
		// For string columns, always prefer index unless overwhelming number of conditions
		if io.isStringColumn(colKey) {
			return numConditions >= ThresholdConditionsOverwhelmingIndex
		}

		// For non-string columns, combine if many conditions
		return numConditions >= ThresholdConditionsOverwhelmingIndex
	}

	// No index or ignoring indexes - combine conditions
	return true
}

// isStringColumn checks if the column is a string type
func (io *IndexOptimizer) isStringColumn(colKey keys.ColKey) bool {
	// Get column type from table metadata
	colType, err := io.table.GetColumnType(colKey)
	if err != nil {
		return false // Default to non-string if type cannot be determined
	}
	return colType == keys.TypeString
}

// OptimizeConditionCombination optimizes multiple conditions on the same column
// Based on realm-core's condition combining logic
func (io *IndexOptimizer) OptimizeConditionCombination(conditions []QueryExpression, colKey keys.ColKey) QueryNode {
	if len(conditions) <= 1 {
		// Single condition - optimize normally
		if len(conditions) == 1 {
			return io.optimizeExpression(conditions[0])
		}
		return nil
	}

	// Check if we should combine conditions
	shouldCombine := io.CanCombineConditions(colKey, len(conditions), false)

	if shouldCombine {
		// Combine conditions into a single optimized node
		return io.createCombinedConditionNode(conditions, colKey)
	}

	// Use index for each condition separately
	return io.createIndexedConditionChain(conditions, colKey)
}

// createCombinedConditionNode creates a node that combines multiple conditions
// Similar to realm-core's StringNode with multiple needles
func (io *IndexOptimizer) createCombinedConditionNode(conditions []QueryExpression, colKey keys.ColKey) QueryNode {
	// Extract values from all conditions
	values := make([]interface{}, 0, len(conditions))

	for _, cond := range conditions {
		if comp, ok := cond.(*ComparisonExpression); ok {
			if literal, ok := comp.Right.(*LiteralExpression); ok {
				values = append(values, literal.Value)
			}
		}
	}

	// Create a combined condition node (similar to realm-core's multi-needle approach)
	return &CombinedConditionNode{
		BaseQueryNode: BaseQueryNode{
			hasIndex: false, // Combined conditions don't use index
		},
		ColKey: colKey,
		Values: values,
	}
}

// createIndexedConditionChain creates a chain of indexed condition nodes
func (io *IndexOptimizer) createIndexedConditionChain(conditions []QueryExpression, colKey keys.ColKey) QueryNode {
	if len(conditions) == 0 {
		return nil
	}

	// Create first node
	rootNode := io.optimizeExpression(conditions[0])

	// Chain remaining conditions
	currentNode := rootNode
	for i := 1; i < len(conditions); i++ {
		nextNode := io.optimizeExpression(conditions[i])

		// Create logical AND node
		andNode := &LogicalQueryNode{
			BaseQueryNode: BaseQueryNode{
				children: []QueryNode{currentNode, nextNode},
			},
			operator: OpAnd,
		}
		currentNode = andNode
	}

	return currentNode
}

// CombinedConditionNode represents multiple conditions combined for efficiency
// Based on realm-core's multi-needle string matching approach
type CombinedConditionNode struct {
	BaseQueryNode
	ColKey keys.ColKey
	Values []interface{}
}

// Cost returns the cost of the combined condition node
func (ccn *CombinedConditionNode) Cost() float64 {
	// Combined conditions have lower cost than individual indexed lookups
	// when there are many conditions (realm-core logic)
	baseCost := float64(len(ccn.Values)) * 0.1 // Lower per-condition cost
	return baseCost
}

// AggregateLocal implements the QueryNode interface
// Based on realm-core's StringNode multi-needle aggregation
func (ccn *CombinedConditionNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	localMatchCount := uint64(0)
	current := start

	// Main aggregation loop - find matches within the range
	for current < end && localMatchCount < localMatches {
		// Find first match starting from current position
		matchPos := ccn.FindFirst(current, end)
		if matchPos == end {
			// No more matches found
			break
		}

		localMatchCount++
		current = matchPos + 1

		// Update statistics based on actual performance
		if localMatchCount > 0 {
			ccn.stats.matchDistance = float64(matchPos-start) / (float64(localMatchCount) + 1.1)
		}
	}

	// Final statistics update
	ccn.updateStatistics(localMatchCount, current-start)
	return current
}

// FindFirst implements the QueryNode interface
// Based on realm-core's StringNode multi-needle search
func (ccn *CombinedConditionNode) FindFirst(start, end uint64) uint64 {
	// Iterate through the range to find first match with any combined value
	for current := start; current < end; current++ {
		// Get object at current position
		objKey := keys.NewObjKey(current)
		obj, err := ccn.GetTable().GetObject(objKey)
		if err != nil {
			continue // Skip invalid objects
		}

		// Check if object matches any of the combined values
		if ccn.Match(obj) {
			return current
		}
	}
	return end // No match found
}

// Match implements the QueryNode interface
func (ccn *CombinedConditionNode) Match(obj *table.Object) bool {
	// Get the column value from the object using the correct method
	value, err := obj.Get(ccn.ColKey)
	if err != nil {
		return false
	}

	// Check if the value matches any of the combined values
	for _, targetValue := range ccn.Values {
		if value == targetValue {
			return true
		}
	}
	return false
}

// Clone implements the QueryNode interface
func (ccn *CombinedConditionNode) Clone() QueryNode {
	clone := &CombinedConditionNode{
		BaseQueryNode: ccn.BaseQueryNode,
		ColKey:        ccn.ColKey,
		Values:        make([]interface{}, len(ccn.Values)),
	}
	copy(clone.Values, ccn.Values)
	return clone
}

// updateStatistics updates node statistics based on matches and probes
func (ccn *CombinedConditionNode) updateStatistics(matches, probes uint64) {
	ccn.stats.probeCount += probes
	ccn.stats.matchCount += matches

	if probes > 0 {
		// Calculate match distance using realm-core formula
		ccn.stats.matchDistance = float64(probes) / (float64(matches) + 1.1)
		// Combined conditions are optimized for fast execution
		ccn.stats.timePerMatch = 0.25 // DT_IntegerNode equivalent
	} else {
		// No probes means no data to calculate statistics
		ccn.stats.matchDistance = 1000000.0 // High cost for unknown
		ccn.stats.timePerMatch = 0.25       // DT_IntegerNode equivalent
	}
}
