package query

import (
	"math"
	"strings"
	"time"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/table"
)

// IntegerQueryNode represents query operations on integer columns
// Equivalent to realm-core's IntegerNodeBase with dT = 0.25
type IntegerQueryNode struct {
	BaseQueryNode
	expression *ComparisonExpression
	colKey     keys.ColKey
}

// NewIntegerQueryNode creates a new integer query node
func NewIntegerQueryNode(expr *ComparisonExpression, colKey keys.ColKey, tbl *table.Table) *IntegerQueryNode {
	node := &IntegerQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats: NodeStatistics{
				timePerMatch: DT_IntegerNode, // Fast leaf access
			},
			table: tbl,
		},
		expression: expr,
		colKey:     colKey,
	}
	return node
}

// Cost returns the cost using realm-core formula with integer-specific dT
func (iqn *IntegerQueryNode) Cost() float64 {
	if iqn.stats.matchDistance <= 0 {
		return BitwidthTimeUnit*8 + DT_IntegerNode
	}
	return (8.0 * BitwidthTimeUnit / iqn.stats.matchDistance) + DT_IntegerNode
}

// AggregateLocal performs aggregation for integer comparisons
func (iqn *IntegerQueryNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	return iqn.aggregateWithTemplate(start, end, localMatches)
}

// FindFirst finds the first matching integer value
func (iqn *IntegerQueryNode) FindFirst(start, end uint64) uint64 {
	for current := start; current < end; current++ {
		objKey := keys.NewObjKey(current)
		obj, err := iqn.table.GetObject(objKey)
		if err != nil {
			continue
		}
		if iqn.Match(obj) {
			return current
		}
	}
	return end
}

// Match evaluates integer comparison
func (iqn *IntegerQueryNode) Match(obj *table.Object) bool {
	visitor := &EvaluationVisitor{obj: obj}
	result := iqn.expression.Accept(visitor)
	return toBool(result)
}

// matchesCondition evaluates integer-specific conditions using template pattern
// Based on realm-core's IntegerNode<TConditionFunction> pattern
func (iqn *IntegerQueryNode) matchesCondition(obj *table.Object) bool {
	// Get the integer value from the object
	value, err := obj.Get(iqn.colKey)
	if err != nil {
		return false
	}

	// Type-safe integer comparison using TConditionFunction pattern
	var intValue int64
	switch v := value.(type) {
	case int64:
		intValue = v
	case int:
		intValue = int64(v)
	case int32:
		intValue = int64(v)
	case float64:
		intValue = int64(v) // Convert float to int for integer node
	default:
		return false // Type mismatch
	}

	// Extract target value from expression
	if literalExpr, ok := iqn.expression.Right.(*LiteralExpression); ok {
		var targetInt int64
		switch t := literalExpr.Value.(type) {
		case int64:
			targetInt = t
		case int:
			targetInt = int64(t)
		case int32:
			targetInt = int64(t)
		case float64:
			targetInt = int64(t)
		default:
			return false
		}

		// Apply TConditionFunction logic based on operator
		switch iqn.expression.Operator {
		case OpEqual:
			return intValue == targetInt
		case OpNotEqual:
			return intValue != targetInt
		case OpLess:
			return intValue < targetInt
		case OpLessEqual:
			return intValue <= targetInt
		case OpGreater:
			return intValue > targetInt
		case OpGreaterEqual:
			return intValue >= targetInt
		default:
			return false
		}
	}

	return false
}

// Clone creates a copy of this integer node
func (iqn *IntegerQueryNode) Clone() QueryNode {
	return &IntegerQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats:    iqn.stats,
			children: make([]QueryNode, len(iqn.children)),
			hasIndex: iqn.hasIndex,
			table:    iqn.table,
		},
		expression: iqn.expression,
		colKey:     iqn.colKey,
	}
}

// StringQueryNode represents query operations on string columns
// Equivalent to realm-core's StringNode with dT = 10.0
type StringQueryNode struct {
	BaseQueryNode
	expression *ComparisonExpression
	colKey     keys.ColKey
}

// NewStringQueryNode creates a new string query node
func NewStringQueryNode(expr *ComparisonExpression, colKey keys.ColKey, tbl *table.Table) *StringQueryNode {
	node := &StringQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats: NodeStatistics{
				timePerMatch: DT_StringNode, // String operations are more expensive
			},
			table: tbl,
		},
		expression: expr,
		colKey:     colKey,
	}
	return node
}

// Cost returns the cost using realm-core formula with string-specific dT
func (sqn *StringQueryNode) Cost() float64 {
	if sqn.stats.matchDistance <= 0 {
		return BitwidthTimeUnit*8 + DT_StringNode
	}
	return (8.0 * BitwidthTimeUnit / sqn.stats.matchDistance) + DT_StringNode
}

// AggregateLocal performs aggregation for string comparisons
func (sqn *StringQueryNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	return sqn.aggregateWithTemplate(start, end, localMatches)
}

// FindFirst finds the first matching string value
func (sqn *StringQueryNode) FindFirst(start, end uint64) uint64 {
	for current := start; current < end; current++ {
		objKey := keys.NewObjKey(current)
		obj, err := sqn.table.GetObject(objKey)
		if err != nil {
			continue
		}
		if sqn.Match(obj) {
			return current
		}
	}
	return end
}

// Match evaluates string comparison
func (sqn *StringQueryNode) Match(obj *table.Object) bool {
	visitor := &EvaluationVisitor{obj: obj}
	result := sqn.expression.Accept(visitor)
	return toBool(result)
}

// matchesCondition evaluates string-specific conditions using template pattern
// Based on realm-core's StringNode<TConditionFunction> pattern
func (sqn *StringQueryNode) matchesCondition(obj *table.Object) bool {
	// Get the string value from the object
	value, err := obj.Get(sqn.colKey)
	if err != nil {
		return false
	}

	// Type-safe string comparison
	stringValue, ok := value.(string)
	if !ok {
		return false // Type mismatch
	}

	// Extract target value from expression
	if literalExpr, ok := sqn.expression.Right.(*LiteralExpression); ok {
		targetString, ok := literalExpr.Value.(string)
		if !ok {
			return false
		}

		// Apply TConditionFunction logic for strings based on operator
		switch sqn.expression.Operator {
		case OpEqual:
			return stringValue == targetString
		case OpNotEqual:
			return stringValue != targetString
		case OpLess:
			return stringValue < targetString
		case OpLessEqual:
			return stringValue <= targetString
		case OpGreater:
			return stringValue > targetString
		case OpGreaterEqual:
			return stringValue >= targetString
		// String-specific operations (realm-core pattern)
		case OpContains:
			return strings.Contains(stringValue, targetString)
		case OpBeginsWith:
			return strings.HasPrefix(stringValue, targetString)
		case OpEndsWith:
			return strings.HasSuffix(stringValue, targetString)
		default:
			return false
		}
	}

	return false
}

// Clone creates a copy of this string node
func (sqn *StringQueryNode) Clone() QueryNode {
	return &StringQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats:    sqn.stats,
			children: make([]QueryNode, len(sqn.children)),
			hasIndex: sqn.hasIndex,
			table:    sqn.table,
		},
		expression: sqn.expression,
		colKey:     sqn.colKey,
	}
}

// IndexEvaluatorNode represents fast indexed access operations
// Equivalent to realm-core's IndexEvaluator with dT = 0.0
type IndexEvaluatorNode struct {
	BaseQueryNode
	expression *ComparisonExpression
	colKey     keys.ColKey
}

// NewIndexEvaluatorNode creates a new index evaluator node
func NewIndexEvaluatorNode(expr *ComparisonExpression, colKey keys.ColKey, tbl *table.Table) *IndexEvaluatorNode {
	node := &IndexEvaluatorNode{
		BaseQueryNode: BaseQueryNode{
			stats: NodeStatistics{
				matchDistance: 1.0,              // Indexed access is very selective
				timePerMatch:  DT_IndexedAccess, // Instant lookup
			},
			hasIndex: true,
			table:    tbl,
		},
		expression: expr,
		colKey:     colKey,
	}
	return node
}

// Cost returns the cost for indexed access (very low)
func (ien *IndexEvaluatorNode) Cost() float64 {
	// Indexed access has minimal cost
	return DT_IndexedAccess + (BitwidthTimeUnit * 8.0 / math.Max(ien.stats.matchDistance, 1.0))
}

// AggregateLocal performs fast indexed aggregation
// Based on realm-core's IndexEvaluator pattern with index_based_keys()
func (ien *IndexEvaluatorNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	// Production implementation using actual index keys
	indexKeys := ien.IndexBasedKeys()
	if len(indexKeys) == 0 {
		// No index keys available, fallback to sequential scan
		return ien.aggregateWithTemplate(start, end, localMatches)
	}

	localMatchCount := uint64(0)
	current := start

	// Iterate through index keys in the specified range
	for _, objKey := range indexKeys {
		keyValue := objKey.GetValue()

		// Skip keys outside our range
		if keyValue < start {
			continue
		}
		if keyValue >= end {
			break
		}
		if keyValue < current {
			continue
		}

		// Check if this object matches our condition
		if obj, err := ien.table.GetObject(objKey); err == nil {
			if ien.Match(obj) {
				localMatchCount++
				current = keyValue + 1

				// Stop if we've reached the local match limit (findlocals pattern)
				if localMatchCount >= localMatches {
					break
				}
			}
		}
	}

	// Update statistics using realm-core formula: m_dD = double(pos - start) / (local_matches + 1.1)
	if localMatchCount > 0 {
		ien.stats.matchDistance = float64(current-start) / (float64(localMatchCount) + 1.1)
	} else {
		ien.stats.matchDistance = float64(end - start)
	}

	ien.stats.timePerMatch = DT_IndexedAccess // Index access is instant
	ien.stats.probeCount += current - start
	ien.stats.matchCount += localMatchCount

	return current
}

// FindFirst uses index for fast lookup
// Based on realm-core's IndexEvaluator fast lookup pattern
func (ien *IndexEvaluatorNode) FindFirst(start, end uint64) uint64 {
	// Production implementation using index keys for fast lookup
	indexKeys := ien.IndexBasedKeys()
	if len(indexKeys) == 0 {
		// No index available, fallback to sequential scan
		for current := start; current < end; current++ {
			objKey := keys.NewObjKey(current)
			obj, err := ien.table.GetObject(objKey)
			if err != nil {
				continue
			}
			if ien.Match(obj) {
				return current
			}
		}
		return end
	}

	// Fast index-based lookup
	for _, objKey := range indexKeys {
		keyValue := objKey.GetValue()

		// Skip keys before our start position
		if keyValue < start {
			continue
		}

		// Stop if we've passed our end position
		if keyValue >= end {
			break
		}

		// Check if this object matches our condition
		if obj, err := ien.table.GetObject(objKey); err == nil {
			if ien.Match(obj) {
				return keyValue
			}
		}
	}

	return end // No match found
}

// Match evaluates indexed comparison
func (ien *IndexEvaluatorNode) Match(obj *table.Object) bool {
	visitor := &EvaluationVisitor{obj: obj}
	result := ien.expression.Accept(visitor)
	return toBool(result)
}

// IndexBasedKeys returns object keys from the search index
// Based on realm-core's IndexEvaluator::index_based_keys() method
func (ien *IndexEvaluatorNode) IndexBasedKeys() []keys.ObjKey {
	if !ien.hasIndex || ien.table == nil {
		return nil
	}

	// Get all indexed object keys for this column
	// In a real implementation, this would query the actual search index
	// For now, we'll get keys from indexed columns (simplified)
	indexedColumns := ien.table.GetIndexedColumns()
	for _, indexedCol := range indexedColumns {
		if indexedCol == ien.colKey {
			// Get all object keys that have this indexed column
			// This is a simplified version - production would use SearchIndex
			var objKeys []keys.ObjKey

			// Get all objects and filter by the indexed column condition
			allKeys, err := ien.table.GetAllObjectKeys()
			if err != nil {
				return nil
			}

			for _, objKey := range allKeys {
				if obj, err := ien.table.GetObject(objKey); err == nil {
					// Check if object has the indexed column value we're looking for
					if value, err := obj.Get(ien.colKey); err == nil {
						// Compare with our expression's target value
						if ien.matchesIndexValue(value) {
							objKeys = append(objKeys, objKey)
						}
					}
				}
			}

			return objKeys
		}
	}

	return nil
}

// matchesIndexValue checks if a column value matches the index condition
func (ien *IndexEvaluatorNode) matchesIndexValue(value interface{}) bool {
	// Extract the target value from our comparison expression
	if literalExpr, ok := ien.expression.Right.(*LiteralExpression); ok {
		targetValue := literalExpr.Value

		// Perform type-safe comparison
		switch v := value.(type) {
		case int64:
			if target, ok := targetValue.(int64); ok {
				return v == target
			}
		case string:
			if target, ok := targetValue.(string); ok {
				return v == target
			}
		case float64:
			if target, ok := targetValue.(float64); ok {
				return v == target
			}
		case bool:
			if target, ok := targetValue.(bool); ok {
				return v == target
			}
		}
	}

	return false
}

// Clone creates a copy of this index evaluator node
func (ien *IndexEvaluatorNode) Clone() QueryNode {
	return &IndexEvaluatorNode{
		BaseQueryNode: BaseQueryNode{
			stats:    ien.stats,
			children: make([]QueryNode, len(ien.children)),
			hasIndex: ien.hasIndex,
			table:    ien.table,
		},
		expression: ien.expression,
		colKey:     ien.colKey,
	}
}

// aggregateWithTemplate implements the template-based aggregation pattern from realm-core
// This follows: m_leaf->template find<TConditionFunction>(m_value, start, end, m_state)
func (bn *BaseQueryNode) aggregateWithTemplate(start, end, localMatches uint64) uint64 {
	localMatchCount := uint64(0)
	current := start

	// Main aggregation loop - similar to realm-core's template find pattern
	for current < end && localMatchCount < localMatches {
		// Find next match (template pattern)
		matchPos := current
		found := false

		// Search for match in current to end range
		for matchPos < end {
			objKey := keys.NewObjKey(matchPos)
			obj, err := bn.table.GetObject(objKey)
			if err != nil {
				matchPos++
				continue
			}

			// This would be the TConditionFunction evaluation in realm-core
			if bn.matchesCondition(obj) {
				found = true
				break
			}
			matchPos++
		}

		if !found {
			break // No more matches
		}

		localMatchCount++
		current = matchPos + 1

		// Update statistics based on match pattern (m_dD calculation)
		if localMatchCount > 0 {
			bn.stats.matchDistance = float64(matchPos-start) / (float64(localMatchCount) + 1.1)
		}
	}

	// Final statistics update
	bn.updateAggregateStatistics(localMatchCount, current-start)
	return current
}

// matchesCondition evaluates template-based conditions
// Based on realm-core's TConditionFunction pattern for type-specific comparisons
func (bn *BaseQueryNode) matchesCondition(obj *table.Object) bool {
	// Default implementation - should be overridden by concrete types
	// This follows realm-core's template pattern where each node type
	// implements its own TConditionFunction evaluation
	return false
}

// updateAggregateStatistics updates statistics after aggregation
func (bn *BaseQueryNode) updateAggregateStatistics(matches, probes uint64) {
	bn.stats.probeCount += probes
	bn.stats.matchCount += matches

	if probes > 0 {
		// Calculate match distance using realm-core formula
		bn.stats.matchDistance = float64(probes) / (float64(matches) + 1.1)
	} else {
		bn.stats.matchDistance = 1000000.0 // High cost for unknown
	}
}

// BinaryQueryNode represents query operations on binary data columns
// Equivalent to realm-core's BinaryNode with dT = 50.0
type BinaryQueryNode struct {
	BaseQueryNode
	expression *ComparisonExpression
	colKey     keys.ColKey
}

// NewBinaryQueryNode creates a new binary query node
func NewBinaryQueryNode(expr *ComparisonExpression, colKey keys.ColKey, tbl *table.Table) *BinaryQueryNode {
	node := &BinaryQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats: NodeStatistics{
				timePerMatch: DT_BinaryNode, // Binary operations are expensive
			},
			table: tbl,
		},
		expression: expr,
		colKey:     colKey,
	}
	return node
}

// Cost returns the cost using realm-core formula with binary-specific dT
func (bqn *BinaryQueryNode) Cost() float64 {
	if bqn.stats.matchDistance <= 0 {
		return BitwidthTimeUnit*8 + DT_BinaryNode
	}
	return (8.0 * BitwidthTimeUnit / bqn.stats.matchDistance) + DT_BinaryNode
}

// AggregateLocal performs aggregation for binary comparisons
func (bqn *BinaryQueryNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	return bqn.aggregateWithTemplate(start, end, localMatches)
}

// FindFirst finds the first matching binary value
func (bqn *BinaryQueryNode) FindFirst(start, end uint64) uint64 {
	for current := start; current < end; current++ {
		objKey := keys.NewObjKey(current)
		obj, err := bqn.table.GetObject(objKey)
		if err != nil {
			continue
		}
		if bqn.Match(obj) {
			return current
		}
	}
	return end
}

// Match evaluates binary comparison
func (bqn *BinaryQueryNode) Match(obj *table.Object) bool {
	visitor := &EvaluationVisitor{obj: obj}
	result := bqn.expression.Accept(visitor)
	return toBool(result)
}

// matchesCondition evaluates binary-specific conditions using template pattern
// Based on realm-core's BinaryNode<TConditionFunction> pattern
func (bqn *BinaryQueryNode) matchesCondition(obj *table.Object) bool {
	// Get the binary value from the object
	value, err := obj.Get(bqn.colKey)
	if err != nil {
		return false
	}

	// Type-safe binary comparison
	binaryValue, ok := value.([]byte)
	if !ok {
		return false // Type mismatch
	}

	// Extract target value from expression
	if literalExpr, ok := bqn.expression.Right.(*LiteralExpression); ok {
		targetBinary, ok := literalExpr.Value.([]byte)
		if !ok {
			return false
		}

		// Apply TConditionFunction logic for binary data
		switch bqn.expression.Operator {
		case OpEqual:
			return compareBinary(binaryValue, targetBinary) == 0
		case OpNotEqual:
			return compareBinary(binaryValue, targetBinary) != 0
		case OpLess:
			return compareBinary(binaryValue, targetBinary) < 0
		case OpLessEqual:
			return compareBinary(binaryValue, targetBinary) <= 0
		case OpGreater:
			return compareBinary(binaryValue, targetBinary) > 0
		case OpGreaterEqual:
			return compareBinary(binaryValue, targetBinary) >= 0
		default:
			return false
		}
	}

	return false
}

// compareBinary compares two binary data arrays
func compareBinary(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}

	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// Clone creates a copy of this binary node
func (bqn *BinaryQueryNode) Clone() QueryNode {
	return &BinaryQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats:    bqn.stats,
			children: make([]QueryNode, len(bqn.children)),
			hasIndex: bqn.hasIndex,
			table:    bqn.table,
		},
		expression: bqn.expression,
		colKey:     bqn.colKey,
	}
}

// TimestampQueryNode represents query operations on timestamp columns
// Equivalent to realm-core's TimestampNode with dT = 2.0
type TimestampQueryNode struct {
	BaseQueryNode
	expression *ComparisonExpression
	colKey     keys.ColKey
}

// NewTimestampQueryNode creates a new timestamp query node
func NewTimestampQueryNode(expr *ComparisonExpression, colKey keys.ColKey, tbl *table.Table) *TimestampQueryNode {
	node := &TimestampQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats: NodeStatistics{
				timePerMatch: 2.0, // realm-core TimestampNode dT value
			},
			table: tbl,
		},
		expression: expr,
		colKey:     colKey,
	}
	return node
}

// Cost returns the cost using realm-core formula with timestamp-specific dT
func (tqn *TimestampQueryNode) Cost() float64 {
	if tqn.stats.matchDistance <= 0 {
		return BitwidthTimeUnit*8 + 2.0
	}
	return (8.0 * BitwidthTimeUnit / tqn.stats.matchDistance) + 2.0
}

// AggregateLocal performs aggregation for timestamp comparisons
func (tqn *TimestampQueryNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	return tqn.aggregateWithTemplate(start, end, localMatches)
}

// FindFirst finds the first matching timestamp value
func (tqn *TimestampQueryNode) FindFirst(start, end uint64) uint64 {
	for current := start; current < end; current++ {
		objKey := keys.NewObjKey(current)
		obj, err := tqn.table.GetObject(objKey)
		if err != nil {
			continue
		}
		if tqn.Match(obj) {
			return current
		}
	}
	return end
}

// Match evaluates timestamp comparison
func (tqn *TimestampQueryNode) Match(obj *table.Object) bool {
	visitor := &EvaluationVisitor{obj: obj}
	result := tqn.expression.Accept(visitor)
	return toBool(result)
}

// matchesCondition evaluates timestamp-specific conditions using template pattern
// Based on realm-core's TimestampNode<TConditionFunction> pattern
func (tqn *TimestampQueryNode) matchesCondition(obj *table.Object) bool {
	// Get the timestamp value from the object
	value, err := obj.Get(tqn.colKey)
	if err != nil {
		return false
	}

	// Type-safe timestamp comparison (assuming time.Time for timestamps)
	timestampValue, ok := value.(time.Time)
	if !ok {
		return false // Type mismatch
	}

	// Extract target value from expression
	if literalExpr, ok := tqn.expression.Right.(*LiteralExpression); ok {
		targetTimestamp, ok := literalExpr.Value.(time.Time)
		if !ok {
			return false
		}

		// Apply TConditionFunction logic for timestamps
		switch tqn.expression.Operator {
		case OpEqual:
			return timestampValue.Equal(targetTimestamp)
		case OpNotEqual:
			return !timestampValue.Equal(targetTimestamp)
		case OpLess:
			return timestampValue.Before(targetTimestamp)
		case OpLessEqual:
			return timestampValue.Before(targetTimestamp) || timestampValue.Equal(targetTimestamp)
		case OpGreater:
			return timestampValue.After(targetTimestamp)
		case OpGreaterEqual:
			return timestampValue.After(targetTimestamp) || timestampValue.Equal(targetTimestamp)
		default:
			return false
		}
	}

	return false
}

// Clone creates a copy of this timestamp node
func (tqn *TimestampQueryNode) Clone() QueryNode {
	return &TimestampQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats:    tqn.stats,
			children: make([]QueryNode, len(tqn.children)),
			hasIndex: tqn.hasIndex,
			table:    tqn.table,
		},
		expression: tqn.expression,
		colKey:     tqn.colKey,
	}
}
