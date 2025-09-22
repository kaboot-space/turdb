package query

import (
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/table"
)

// ComparisonQueryNode represents a comparison operation node
type ComparisonQueryNode struct {
	BaseQueryNode
	expression *ComparisonExpression
}

// findAllLocal performs find_all_local equivalent for comparison nodes
// Based on realm-core's IntegerNodeBase::find_all_local template implementation
// This follows: m_leaf->template find<TConditionFunction>(m_value, start, end, m_state)
func (cqn *ComparisonQueryNode) findAllLocal(start, end uint64) uint64 {
	// Simulate realm-core's leaf template find pattern
	// In realm-core: m_leaf->template find<TConditionFunction>(m_value, start, end, m_state)
	for i := start; i < end; i++ {
		// Get object at current position
		objKey := keys.NewObjKey(i)
		obj, err := cqn.getTable().GetObject(objKey)
		if err != nil {
			continue // Skip invalid objects
		}

		// Check if object matches the comparison expression (TConditionFunction equivalent)
		if cqn.Match(obj) {
			// Found a match at position i
			// In realm-core this would call: state->match(i)
			// For now we just continue - the caller handles match accumulation
		}
	}
	return end
}

// AggregateLocal performs local aggregation on a range of objects
func (cqn *ComparisonQueryNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	// Use find_all_local pattern for single condition nodes
	// This follows realm-core's ParentNode::aggregate_local when m_nb_needles == 0
	if localMatches == 0 {
		return cqn.findAllLocal(start, end)
	}

	localMatchCount := uint64(0)
	current := start

	// Main aggregation loop - find matches within the range
	for current < end && localMatchCount < localMatches {
		// Find first match starting from current position
		matchPos := cqn.FindFirst(current, end)
		if matchPos == end {
			// No more matches found
			break
		}

		localMatchCount++
		current = matchPos + 1

		// Update statistics based on actual performance (m_dD calculation)
		if localMatchCount > 0 {
			cqn.stats.matchDistance = float64(matchPos-start) / (float64(localMatchCount) + 1.1)
		}
	}

	// Final statistics update
	cqn.updateStatistics(localMatchCount, current-start)
	return current
}

// FindFirst finds the first matching object in the given range
func (cqn *ComparisonQueryNode) FindFirst(start, end uint64) uint64 {
	// Iterate through the range to find first match
	for current := start; current < end; current++ {
		// Get object at current position
		objKey := keys.NewObjKey(current)
		obj, err := cqn.getTable().GetObject(objKey)
		if err != nil {
			continue // Skip invalid objects
		}

		// Check if object matches the comparison expression
		if cqn.Match(obj) {
			return current
		}
	}
	return end // No match found (equivalent to not_found)
}

// getTable returns the table associated with this node
// This follows realm-core ParentNode pattern where m_table is accessed directly
func (cqn *ComparisonQueryNode) getTable() *table.Table {
	return cqn.BaseQueryNode.GetTable()
}

// Match evaluates if the given object matches this comparison
func (cqn *ComparisonQueryNode) Match(obj *table.Object) bool {
	// Create evaluation visitor to check the comparison
	visitor := &EvaluationVisitor{
		obj: obj,
	}

	result := cqn.expression.Accept(visitor)
	return toBool(result)
}

// Clone creates a copy of this node
func (cqn *ComparisonQueryNode) Clone() QueryNode {
	return &ComparisonQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats:    cqn.stats,
			children: make([]QueryNode, len(cqn.children)),
			hasIndex: cqn.hasIndex,
		},
		expression: cqn.expression,
	}
}

// updateStatistics updates node statistics based on matches and probes
func (cqn *ComparisonQueryNode) updateStatistics(matches, probes uint64) {
	cqn.stats.probeCount += probes
	cqn.stats.matchCount += matches

	if probes > 0 {
		// Calculate match distance (average distance between matches)
		// Based on m_dD = double(pos - start) / (matches + 1.1)
		cqn.stats.matchDistance = float64(probes) / (float64(matches) + 1.1)

		// Update time per match (simplified calculation)
		cqn.stats.timePerMatch = 1.0 / (float64(matches) + 1.0)
	} else {
		// No probes means no data to calculate statistics
		cqn.stats.matchDistance = 1000000.0 // High cost for unknown
		cqn.stats.timePerMatch = 1.0
	}
}

// LogicalQueryNode represents a logical operation node (AND, OR, NOT)
type LogicalQueryNode struct {
	BaseQueryNode
	operator LogicalOp
}

// AggregateLocal performs local aggregation for logical operations
func (lqn *LogicalQueryNode) AggregateLocal(start, end, localMatches uint64) uint64 {
	switch lqn.operator {
	case OpAnd:
		return lqn.aggregateAnd(start, end, localMatches)
	case OpOr:
		return lqn.aggregateOr(start, end, localMatches)
	case OpNot:
		return lqn.aggregateNot(start, end, localMatches)
	default:
		return end
	}
}

// aggregateAnd performs AND aggregation across child nodes
// Based on realm-core's ParentNode::aggregate_local for AND logic
func (lqn *LogicalQueryNode) aggregateAnd(start, end, localMatches uint64) uint64 {
	localMatchCount := uint64(0)

	// If only one child, use find_all_local equivalent
	if len(lqn.children) == 1 {
		return lqn.findAllLocal(start, end)
	}

	r := start
	for r < end && localMatchCount < localMatches {
		// Find first match in the first condition node
		r = lqn.children[0].FindFirst(r, end)
		if r == end {
			break // No more matches in first condition
		}

		// Check if all remaining children match at position r (AND logic)
		allMatch := true
		for c := 1; c < len(lqn.children); c++ {
			match := lqn.children[c].FindFirst(r, r+1)
			if match != r {
				allMatch = false
				break // No match in this child, so overall no match
			}
		}

		// If all children match at position r, we have a final match
		if allMatch {
			localMatchCount++
			// In a full implementation, this would call state.match(r)
		}

		r++ // Move to next position
	}

	// Update statistics based on actual performance
	lqn.updateStatistics(localMatchCount, r-start)
	return r
}

// aggregateOr performs OR aggregation across child nodes
// Based on realm-core's OrNode::find_first_local implementation
func (lqn *LogicalQueryNode) aggregateOr(start, end, localMatches uint64) uint64 {
	localMatchCount := uint64(0)

	// If only one child, use find_all_local equivalent
	if len(lqn.children) == 1 {
		return lqn.findAllLocal(start, end)
	}

	r := start
	for r < end && localMatchCount < localMatches {
		// Find the earliest match among all children (OR logic)
		nextMatch := end
		matchingChild := -1

		// Check all children to find the earliest match
		for c := 0; c < len(lqn.children); c++ {
			match := lqn.children[c].FindFirst(r, end)
			if match < nextMatch {
				nextMatch = match
				matchingChild = c
			}
		}

		// If no child has a match, we're done
		if matchingChild == -1 || nextMatch == end {
			break
		}

		// We found a match at nextMatch position
		r = nextMatch
		localMatchCount++
		// In a full implementation, this would call state.match(r)

		r++ // Move to next position
	}

	// Update statistics based on actual performance
	lqn.updateStatistics(localMatchCount, r-start)
	return r
}

// aggregateNot performs NOT aggregation (negation) on child nodes
// Based on realm-core's NotNode::find_first_local implementation
func (lqn *LogicalQueryNode) aggregateNot(start, end, localMatches uint64) uint64 {
	localMatchCount := uint64(0)

	// NOT operation should only have one child
	if len(lqn.children) != 1 {
		// Invalid NOT operation with multiple children
		return end
	}

	child := lqn.children[0]
	r := start

	for r < end && localMatchCount < localMatches {
		// Find the next match in the child condition
		childMatch := child.FindFirst(r, end)

		// For NOT logic, we want positions where the child does NOT match
		// So we iterate through positions before the child's next match
		for r < childMatch && r < end && localMatchCount < localMatches {
			// This position r is NOT matched by the child, so it's a match for NOT
			localMatchCount++
			// In a full implementation, this would call state.match(r)
			r++
		}

		// If we've reached the child's match position, skip it
		if r == childMatch && r < end {
			r++
		}
	}

	// Update statistics based on actual performance
	lqn.updateStatistics(localMatchCount, r-start)
	return r
}

// findAllLocal is a helper method that finds all matches in a range
// Based on realm-core's ParentNode::find_all_local implementation
// This follows the template pattern: m_leaf->template find<TConditionFunction>(m_value, start, end, m_state)
func (lqn *LogicalQueryNode) findAllLocal(start, end uint64) uint64 {
	// If only one child, delegate to its find_all_local equivalent
	if len(lqn.children) == 1 {
		child := lqn.children[0]

		// Simulate realm-core's template find pattern
		// In realm-core: m_leaf->template find<TConditionFunction>(m_value, start, end, m_state)
		for i := start; i < end; i++ {
			match := child.FindFirst(i, i+1)
			if match == i {
				// Found a match at position i
				// In realm-core this would call: state->match(i)
				// For now we just continue - the caller handles match accumulation
			}
		}
		return end
	}

	// For multiple children, use iterative approach
	r := start
	for r < end {
		match := lqn.FindFirst(r, end)
		if match == end {
			break // No more matches found
		}

		// Found a match at position 'match'
		// In realm-core this would call: state->match(match)
		// The actual match recording is handled by the calling aggregation method

		// Move to the next position after the match
		r = match + 1
	}

	return r
}

// FindFirst finds the first matching object in the given range for logical operations
// Based on realm-core's logical node find_first_local implementations
func (lqn *LogicalQueryNode) FindFirst(start, end uint64) uint64 {
	switch lqn.operator {
	case OpAnd:
		return lqn.findFirstAnd(start, end)
	case OpOr:
		return lqn.findFirstOr(start, end)
	case OpNot:
		return lqn.findFirstNot(start, end)
	default:
		return end // Unknown operation
	}
}

// findFirstAnd finds first position where all children match
func (lqn *LogicalQueryNode) findFirstAnd(start, end uint64) uint64 {
	for current := start; current < end; current++ {
		allMatch := true
		for _, child := range lqn.children {
			if child.FindFirst(current, current+1) != current {
				allMatch = false
				break
			}
		}
		if allMatch {
			return current
		}
	}
	return end
}

// findFirstOr finds first position where any child matches
func (lqn *LogicalQueryNode) findFirstOr(start, end uint64) uint64 {
	nextMatch := end
	for _, child := range lqn.children {
		match := child.FindFirst(start, end)
		if match < nextMatch {
			nextMatch = match
		}
	}
	return nextMatch
}

// findFirstNot finds first position where child doesn't match
func (lqn *LogicalQueryNode) findFirstNot(start, end uint64) uint64 {
	if len(lqn.children) != 1 {
		return end // NOT requires exactly one child
	}

	child := lqn.children[0]
	for current := start; current < end; current++ {
		if child.FindFirst(current, current+1) != current {
			return current // Child doesn't match, so NOT condition is satisfied
		}
	}
	return end
}

// Match evaluates if the given object matches this logical condition
func (lqn *LogicalQueryNode) Match(obj *table.Object) bool {
	switch lqn.operator {
	case OpAnd:
		for _, child := range lqn.children {
			if !child.Match(obj) {
				return false
			}
		}
		return true

	case OpOr:
		for _, child := range lqn.children {
			if child.Match(obj) {
				return true
			}
		}
		return false

	case OpNot:
		if len(lqn.children) > 0 {
			return !lqn.children[0].Match(obj)
		}
		return false

	default:
		return false
	}
}

// Clone creates a copy of this logical node
func (lqn *LogicalQueryNode) Clone() QueryNode {
	clonedChildren := make([]QueryNode, len(lqn.children))
	for i, child := range lqn.children {
		clonedChildren[i] = child.Clone()
	}

	return &LogicalQueryNode{
		BaseQueryNode: BaseQueryNode{
			stats:    lqn.stats,
			children: clonedChildren,
			hasIndex: lqn.hasIndex,
		},
		operator: lqn.operator,
	}
}

// updateStatistics updates node statistics based on matches and probes
// Based on realm-core's logical node statistics calculation
func (lqn *LogicalQueryNode) updateStatistics(matches, probes uint64) {
	lqn.stats.probeCount += probes
	lqn.stats.matchCount += matches

	if probes > 0 {
		// Calculate match distance for logical operations
		// For AND: higher cost due to multiple condition checks
		// For OR: lower cost as we stop at first match
		// For NOT: similar to single condition
		switch lqn.operator {
		case OpAnd:
			// AND operations are more expensive due to multiple checks
			lqn.stats.matchDistance = float64(probes) * 1.5 / (float64(matches) + 1.1)
		case OpOr:
			// OR operations are cheaper as we stop at first match
			lqn.stats.matchDistance = float64(probes) * 0.8 / (float64(matches) + 1.1)
		case OpNot:
			// NOT operations have similar cost to regular conditions
			lqn.stats.matchDistance = float64(probes) / (float64(matches) + 1.1)
		}

		// Update time per match based on operation complexity
		childCount := float64(len(lqn.children))
		lqn.stats.timePerMatch = (1.0 + childCount*0.1) / (float64(matches) + 1.0)
	} else {
		// No probes means no data to calculate statistics
		lqn.stats.matchDistance = 1000000.0 // High cost for unknown
		lqn.stats.timePerMatch = 1.0
	}
}
