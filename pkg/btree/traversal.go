package btree

import (
	"fmt"
)

// TraversalOrder defines the order of traversal
type TraversalOrder int

const (
	InOrder TraversalOrder = iota
	PreOrder
	PostOrder
	LevelOrder
)

// TraversalCallback represents a callback function for traversal
type TraversalCallback func(key uint64, value interface{}, depth int) bool

// RangeCallback represents a callback function for range traversal
type RangeCallback func(key uint64, value interface{}) bool

// AdvancedTraversal provides advanced traversal methods for B-tree
type AdvancedTraversal struct {
	tree *BTree
}

// NewAdvancedTraversal creates a new advanced traversal instance
func NewAdvancedTraversal(tree *BTree) *AdvancedTraversal {
	return &AdvancedTraversal{tree: tree}
}

// DepthFirstTraversal performs depth-first traversal of the B-tree
func (at *AdvancedTraversal) DepthFirstTraversal(order TraversalOrder, callback TraversalCallback) error {
	at.tree.mutex.RLock()
	defer at.tree.mutex.RUnlock()
	
	if at.tree.root == nil {
		return nil
	}
	
	switch order {
	case InOrder:
		return at.inOrderTraversal(at.tree.root, callback, 0)
	case PreOrder:
		return at.preOrderTraversal(at.tree.root, callback, 0)
	case PostOrder:
		return at.postOrderTraversal(at.tree.root, callback, 0)
	default:
		return fmt.Errorf("unsupported traversal order for depth-first: %v", order)
	}
}

// BreadthFirstTraversal performs breadth-first (level-order) traversal
func (at *AdvancedTraversal) BreadthFirstTraversal(callback TraversalCallback) error {
	at.tree.mutex.RLock()
	defer at.tree.mutex.RUnlock()
	
	if at.tree.root == nil {
		return nil
	}
	
	queue := []*nodeWithDepth{{node: at.tree.root, depth: 0}}
	
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		
		node := current.node
		depth := current.depth
		
		// Process current node
		if node.isLeaf {
			for i, key := range node.keys {
				if i < len(node.values) {
					if !callback(key, node.values[i], depth) {
						return nil // Stop traversal
					}
				}
			}
		} else {
			// For internal nodes, process keys
			for _, key := range node.keys {
				if !callback(key, nil, depth) {
					return nil // Stop traversal
				}
			}
		}
		
		// Add children to queue
		if !node.isLeaf {
			for _, child := range node.children {
				if child != nil {
					queue = append(queue, &nodeWithDepth{node: child, depth: depth + 1})
				}
			}
		}
	}
	
	return nil
}

// RangeTraversal performs traversal within a specified key range
func (at *AdvancedTraversal) RangeTraversal(startKey, endKey uint64, callback RangeCallback) error {
	at.tree.mutex.RLock()
	defer at.tree.mutex.RUnlock()
	
	if at.tree.root == nil {
		return nil
	}
	
	return at.rangeTraversalNode(at.tree.root, startKey, endKey, callback)
}

// ConditionalTraversal performs traversal with a condition predicate
func (at *AdvancedTraversal) ConditionalTraversal(condition func(key uint64, value interface{}) bool, callback TraversalCallback) error {
	at.tree.mutex.RLock()
	defer at.tree.mutex.RUnlock()
	
	if at.tree.root == nil {
		return nil
	}
	
	return at.conditionalTraversalNode(at.tree.root, condition, callback, 0)
}

// ParallelRangeTraversal performs parallel traversal of multiple ranges
func (at *AdvancedTraversal) ParallelRangeTraversal(ranges []KeyRange, callback RangeCallback) error {
	at.tree.mutex.RLock()
	defer at.tree.mutex.RUnlock()
	
	if at.tree.root == nil {
		return nil
	}
	
	// Process each range sequentially for now
	// In a real implementation, this could be parallelized
	for _, keyRange := range ranges {
		err := at.rangeTraversalNode(at.tree.root, keyRange.Start, keyRange.End, callback)
		if err != nil {
			return err
		}
	}
	
	return nil
}

// KeyRange represents a range of keys
type KeyRange struct {
	Start uint64
	End   uint64
}

// nodeWithDepth helper struct for breadth-first traversal
type nodeWithDepth struct {
	node  *BTreeNode
	depth int
}

// inOrderTraversal performs in-order traversal (left, root, right)
func (at *AdvancedTraversal) inOrderTraversal(node *BTreeNode, callback TraversalCallback, depth int) error {
	if node == nil {
		return nil
	}
	
	if node.isLeaf {
		// For leaf nodes, process all key-value pairs
		for i, key := range node.keys {
			if i < len(node.values) {
				if !callback(key, node.values[i], depth) {
					return nil // Stop traversal
				}
			}
		}
	} else {
		// For internal nodes, interleave children and keys
		for i := 0; i < len(node.keys); i++ {
			// Process left child
			if i < len(node.children) {
				err := at.inOrderTraversal(node.children[i], callback, depth+1)
				if err != nil {
					return err
				}
			}
			
			// Process current key
			if !callback(node.keys[i], nil, depth) {
				return nil // Stop traversal
			}
		}
		
		// Process rightmost child
		if len(node.children) > len(node.keys) {
			err := at.inOrderTraversal(node.children[len(node.children)-1], callback, depth+1)
			if err != nil {
				return err
			}
		}
	}
	
	return nil
}

// preOrderTraversal performs pre-order traversal (root, left, right)
func (at *AdvancedTraversal) preOrderTraversal(node *BTreeNode, callback TraversalCallback, depth int) error {
	if node == nil {
		return nil
	}
	
	// Process current node first
	if node.isLeaf {
		for i, key := range node.keys {
			if i < len(node.values) {
				if !callback(key, node.values[i], depth) {
					return nil // Stop traversal
				}
			}
		}
	} else {
		for _, key := range node.keys {
			if !callback(key, nil, depth) {
				return nil // Stop traversal
			}
		}
		
		// Then process children
		for _, child := range node.children {
			if child != nil {
				err := at.preOrderTraversal(child, callback, depth+1)
				if err != nil {
					return err
				}
			}
		}
	}
	
	return nil
}

// postOrderTraversal performs post-order traversal (left, right, root)
func (at *AdvancedTraversal) postOrderTraversal(node *BTreeNode, callback TraversalCallback, depth int) error {
	if node == nil {
		return nil
	}
	
	if !node.isLeaf {
		// Process children first
		for _, child := range node.children {
			if child != nil {
				err := at.postOrderTraversal(child, callback, depth+1)
				if err != nil {
					return err
				}
			}
		}
	}
	
	// Then process current node
	if node.isLeaf {
		for i, key := range node.keys {
			if i < len(node.values) {
				if !callback(key, node.values[i], depth) {
					return nil // Stop traversal
				}
			}
		}
	} else {
		for _, key := range node.keys {
			if !callback(key, nil, depth) {
				return nil // Stop traversal
			}
		}
	}
	
	return nil
}

// rangeTraversalNode performs range traversal on a specific node
func (at *AdvancedTraversal) rangeTraversalNode(node *BTreeNode, startKey, endKey uint64, callback RangeCallback) error {
	if node == nil {
		return nil
	}
	
	if node.isLeaf {
		// For leaf nodes, check each key-value pair
		for i, key := range node.keys {
			if key >= startKey && key <= endKey && i < len(node.values) {
				if !callback(key, node.values[i]) {
					return nil // Stop traversal
				}
			}
		}
	} else {
		// For internal nodes, traverse relevant children
		for i, key := range node.keys {
			// Traverse left child if it might contain keys in range
			if i < len(node.children) && startKey < key {
				err := at.rangeTraversalNode(node.children[i], startKey, endKey, callback)
				if err != nil {
					return err
				}
			}
			
			// Process current key if in range
			if key >= startKey && key <= endKey {
				if !callback(key, nil) {
					return nil // Stop traversal
				}
			}
		}
		
		// Traverse rightmost child if it might contain keys in range
		if len(node.children) > len(node.keys) {
			lastKey := uint64(0)
			if len(node.keys) > 0 {
				lastKey = node.keys[len(node.keys)-1]
			}
			if endKey > lastKey {
				err := at.rangeTraversalNode(node.children[len(node.children)-1], startKey, endKey, callback)
				if err != nil {
					return err
				}
			}
		}
	}
	
	return nil
}

// conditionalTraversalNode performs conditional traversal on a specific node
func (at *AdvancedTraversal) conditionalTraversalNode(node *BTreeNode, condition func(key uint64, value interface{}) bool, callback TraversalCallback, depth int) error {
	if node == nil {
		return nil
	}
	
	if node.isLeaf {
		for i, key := range node.keys {
			if i < len(node.values) && condition(key, node.values[i]) {
				if !callback(key, node.values[i], depth) {
					return nil // Stop traversal
				}
			}
		}
	} else {
		// For internal nodes, traverse all children
		for _, child := range node.children {
			if child != nil {
				err := at.conditionalTraversalNode(child, condition, callback, depth+1)
				if err != nil {
					return err
				}
			}
		}
	}
	
	return nil
}