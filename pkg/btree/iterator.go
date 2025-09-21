package btree

import (
	"fmt"
)

// Iterator provides sequential access to B-tree elements with caching support
type Iterator struct {
	tree        *BTree
	currentNode *BTreeNode
	currentPos  int
	valid       bool
	
	// Prefetching support
	prefetchEnabled bool
	prefetchBuffer  []*BTreeNode
	prefetchSize    int
}

// NewIterator creates a new iterator for the B-tree
func NewIterator(tree *BTree) *Iterator {
	return &Iterator{
		tree:            tree,
		prefetchEnabled: false,
		prefetchSize:    3, // Default prefetch size
	}
}

// NewIteratorWithPrefetch creates a new iterator with prefetching enabled
func NewIteratorWithPrefetch(tree *BTree, prefetchSize int) *Iterator {
	return &Iterator{
		tree:            tree,
		prefetchEnabled: true,
		prefetchSize:    prefetchSize,
		prefetchBuffer:  make([]*BTreeNode, 0, prefetchSize),
	}
}

// SeekToFirst moves the iterator to the first element
func (it *Iterator) SeekToFirst() error {
	it.tree.mutex.RLock()
	defer it.tree.mutex.RUnlock()

	if it.tree.root == nil {
		it.valid = false
		return nil
	}

	// Find leftmost leaf node
	node := it.tree.root
	for !node.isLeaf {
		if len(node.children) == 0 {
			it.valid = false
			return nil
		}
		node = node.children[0]
	}

	it.currentNode = node
	it.currentPos = 0
	it.valid = len(node.keys) > 0

	return nil
}

// SeekToLast moves the iterator to the last element
func (it *Iterator) SeekToLast() error {
	it.tree.mutex.RLock()
	defer it.tree.mutex.RUnlock()

	if it.tree.root == nil {
		it.valid = false
		return nil
	}

	// Find rightmost leaf node
	node := it.tree.root
	for !node.isLeaf {
		if len(node.children) == 0 {
			it.valid = false
			return nil
		}
		node = node.children[len(node.children)-1]
	}

	it.currentNode = node
	if len(node.keys) > 0 {
		it.currentPos = len(node.keys) - 1
		it.valid = true
	} else {
		it.valid = false
	}

	return nil
}

// Seek moves the iterator to the specified key or the first key greater than it
func (it *Iterator) Seek(key uint64) error {
	it.tree.mutex.RLock()
	defer it.tree.mutex.RUnlock()

	if it.tree.root == nil {
		it.valid = false
		return nil
	}

	// Find the leaf node that should contain the key
	node := it.tree.root
	for !node.isLeaf {
		pos := node.FindKey(key)
		if pos >= len(node.children) {
			pos = len(node.children) - 1
		}
		node = node.children[pos]
	}

	// Find position in leaf
	pos := node.FindKey(key)
	it.currentNode = node
	it.currentPos = pos

	// Check if we found a valid position
	if pos < len(node.keys) {
		it.valid = true
	} else {
		// Move to next available key
		it.valid = it.next()
	}

	return nil
}

// Valid returns true if the iterator is at a valid position
func (it *Iterator) Valid() bool {
	return it.valid
}

// Key returns the current key
func (it *Iterator) Key() (uint64, error) {
	if !it.valid {
		return 0, fmt.Errorf("iterator not valid")
	}

	if it.currentNode == nil || it.currentPos >= len(it.currentNode.keys) {
		return 0, fmt.Errorf("invalid iterator state")
	}

	return it.currentNode.keys[it.currentPos], nil
}

// Value returns the current value
func (it *Iterator) Value() (interface{}, error) {
	if !it.valid {
		return nil, fmt.Errorf("iterator not valid")
	}

	if it.currentNode == nil || it.currentPos >= len(it.currentNode.values) {
		return nil, fmt.Errorf("invalid iterator state")
	}

	return it.currentNode.values[it.currentPos], nil
}

// Next moves the iterator to the next element
func (it *Iterator) Next() error {
	if !it.valid {
		return fmt.Errorf("iterator not valid")
	}

	it.valid = it.next()
	return nil
}

// Previous moves the iterator to the previous element
func (it *Iterator) Previous() error {
	if !it.valid {
		return fmt.Errorf("iterator not valid")
	}

	it.valid = it.previous()
	return nil
}

// next advances the iterator to the next position
func (it *Iterator) next() bool {
	if it.currentNode == nil {
		return false
	}

	it.currentPos++

	// If we're still within the current node
	if it.currentPos < len(it.currentNode.keys) {
		return true
	}

	// Move to next leaf node
	if it.currentNode.next != nil {
		it.currentNode = it.currentNode.next
		it.currentPos = 0
		
		// Trigger prefetching when moving to a new node
		if it.prefetchEnabled {
			it.prefetchNodes()
		}
		
		return len(it.currentNode.keys) > 0
	}

	// No more nodes
	return false
}

// previous moves the iterator to the previous position
func (it *Iterator) previous() bool {
	if it.currentNode == nil {
		return false
	}

	it.currentPos--

	// If we're still within the current node
	if it.currentPos >= 0 {
		return true
	}

	// Need to find previous leaf node
	// This is more complex as we don't have backward links
	// For now, we'll traverse from root to find the previous node
	if it.currentNode == it.findFirstLeaf() {
		// We're at the first node, no previous
		return false
	}

	// Find the previous leaf node by traversing the tree
	// This is inefficient but correct
	prevNode := it.findPreviousLeaf(it.currentNode)
	if prevNode == nil {
		return false
	}

	it.currentNode = prevNode
	if len(prevNode.keys) > 0 {
		it.currentPos = len(prevNode.keys) - 1
		return true
	}

	return false
}

// findFirstLeaf finds the first (leftmost) leaf node
func (it *Iterator) findFirstLeaf() *BTreeNode {
	if it.tree.root == nil {
		return nil
	}

	node := it.tree.root
	for !node.isLeaf {
		if len(node.children) == 0 {
			return nil
		}
		node = node.children[0]
	}

	return node
}

// findPreviousLeaf finds the leaf node that comes before the given node
func (it *Iterator) findPreviousLeaf(target *BTreeNode) *BTreeNode {
	// This is a simplified implementation
	// In a real implementation, you'd want to maintain backward links
	// or use a more efficient traversal method

	first := it.findFirstLeaf()
	if first == nil || first == target {
		return nil
	}

	current := first
	for current.next != nil && current.next != target {
		current = current.next
	}

	if current.next == target {
		return current
	}

	return nil
}

// Range iterates over all key-value pairs in the given range [start, end]
func (it *Iterator) Range(start, end uint64, callback func(key uint64, value interface{}) error) error {
	if err := it.Seek(start); err != nil {
		return err
	}

	for it.Valid() {
		key, err := it.Key()
		if err != nil {
			return err
		}

		if key > end {
			break
		}

		value, err := it.Value()
		if err != nil {
			return err
		}

		if err := callback(key, value); err != nil {
			return err
		}

		if err := it.Next(); err != nil {
			return err
		}
	}

	return nil
}

// ForEach iterates over all elements in the tree
func (it *Iterator) ForEach(callback func(key uint64, value interface{}) error) error {
	if err := it.SeekToFirst(); err != nil {
		return err
	}

	for it.Valid() {
		key, err := it.Key()
		if err != nil {
			return err
		}

		value, err := it.Value()
		if err != nil {
			return err
		}

		if err := callback(key, value); err != nil {
			return err
		}

		if err := it.Next(); err != nil {
			return err
		}
	}

	return nil
}

// Reset resets the iterator to an invalid state
func (it *Iterator) Reset() {
	it.currentNode = nil
	it.currentPos = 0
	it.valid = false
	it.clearPrefetchBuffer()
}

// EnablePrefetch enables prefetching with the specified buffer size
func (it *Iterator) EnablePrefetch(size int) {
	it.prefetchEnabled = true
	it.prefetchSize = size
	it.prefetchBuffer = make([]*BTreeNode, 0, size)
}

// DisablePrefetch disables prefetching
func (it *Iterator) DisablePrefetch() {
	it.prefetchEnabled = false
	it.clearPrefetchBuffer()
}

// clearPrefetchBuffer clears the prefetch buffer
func (it *Iterator) clearPrefetchBuffer() {
	if it.prefetchBuffer != nil {
		it.prefetchBuffer = it.prefetchBuffer[:0]
	}
}

// prefetchNodes prefetches the next few nodes for faster iteration
func (it *Iterator) prefetchNodes() {
	if !it.prefetchEnabled || it.currentNode == nil {
		return
	}

	it.clearPrefetchBuffer()
	
	node := it.currentNode.next
	for i := 0; i < it.prefetchSize && node != nil; i++ {
		// Cache the node in the tree's cache
		if it.tree.leafCache != nil && node.isLeaf {
			// Use the first key as cache key
			if len(node.keys) > 0 {
				it.tree.leafCache.Put(node.keys[0], node)
			}
		}
		
		it.prefetchBuffer = append(it.prefetchBuffer, node)
		node = node.next
	}
}
