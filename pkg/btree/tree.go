package btree

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/turdb/tur/pkg/storage"
)

// BPlusTreeBase represents the abstract base for B+ tree implementations
type BPlusTreeBase interface {
	Search(key uint64) (interface{}, bool)
	Insert(key uint64, value interface{}) error
	Delete(key uint64) error
	Traverse(fn TraverseFunc) error
	Verify() error
	Size() int64
	Height() int
}

// BPlusTreeNode represents the abstract base for B+ tree nodes
type BPlusTreeNode interface {
	IsLeaf() bool
	IsFull() bool
	IsUnderflow() bool
	GetKeys() []uint64
	GetValues() []interface{}
	GetChildren() []BPlusTreeNode
	Split() (BPlusTreeNode, uint64, error)
	Merge(other BPlusTreeNode) error
	FindKey(key uint64) int
}

// InsertFunc represents a function for custom insert operations
type InsertFunc func(key uint64, value interface{}) error

// AccessFunc represents a function for custom access operations
type AccessFunc func(key uint64) (interface{}, bool)

// EraseFunc represents a function for custom erase operations
type EraseFunc func(key uint64) error

// TraverseFunc represents a function for tree traversal
type TraverseFunc func(key uint64, value interface{}) bool

// VerifyFunc represents a function for tree verification
type VerifyFunc func(node BPlusTreeNode) error

// TreeConfig represents configuration for different tree types
type TreeConfig struct {
	MaxKeys      int
	MinKeys      int
	AllowDups    bool
	CustomInsert InsertFunc
	CustomAccess AccessFunc
	CustomErase  EraseFunc
	CustomVerify VerifyFunc
}

// DefaultTreeConfig returns a default tree configuration
func DefaultTreeConfig() *TreeConfig {
	return &TreeConfig{
		MaxKeys:   255,
		MinKeys:   127,
		AllowDups: false,
	}
}

// BTree represents a B+ tree with generic framework support
type BTree struct {
	root   *BTreeNode
	height int
	size   int64
	mutex  sync.RWMutex
	config *TreeConfig

	// File storage
	fileFormat *storage.FileFormat
	
	// Caching system
	leafCache *LeafCache
	
	// Advanced traversal
	traversal *AdvancedTraversal
}

// NewBTree creates a new B+ tree with default configuration
func NewBTree(fileFormat *storage.FileFormat) *BTree {
	return NewBTreeWithConfig(fileFormat, DefaultTreeConfig())
}

// NewBTreeWithConfig creates a new B+ tree with custom configuration
func NewBTreeWithConfig(fileFormat *storage.FileFormat, config *TreeConfig) *BTree {
	tree := &BTree{
		height:     1,
		size:       0,
		fileFormat: fileFormat,
		config:     config,
		leafCache:  NewLeafCache(100), // Default cache size
	}

	// Create initial root node
	tree.root = NewLeafNodeWithConfig(tree, config)
	tree.traversal = NewAdvancedTraversal(tree)
	return tree
}

// Search searches for a key in the tree using custom access function if available
func (bt *BTree) Search(key uint64) (interface{}, bool) {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	// Use custom access function if available
	if bt.config.CustomAccess != nil {
		return bt.config.CustomAccess(key)
	}

	if bt.root == nil {
		return nil, false
	}

	return bt.root.Search(key)
}

// Insert inserts a key-value pair using custom insert function if available
func (bt *BTree) Insert(key uint64, value interface{}) error {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// Use custom insert function if available
	if bt.config.CustomInsert != nil {
		if err := bt.config.CustomInsert(key, value); err != nil {
			return err
		}
		bt.size++
		return nil
	}

	if bt.root == nil {
		bt.root = NewLeafNodeWithConfig(bt, bt.config)
	}

	// Insert into root
	if err := bt.root.Insert(key, value); err != nil {
		return err
	}

	// Check if root needs to be split
	if bt.root.IsFull() {
		if err := bt.splitRoot(); err != nil {
			return err
		}
	}

	bt.size++
	return nil
}

// Traverse traverses the tree in order, calling the provided function for each key-value pair
func (bt *BTree) Traverse(fn TraverseFunc) error {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if bt.root == nil {
		return nil
	}

	return bt.traverseNode(bt.root, fn)
}

// traverseNode recursively traverses nodes
func (bt *BTree) traverseNode(node *BTreeNode, fn TraverseFunc) error {
	if node.isLeaf {
		// Traverse leaf node
		for i, key := range node.keys {
			if !fn(key, node.values[i]) {
				return nil // Stop traversal
			}
		}
	} else {
		// Traverse internal node
		for i := 0; i < len(node.children); i++ {
			if err := bt.traverseNode(node.children[i], fn); err != nil {
				return err
			}

			// Process key between children (except for last child)
			if i < len(node.keys) {
				if !fn(node.keys[i], nil) {
					return nil // Stop traversal
				}
			}
		}
	}

	return nil
}

// Verify verifies the integrity of the B+ tree
func (bt *BTree) Verify() error {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if bt.root == nil {
		return nil
	}

	// Use custom verify function if available
	if bt.config.CustomVerify != nil {
		return bt.config.CustomVerify(bt.root)
	}

	return bt.verifyNode(bt.root, nil, nil, 0)
}

// verifyNode recursively verifies node integrity
func (bt *BTree) verifyNode(node *BTreeNode, minKey, maxKey *uint64, depth int) error {
	// Verify key count constraints
	if node != bt.root {
		if len(node.keys) < bt.config.MinKeys {
			return fmt.Errorf("node has too few keys: %d < %d", len(node.keys), bt.config.MinKeys)
		}
	}

	if len(node.keys) > bt.config.MaxKeys {
		return fmt.Errorf("node has too many keys: %d > %d", len(node.keys), bt.config.MaxKeys)
	}

	// Verify key ordering
	for i := 1; i < len(node.keys); i++ {
		if node.keys[i-1] >= node.keys[i] {
			return fmt.Errorf("keys not in order: %d >= %d", node.keys[i-1], node.keys[i])
		}
	}

	// Verify key bounds
	if minKey != nil && len(node.keys) > 0 && node.keys[0] < *minKey {
		return fmt.Errorf("key %d violates minimum bound %d", node.keys[0], *minKey)
	}

	if maxKey != nil && len(node.keys) > 0 && node.keys[len(node.keys)-1] > *maxKey {
		return fmt.Errorf("key %d violates maximum bound %d", node.keys[len(node.keys)-1], *maxKey)
	}

	if node.isLeaf {
		// Verify leaf node
		if len(node.keys) != len(node.values) {
			return fmt.Errorf("leaf node key/value count mismatch: %d keys, %d values",
				len(node.keys), len(node.values))
		}
	} else {
		// Verify internal node
		if len(node.children) != len(node.keys)+1 {
			return fmt.Errorf("internal node children/key count mismatch: %d children, %d keys",
				len(node.children), len(node.keys))
		}

		// Recursively verify children
		for i, child := range node.children {
			var childMinKey, childMaxKey *uint64

			if i > 0 {
				childMinKey = &node.keys[i-1]
			} else {
				childMinKey = minKey
			}

			if i < len(node.keys) {
				childMaxKey = &node.keys[i]
			} else {
				childMaxKey = maxKey
			}

			if err := bt.verifyNode(child, childMinKey, childMaxKey, depth+1); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetConfig returns the tree configuration
func (bt *BTree) GetConfig() *TreeConfig {
	return bt.config
}

// SetConfig updates the tree configuration
func (bt *BTree) SetConfig(config *TreeConfig) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	bt.config = config
}

// splitRoot splits the root node
func (bt *BTree) splitRoot() error {
	oldRoot := bt.root
	newRoot := NewInternalNode(bt)

	// Split the old root
	newNode, midKey, err := oldRoot.Split()
	if err != nil {
		return err
	}

	// Set up new root
	newRoot.keys = append(newRoot.keys, midKey)

	// Type assert the interface back to *BTreeNode
	newNodeConcrete, ok := newNode.(*BTreeNode)
	if !ok {
		return fmt.Errorf("failed to cast split node")
	}

	newRoot.children = append(newRoot.children, oldRoot, newNodeConcrete)

	// Update parent pointers
	oldRoot.parent = newRoot
	newNodeConcrete.parent = newRoot

	bt.root = newRoot
	bt.height++

	return nil
}

// Delete deletes a key from the tree
func (bt *BTree) Delete(key uint64) error {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	// Use custom erase function if available
	if bt.config.CustomErase != nil {
		if err := bt.config.CustomErase(key); err != nil {
			return err
		}
		bt.size--
		return nil
	}

	if bt.root == nil {
		return fmt.Errorf("key not found")
	}

	if err := bt.deleteFromNode(bt.root, key); err != nil {
		return err
	}

	// If root becomes empty and has children, promote child
	if len(bt.root.keys) == 0 && !bt.root.isLeaf {
		if len(bt.root.children) > 0 {
			bt.root = bt.root.children[0]
			bt.root.parent = nil
			bt.height--
		}
	}

	bt.size--
	return nil
}

// deleteFromNode deletes a key from a specific node
func (bt *BTree) deleteFromNode(node *BTreeNode, key uint64) error {
	pos := node.FindKey(key)

	if node.isLeaf {
		// Leaf node
		if pos < len(node.keys) && node.keys[pos] == key {
			// Remove key and value
			copy(node.keys[pos:], node.keys[pos+1:])
			copy(node.values[pos:], node.values[pos+1:])
			node.keys = node.keys[:len(node.keys)-1]
			node.values = node.values[:len(node.values)-1]

			// Handle underflow
			if node.IsUnderflow() && node != bt.root {
				return bt.handleUnderflow(node)
			}
			return nil
		}
		return fmt.Errorf("key not found")
	}

	// Internal node
	if pos < len(node.children) {
		return bt.deleteFromNode(node.children[pos], key)
	}

	return fmt.Errorf("key not found")
}

// handleUnderflow handles node underflow by borrowing or merging
func (bt *BTree) handleUnderflow(node *BTreeNode) error {
	if node.parent == nil {
		return nil // Root node
	}

	parentPos := -1
	for i, child := range node.parent.children {
		if child == node {
			parentPos = i
			break
		}
	}

	if parentPos == -1 {
		return fmt.Errorf("node not found in parent")
	}

	// Try to borrow from left sibling
	if parentPos > 0 {
		leftSibling := node.parent.children[parentPos-1]
		if len(leftSibling.keys) > MaxKeys/2 {
			return bt.borrowFromLeft(node, leftSibling, parentPos-1)
		}
	}

	// Try to borrow from right sibling
	if parentPos < len(node.parent.children)-1 {
		rightSibling := node.parent.children[parentPos+1]
		if len(rightSibling.keys) > MaxKeys/2 {
			return bt.borrowFromRight(node, rightSibling, parentPos)
		}
	}

	// Merge with sibling
	if parentPos > 0 {
		leftSibling := node.parent.children[parentPos-1]
		return bt.mergeWithLeft(node, leftSibling, parentPos-1)
	} else {
		rightSibling := node.parent.children[parentPos+1]
		return bt.mergeWithRight(node, rightSibling, parentPos)
	}
}

// borrowFromLeft borrows a key from left sibling
func (bt *BTree) borrowFromLeft(node, leftSibling *BTreeNode, parentKeyPos int) error {
	parent := node.parent

	if node.isLeaf {
		// Move last key from left sibling to first position of node
		borrowedKey := leftSibling.keys[len(leftSibling.keys)-1]
		borrowedValue := leftSibling.values[len(leftSibling.values)-1]

		// Remove from left sibling
		leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
		leftSibling.values = leftSibling.values[:len(leftSibling.values)-1]

		// Add to node
		node.keys = append([]uint64{borrowedKey}, node.keys...)
		node.values = append([]interface{}{borrowedValue}, node.values...)

		// Update parent key
		parent.keys[parentKeyPos] = borrowedKey
	} else {
		// Internal node borrowing
		// More complex - involves moving parent key down and child up
		// Simplified implementation
		return fmt.Errorf("internal node borrowing not implemented")
	}

	return nil
}

// borrowFromRight borrows a key from right sibling
func (bt *BTree) borrowFromRight(node, rightSibling *BTreeNode, parentKeyPos int) error {
	parent := node.parent

	if node.isLeaf {
		// Move first key from right sibling to last position of node
		borrowedKey := rightSibling.keys[0]
		borrowedValue := rightSibling.values[0]

		// Remove from right sibling
		copy(rightSibling.keys, rightSibling.keys[1:])
		copy(rightSibling.values, rightSibling.values[1:])
		rightSibling.keys = rightSibling.keys[:len(rightSibling.keys)-1]
		rightSibling.values = rightSibling.values[:len(rightSibling.values)-1]

		// Add to node
		node.keys = append(node.keys, borrowedKey)
		node.values = append(node.values, borrowedValue)

		// Update parent key
		parent.keys[parentKeyPos] = rightSibling.keys[0]
	} else {
		// Internal node borrowing
		// Simplified implementation
		return fmt.Errorf("internal node borrowing not implemented")
	}

	return nil
}

// mergeWithLeft merges node with left sibling
func (bt *BTree) mergeWithLeft(node, leftSibling *BTreeNode, parentKeyPos int) error {
	parent := node.parent

	if node.isLeaf {
		// Merge all keys and values from node into left sibling
		leftSibling.keys = append(leftSibling.keys, node.keys...)
		leftSibling.values = append(leftSibling.values, node.values...)
		leftSibling.next = node.next
	} else {
		// Internal node merging
		// Add parent key and all keys from node
		parentKey := parent.keys[parentKeyPos]
		leftSibling.keys = append(leftSibling.keys, parentKey)
		leftSibling.keys = append(leftSibling.keys, node.keys...)
		leftSibling.children = append(leftSibling.children, node.children...)

		// Update parent pointers
		for _, child := range node.children {
			child.parent = leftSibling
		}
	}

	// Remove key from parent
	copy(parent.keys[parentKeyPos:], parent.keys[parentKeyPos+1:])
	parent.keys = parent.keys[:len(parent.keys)-1]

	// Remove child reference from parent
	copy(parent.children[parentKeyPos+1:], parent.children[parentKeyPos+2:])
	parent.children = parent.children[:len(parent.children)-1]

	// Handle parent underflow
	if parent.IsUnderflow() && parent != bt.root {
		return bt.handleUnderflow(parent)
	}

	return nil
}

// mergeWithRight merges node with right sibling
func (bt *BTree) mergeWithRight(node, rightSibling *BTreeNode, parentKeyPos int) error {
	parent := node.parent

	if node.isLeaf {
		// Merge all keys and values from right sibling into node
		node.keys = append(node.keys, rightSibling.keys...)
		node.values = append(node.values, rightSibling.values...)
		node.next = rightSibling.next
	} else {
		// Internal node merging
		parentKey := parent.keys[parentKeyPos]
		node.keys = append(node.keys, parentKey)
		node.keys = append(node.keys, rightSibling.keys...)
		node.children = append(node.children, rightSibling.children...)

		// Update parent pointers
		for _, child := range rightSibling.children {
			child.parent = node
		}
	}

	// Remove key from parent
	copy(parent.keys[parentKeyPos:], parent.keys[parentKeyPos+1:])
	parent.keys = parent.keys[:len(parent.keys)-1]

	// Remove child reference from parent
	copy(parent.children[parentKeyPos+1:], parent.children[parentKeyPos+2:])
	parent.children = parent.children[:len(parent.children)-1]

	// Handle parent underflow
	if parent.IsUnderflow() && parent != bt.root {
		return bt.handleUnderflow(parent)
	}

	return nil
}

// Size returns the number of elements in the tree
func (bt *BTree) Size() int64 {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return bt.size
}

// Height returns the height of the tree
func (bt *BTree) Height() int {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return bt.height
}

// Root returns the root node
func (bt *BTree) Root() *BTreeNode {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return bt.root
}

// Iterator creates a new iterator for the tree
func (bt *BTree) Iterator() *Iterator {
	return NewIterator(bt)
}

// SaveToDisk saves the tree to disk
func (bt *BTree) SaveToDisk() error {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()

	if bt.fileFormat == nil {
		return fmt.Errorf("no file format available")
	}

	return bt.saveNode(bt.root)
}

// saveNode recursively saves a node and its children to disk
func (bt *BTree) saveNode(node *BTreeNode) error {
	if node == nil {
		return nil
	}

	// Serialize node
	data, err := node.Serialize()
	if err != nil {
		return err
	}

	// Allocate space in file
	ref, err := bt.fileFormat.AllocateSpace(uint32(len(data)))
	if err != nil {
		return err
	}

	// Write data to file
	mapper := bt.fileFormat.GetMapper()
	offset := bt.fileFormat.RefToOffset(ref)
	copy(mapper.GetData()[offset:], data)

	// Set node reference
	node.SetRef(ref)

	// Save children for internal nodes
	if !node.isLeaf {
		for _, child := range node.children {
			if err := bt.saveNode(child); err != nil {
				return err
			}
		}
	}

	return nil
}

// LoadFromDisk loads the tree from disk
func (bt *BTree) LoadFromDisk(rootRef storage.Ref) error {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	if bt.fileFormat == nil {
		return fmt.Errorf("no file format available")
	}

	root, err := bt.loadNode(rootRef)
	if err != nil {
		return err
	}

	bt.root = root
	return nil
}

// loadNode loads a node from disk
func (bt *BTree) loadNode(ref storage.Ref) (*BTreeNode, error) {
	mapper := bt.fileFormat.GetMapper()
	offset := bt.fileFormat.RefToOffset(ref)

	// Read node header first
	headerData := mapper.ReadAt(offset, int(NodeHeaderSize))
	if headerData == nil {
		return nil, fmt.Errorf("failed to read node header")
	}

	header := (*NodeHeader)(unsafe.Pointer(&headerData[0]))

	// Calculate total node size
	totalSize := NodeHeaderSize + uintptr(header.KeyCount)*8 // Keys
	if header.NodeType == 0 {
		// Leaf node - add space for values
		totalSize += uintptr(header.KeyCount) * 8
	} else {
		// Internal node - add space for children
		totalSize += uintptr(header.KeyCount+1) * 8
	}

	// Read full node data
	nodeData := mapper.ReadAt(offset, int(totalSize))
	if nodeData == nil {
		return nil, fmt.Errorf("failed to read node data")
	}

	// Create and deserialize node
	var node *BTreeNode
	if header.NodeType == 0 {
		node = NewLeafNode(bt)
	} else {
		node = NewInternalNode(bt)
	}

	if err := node.Deserialize(nodeData); err != nil {
		return nil, err
	}

	node.SetRef(ref)
	return node, nil
}

// GetLeafCache returns the leaf cache
func (bt *BTree) GetLeafCache() *LeafCache {
	return bt.leafCache
}

// GetTraversal returns the advanced traversal system
func (bt *BTree) GetTraversal() *AdvancedTraversal {
	return bt.traversal
}

// CacheLeafNode caches a leaf node
func (bt *BTree) CacheLeafNode(key uint64, node *BTreeNode) {
	if bt.leafCache != nil && node.isLeaf {
		bt.leafCache.Put(key, node)
	}
}

// GetCachedLeafNode retrieves a cached leaf node
func (bt *BTree) GetCachedLeafNode(key uint64) (*BTreeNode, bool) {
	if bt.leafCache != nil {
		return bt.leafCache.Get(key)
	}
	return nil, false
}

// ClearCache clears the leaf cache
func (bt *BTree) ClearCache() {
	if bt.leafCache != nil {
		bt.leafCache.Clear()
	}
}
