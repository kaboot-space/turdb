package cluster

import (
	"fmt"
	"unsafe"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// BTreeNodeType represents the type of B+ tree node
type BTreeNodeType uint8

const (
	LeafNode     BTreeNodeType = 0
	InternalNode BTreeNodeType = 1
)

// BTreeNodeHeader represents the header of a B+ tree node
type BTreeNodeHeader struct {
	NodeType    BTreeNodeType // Type of node (leaf or internal)
	KeyCount    uint16        // Number of keys in this node
	IsRoot      uint8         // Whether this is the root node
	Reserved    uint8         // Reserved for alignment
	Parent      storage.Ref   // Reference to parent node
	NextSibling storage.Ref   // Reference to next sibling (for leaf nodes)
	PrevSibling storage.Ref   // Reference to previous sibling (for leaf nodes)
}

// BTreeNode represents a node in the B+ tree
type BTreeNode struct {
	ref        storage.Ref
	header     *BTreeNodeHeader
	keys       []keys.ObjKey
	values     []storage.Ref // For internal nodes: refs to child nodes, for leaf nodes: refs to clusters
	alloc      *core.Allocator
	fileFormat *storage.FileFormat
}

// BTree represents a B+ tree for managing clusters
type BTree struct {
	root       *BTreeNode
	rootRef    storage.Ref
	height     int
	order      int // Maximum number of keys per node
	alloc      *core.Allocator
	fileFormat *storage.FileFormat
}

// NewBTree creates a new B+ tree
func NewBTree(order int, alloc *core.Allocator, fileFormat *storage.FileFormat) (*BTree, error) {
	if order < 3 {
		return nil, fmt.Errorf("b+ tree order must be at least 3")
	}

	return &BTree{
		root:       nil,
		rootRef:    0,
		height:     0,
		order:      order,
		alloc:      alloc,
		fileFormat: fileFormat,
	}, nil
}

// NewBTreeNode creates a new B+ tree node
func NewBTreeNode(nodeType BTreeNodeType, alloc *core.Allocator, fileFormat *storage.FileFormat) (*BTreeNode, error) {
	// Allocate space for node header
	headerSize := uint32(unsafe.Sizeof(BTreeNodeHeader{}))
	ref, err := fileFormat.AllocateSpace(headerSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate node space: %w", err)
	}

	// Initialize header
	header := &BTreeNodeHeader{
		NodeType:    nodeType,
		KeyCount:    0,
		IsRoot:      0,
		Reserved:    0,
		Parent:      0,
		NextSibling: 0,
		PrevSibling: 0,
	}

	node := &BTreeNode{
		ref:        ref,
		header:     header,
		keys:       make([]keys.ObjKey, 0),
		values:     make([]storage.Ref, 0),
		alloc:      alloc,
		fileFormat: fileFormat,
	}

	// Write header to file
	if err := node.writeHeader(); err != nil {
		return nil, fmt.Errorf("failed to write node header: %w", err)
	}

	return node, nil
}

// writeHeader writes the node header to file
func (n *BTreeNode) writeHeader() error {
	offset := n.fileFormat.RefToOffset(n.ref)
	headerData := (*[24]byte)(unsafe.Pointer(n.header))
	mapper := n.fileFormat.GetMapper()

	// Get pointer to the location and copy data
	ptr := mapper.GetPointer(offset)
	copy((*[24]byte)(ptr)[:], headerData[:])

	return mapper.Sync()
}

// readHeader reads the node header from file
func (n *BTreeNode) readHeader() error {
	offset := n.fileFormat.RefToOffset(n.ref)
	mapper := n.fileFormat.GetMapper()
	headerData := mapper.ReadAt(offset, int(unsafe.Sizeof(BTreeNodeHeader{})))

	if len(headerData) < int(unsafe.Sizeof(BTreeNodeHeader{})) {
		return fmt.Errorf("insufficient data for node header")
	}

	n.header = (*BTreeNodeHeader)(unsafe.Pointer(&headerData[0]))
	return nil
}

// Insert inserts a key-value pair into the B+ tree
func (bt *BTree) Insert(key keys.ObjKey, clusterRef storage.Ref) error {
	if bt.root == nil {
		// Create root node
		root, err := NewBTreeNode(LeafNode, bt.alloc, bt.fileFormat)
		if err != nil {
			return fmt.Errorf("failed to create root node: %w", err)
		}
		root.header.IsRoot = 1
		bt.root = root
		bt.rootRef = root.ref
		bt.height = 1
	}

	return bt.insertIntoNode(bt.root, key, clusterRef)
}

// insertIntoNode inserts a key-value pair into a specific node
func (bt *BTree) insertIntoNode(node *BTreeNode, key keys.ObjKey, value storage.Ref) error {
	if node.header.NodeType == LeafNode {
		// Insert into leaf node
		return bt.insertIntoLeaf(node, key, value)
	} else {
		// Find child node to insert into
		_ = bt.findChild(node, key)
		// In a full implementation, we would load the child node and recurse
		// For now, just return an error
		return fmt.Errorf("internal node insertion not fully implemented")
	}
}

// insertIntoLeaf inserts a key-value pair into a leaf node
func (bt *BTree) insertIntoLeaf(node *BTreeNode, key keys.ObjKey, value storage.Ref) error {
	// Find insertion position
	pos := 0
	for pos < len(node.keys) && node.keys[pos] < key {
		pos++
	}

	// Check if key already exists
	if pos < len(node.keys) && node.keys[pos] == key {
		return fmt.Errorf("key already exists")
	}

	// Insert key and value
	node.keys = append(node.keys[:pos], append([]keys.ObjKey{key}, node.keys[pos:]...)...)
	node.values = append(node.values[:pos], append([]storage.Ref{value}, node.values[pos:]...)...)
	node.header.KeyCount++

	// Check if node needs to be split
	if len(node.keys) > bt.order {
		return bt.splitLeafNode(node)
	}

	return node.writeHeader()
}

// splitLeafNode splits a leaf node when it becomes too full
func (bt *BTree) splitLeafNode(node *BTreeNode) error {
	// Create new node
	newNode, err := NewBTreeNode(LeafNode, bt.alloc, bt.fileFormat)
	if err != nil {
		return fmt.Errorf("failed to create new leaf node: %w", err)
	}

	// Split keys and values
	mid := len(node.keys) / 2
	newNode.keys = append(newNode.keys, node.keys[mid:]...)
	newNode.values = append(newNode.values, node.values[mid:]...)
	newNode.header.KeyCount = uint16(len(newNode.keys))

	// Update original node
	node.keys = node.keys[:mid]
	node.values = node.values[:mid]
	node.header.KeyCount = uint16(len(node.keys))

	// Update sibling pointers
	newNode.header.NextSibling = node.header.NextSibling
	newNode.header.PrevSibling = node.ref
	node.header.NextSibling = newNode.ref

	// Write headers
	if err := node.writeHeader(); err != nil {
		return err
	}
	if err := newNode.writeHeader(); err != nil {
		return err
	}

	// If this is the root, create new root
	if node.header.IsRoot == 1 {
		return bt.createNewRoot(node, newNode)
	}

	// Otherwise, insert the new key into parent
	// In a full implementation, we would handle parent insertion
	return fmt.Errorf("parent insertion not fully implemented")
}

// createNewRoot creates a new root node when the old root splits
func (bt *BTree) createNewRoot(leftChild, rightChild *BTreeNode) error {
	// Create new root
	newRoot, err := NewBTreeNode(InternalNode, bt.alloc, bt.fileFormat)
	if err != nil {
		return fmt.Errorf("failed to create new root: %w", err)
	}

	newRoot.header.IsRoot = 1
	newRoot.keys = append(newRoot.keys, rightChild.keys[0])
	newRoot.values = append(newRoot.values, leftChild.ref, rightChild.ref)
	newRoot.header.KeyCount = 1

	// Update old root
	leftChild.header.IsRoot = 0
	leftChild.header.Parent = newRoot.ref
	rightChild.header.Parent = newRoot.ref

	// Update tree
	bt.root = newRoot
	bt.rootRef = newRoot.ref
	bt.height++

	// Write headers
	if err := newRoot.writeHeader(); err != nil {
		return err
	}
	if err := leftChild.writeHeader(); err != nil {
		return err
	}
	return rightChild.writeHeader()
}

// Search searches for a key in the B+ tree
func (bt *BTree) Search(key keys.ObjKey) (storage.Ref, error) {
	if bt.root == nil {
		return 0, fmt.Errorf("key not found")
	}

	return bt.searchInNode(bt.root, key)
}

// searchInNode searches for a key in a specific node
func (bt *BTree) searchInNode(node *BTreeNode, key keys.ObjKey) (storage.Ref, error) {
	if node.header.NodeType == LeafNode {
		// Search in leaf node
		for i, k := range node.keys {
			if k == key {
				return node.values[i], nil
			}
		}
		return 0, fmt.Errorf("key not found")
	} else {
		// Find child to search in
		_ = bt.findChild(node, key)
		// In a full implementation, we would load the child node and recurse
		return 0, fmt.Errorf("internal node search not fully implemented")
	}
}

// findChild finds the appropriate child node for a given key
func (bt *BTree) findChild(node *BTreeNode, key keys.ObjKey) storage.Ref {
	for i, k := range node.keys {
		if key < k {
			return node.values[i]
		}
	}
	// Key is greater than all keys, return rightmost child
	return node.values[len(node.values)-1]
}
