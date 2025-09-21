package btree

import (
	"fmt"
	"unsafe"

	"github.com/turdb/tur/pkg/storage"
)

// NodeType represents the type of B+ tree node
type NodeType uint8

const (
	LeafNode NodeType = iota
	InternalNode
)

// BTreeNode represents a node in the B+ tree
type BTreeNode struct {
	nodeType NodeType
	keys     []uint64
	isLeaf   bool
	parent   *BTreeNode
	ref      storage.Ref // Reference to node in file

	// For leaf nodes
	values []interface{}
	next   *BTreeNode // Pointer to next leaf node

	// For internal nodes
	children []*BTreeNode

	// Tree reference
	tree *BTree
}

// NodeHeader represents the on-disk header for a B+ tree node
type NodeHeader struct {
	Magic    [4]byte // Node magic number
	NodeType uint8   // 0 = leaf, 1 = internal
	KeyCount uint16  // Number of keys in node
	Reserved uint8   // Padding
	Next     uint64  // Reference to next node (for leaf nodes)
	Parent   uint64  // Reference to parent node
}

const (
	NodeHeaderSize = unsafe.Sizeof(NodeHeader{})
	MaxKeys        = 255 // Maximum keys per node
)

var (
	LeafMagic     = [4]byte{0x4C, 0x45, 0x41, 0x46} // "LEAF"
	InternalMagic = [4]byte{0x49, 0x4E, 0x54, 0x4C} // "INTL"
)

// NewLeafNode creates a new leaf node
func NewLeafNode(tree *BTree) *BTreeNode {
	return &BTreeNode{
		nodeType: LeafNode,
		isLeaf:   true,
		keys:     make([]uint64, 0, MaxKeys),
		values:   make([]interface{}, 0, MaxKeys),
		tree:     tree,
	}
}

// NewInternalNode creates a new internal node
func NewInternalNode(tree *BTree) *BTreeNode {
	return &BTreeNode{
		nodeType: InternalNode,
		isLeaf:   false,
		keys:     make([]uint64, 0, MaxKeys),
		children: make([]*BTreeNode, 0, MaxKeys+1),
		tree:     tree,
	}
}

// NewLeafNodeWithConfig creates a new leaf node with configuration
func NewLeafNodeWithConfig(tree *BTree, config *TreeConfig) *BTreeNode {
	maxKeys := MaxKeys
	if config != nil && config.MaxKeys > 0 {
		maxKeys = config.MaxKeys
	}

	return &BTreeNode{
		nodeType: LeafNode,
		isLeaf:   true,
		keys:     make([]uint64, 0, maxKeys),
		values:   make([]interface{}, 0, maxKeys),
		tree:     tree,
	}
}

// NewInternalNodeWithConfig creates a new internal node with configuration
func NewInternalNodeWithConfig(tree *BTree, config *TreeConfig) *BTreeNode {
	maxKeys := MaxKeys
	if config != nil && config.MaxKeys > 0 {
		maxKeys = config.MaxKeys
	}

	return &BTreeNode{
		nodeType: InternalNode,
		isLeaf:   false,
		keys:     make([]uint64, 0, maxKeys),
		children: make([]*BTreeNode, 0, maxKeys+1),
		tree:     tree,
	}
}

// IsLeaf returns true if this is a leaf node
func (n *BTreeNode) IsLeaf() bool {
	return n.isLeaf
}

// KeyCount returns the number of keys in the node
func (n *BTreeNode) KeyCount() int {
	return len(n.keys)
}

// IsFull returns true if the node is full
func (n *BTreeNode) IsFull() bool {
	return len(n.keys) >= MaxKeys
}

// IsUnderflow returns true if the node has too few keys
func (n *BTreeNode) IsUnderflow() bool {
	minKeys := MaxKeys / 2
	if n.tree != nil && n == n.tree.root {
		minKeys = 1 // Root can have fewer keys
	}
	return len(n.keys) < minKeys
}

// FindKey finds the position where key should be inserted
func (n *BTreeNode) FindKey(key uint64) int {
	left, right := 0, len(n.keys)
	for left < right {
		mid := (left + right) / 2
		if n.keys[mid] < key {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// Search searches for a key in the node
func (n *BTreeNode) Search(key uint64) (interface{}, bool) {
	pos := n.FindKey(key)

	if n.isLeaf {
		if pos < len(n.keys) && n.keys[pos] == key {
			return n.values[pos], true
		}
		return nil, false
	}

	// Internal node - descend to child
	if pos < len(n.children) {
		return n.children[pos].Search(key)
	}
	return nil, false
}

// Insert inserts a key-value pair into the node
func (n *BTreeNode) Insert(key uint64, value interface{}) error {
	if n.isLeaf {
		return n.insertLeaf(key, value)
	}
	return n.insertInternal(key, value)
}

// insertLeaf inserts into a leaf node
func (n *BTreeNode) insertLeaf(key uint64, value interface{}) error {
	pos := n.FindKey(key)

	// Check if key already exists
	if pos < len(n.keys) && n.keys[pos] == key {
		n.values[pos] = value // Update existing
		return nil
	}

	// Insert new key-value pair
	n.keys = append(n.keys, 0)
	n.values = append(n.values, nil)

	// Shift elements to make room
	copy(n.keys[pos+1:], n.keys[pos:])
	copy(n.values[pos+1:], n.values[pos:])

	n.keys[pos] = key
	n.values[pos] = value

	return nil
}

// insertInternal inserts into an internal node
func (n *BTreeNode) insertInternal(key uint64, value interface{}) error {
	pos := n.FindKey(key)

	if pos < len(n.children) {
		return n.children[pos].Insert(key, value)
	}

	return fmt.Errorf("invalid child position")
}

// Split splits a full node into two nodes (implements BPlusTreeNode interface)
func (n *BTreeNode) Split() (BPlusTreeNode, uint64, error) {
	if !n.IsFull() {
		return nil, 0, fmt.Errorf("node is not full")
	}

	midIndex := len(n.keys) / 2
	midKey := n.keys[midIndex]

	var newNode *BTreeNode
	if n.isLeaf {
		newNode = NewLeafNode(n.tree)
		// Copy second half of keys and values
		newNode.keys = append(newNode.keys, n.keys[midIndex:]...)
		newNode.values = append(newNode.values, n.values[midIndex:]...)

		// Link leaf nodes
		newNode.next = n.next
		n.next = newNode

		// Truncate original node
		n.keys = n.keys[:midIndex]
		n.values = n.values[:midIndex]
	} else {
		newNode = NewInternalNode(n.tree)
		// Copy second half of keys and children
		newNode.keys = append(newNode.keys, n.keys[midIndex+1:]...)
		newNode.children = append(newNode.children, n.children[midIndex+1:]...)

		// Update parent pointers
		for _, child := range newNode.children {
			child.parent = newNode
		}

		// Truncate original node
		n.keys = n.keys[:midIndex]
		n.children = n.children[:midIndex+1]
	}

	newNode.parent = n.parent
	return newNode, midKey, nil
}

// Serialize serializes the node to bytes for storage
func (n *BTreeNode) Serialize() ([]byte, error) {
	header := NodeHeader{
		KeyCount: uint16(len(n.keys)),
	}

	if n.isLeaf {
		header.Magic = LeafMagic
		header.NodeType = 0
		if n.next != nil {
			header.Next = uint64(n.next.ref)
		}
	} else {
		header.Magic = InternalMagic
		header.NodeType = 1
	}

	if n.parent != nil {
		header.Parent = uint64(n.parent.ref)
	}

	// Calculate total size
	totalSize := NodeHeaderSize + uintptr(len(n.keys)*8) // 8 bytes per key

	if n.isLeaf {
		// Add space for values (simplified - just storing as uint64)
		totalSize += uintptr(len(n.values) * 8)
	} else {
		// Add space for child references
		totalSize += uintptr(len(n.children) * 8)
	}

	data := make([]byte, totalSize)

	// Copy header
	headerBytes := (*[NodeHeaderSize]byte)(unsafe.Pointer(&header))[:]
	copy(data[:NodeHeaderSize], headerBytes)

	offset := NodeHeaderSize

	// Copy keys
	for _, key := range n.keys {
		keyBytes := (*[8]byte)(unsafe.Pointer(&key))[:]
		copy(data[offset:offset+8], keyBytes)
		offset += 8
	}

	// Copy values or child references
	if n.isLeaf {
		for _, value := range n.values {
			// Simplified: store as uint64
			var val uint64
			if value != nil {
				if v, ok := value.(uint64); ok {
					val = v
				}
			}
			valBytes := (*[8]byte)(unsafe.Pointer(&val))[:]
			copy(data[offset:offset+8], valBytes)
			offset += 8
		}
	} else {
		for _, child := range n.children {
			ref := uint64(0)
			if child != nil {
				ref = uint64(child.ref)
			}
			refBytes := (*[8]byte)(unsafe.Pointer(&ref))[:]
			copy(data[offset:offset+8], refBytes)
			offset += 8
		}
	}

	return data, nil
}

// Deserialize deserializes bytes into the node
func (n *BTreeNode) Deserialize(data []byte) error {
	if len(data) < int(NodeHeaderSize) {
		return fmt.Errorf("data too small for node header")
	}

	header := (*NodeHeader)(unsafe.Pointer(&data[0]))

	// Validate magic
	if header.Magic != LeafMagic && header.Magic != InternalMagic {
		return fmt.Errorf("invalid node magic")
	}

	n.isLeaf = header.NodeType == 0
	if n.isLeaf {
		n.nodeType = LeafNode
	} else {
		n.nodeType = InternalNode
	}

	keyCount := int(header.KeyCount)
	n.keys = make([]uint64, keyCount)

	offset := NodeHeaderSize

	// Read keys
	for i := 0; i < keyCount; i++ {
		if offset+8 > uintptr(len(data)) {
			return fmt.Errorf("insufficient data for keys")
		}
		n.keys[i] = *(*uint64)(unsafe.Pointer(&data[offset]))
		offset += 8
	}

	// Read values or child references
	if n.isLeaf {
		n.values = make([]interface{}, keyCount)
		for i := 0; i < keyCount; i++ {
			if offset+8 > uintptr(len(data)) {
				return fmt.Errorf("insufficient data for values")
			}
			val := *(*uint64)(unsafe.Pointer(&data[offset]))
			n.values[i] = val
			offset += 8
		}
	} else {
		childCount := keyCount + 1
		n.children = make([]*BTreeNode, childCount)
		// Child references will be resolved later
		for i := 0; i < childCount; i++ {
			if offset+8 > uintptr(len(data)) {
				return fmt.Errorf("insufficient data for children")
			}
			// Store reference for later resolution
			offset += 8
		}
	}

	return nil
}

// GetRef returns the node's file reference
func (n *BTreeNode) GetRef() storage.Ref {
	return n.ref
}

// SetRef sets the node's file reference
func (n *BTreeNode) SetRef(ref storage.Ref) {
	n.ref = ref
}

// GetKeys returns the keys in the node (implements BPlusTreeNode interface)
func (n *BTreeNode) GetKeys() []uint64 {
	return n.keys
}

// GetValues returns the values in the node (implements BPlusTreeNode interface)
func (n *BTreeNode) GetValues() []interface{} {
	return n.values
}

// GetChildren returns the children as BPlusTreeNode interface (implements BPlusTreeNode interface)
func (n *BTreeNode) GetChildren() []BPlusTreeNode {
	children := make([]BPlusTreeNode, len(n.children))
	for i, child := range n.children {
		children[i] = child
	}
	return children
}

// Merge merges another node into this node (implements BPlusTreeNode interface)
func (n *BTreeNode) Merge(other BPlusTreeNode) error {
	otherNode, ok := other.(*BTreeNode)
	if !ok {
		return fmt.Errorf("cannot merge incompatible node types")
	}

	if n.isLeaf != otherNode.isLeaf {
		return fmt.Errorf("cannot merge leaf and internal nodes")
	}

	// Merge keys
	n.keys = append(n.keys, otherNode.keys...)

	if n.isLeaf {
		// Merge values for leaf nodes
		n.values = append(n.values, otherNode.values...)
		// Update next pointer
		n.next = otherNode.next
	} else {
		// Merge children for internal nodes
		n.children = append(n.children, otherNode.children...)
		// Update parent pointers
		for _, child := range otherNode.children {
			child.parent = n
		}
	}

	return nil
}
