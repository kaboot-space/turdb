package query

import (
	"github.com/turdb/tur/pkg/cluster"
	"github.com/turdb/tur/pkg/keys"
)

// Iterator provides lazy iteration over query results
type Iterator struct {
	tree         *cluster.ClusterTree
	position     int64
	key          keys.ObjKey
	leafInvalid  bool
	leafStartPos int64
	version      uint64
}

// NewIterator creates a new iterator at the specified position
func NewIterator(tree *cluster.ClusterTree, position int64) *Iterator {
	it := &Iterator{
		tree:     tree,
		position: position,
		version:  0, // TODO: implement version tracking
	}

	root, err := tree.GetRootCluster()
	if err != nil || root == nil {
		it.leafInvalid = true
		return it
	}

	size := int64(root.ObjectCount())
	if position >= size {
		it.position = size
		it.leafInvalid = true
	} else if position == 0 && size > 0 {
		it.key, _ = root.GetObjectKey(0)
		it.leafStartPos = 0
	} else if position < size {
		it.key, _ = root.GetObjectKey(int(position))
		it.leafStartPos = position
	} else {
		it.leafInvalid = true
	}

	return it
}

// HasNext returns true if there are more elements
func (it *Iterator) HasNext() bool {
	root, err := it.tree.GetRootCluster()
	if err != nil || root == nil {
		return false
	}
	return it.position < int64(root.ObjectCount()) && !it.leafInvalid
}

// Next advances the iterator and returns the next key
func (it *Iterator) Next() (keys.ObjKey, bool) {
	if !it.HasNext() {
		return keys.ObjKey(0), false
	}

	currentKey := it.key
	it.position++

	root, err := it.tree.GetRootCluster()
	if err != nil || root == nil {
		it.leafInvalid = true
		return currentKey, true
	}

	if it.position < int64(root.ObjectCount()) {
		it.key, _ = root.GetObjectKey(int(it.position))
	} else {
		it.leafInvalid = true
	}

	return currentKey, true
}

// Position returns the current position
func (it *Iterator) Position() int64 {
	return it.position
}

// Reset resets the iterator to the beginning
func (it *Iterator) Reset() {
	it.position = 0
	it.leafInvalid = false
	it.leafStartPos = 0
	
	root, err := it.tree.GetRootCluster()
	if err != nil || root == nil || root.ObjectCount() == 0 {
		it.leafInvalid = true
		return
	}
	
	it.key, _ = root.GetObjectKey(0)
}

// Skip advances the iterator by n positions
func (it *Iterator) Skip(n int64) bool {
	newPos := it.position + n
	
	root, err := it.tree.GetRootCluster()
	if err != nil || root == nil {
		it.leafInvalid = true
		return false
	}
	
	if newPos >= int64(root.ObjectCount()) {
		it.position = int64(root.ObjectCount())
		it.leafInvalid = true
		return false
	}

	it.position = newPos
	it.key, _ = root.GetObjectKey(int(it.position))
	return true
}

func (it *Iterator) loadLeaf(key keys.ObjKey) keys.ObjKey {
	// Load the leaf containing the specified key
	root, err := it.tree.GetRootCluster()
	if err != nil || root == nil {
		return keys.ObjKey(0)
	}
	
	objKey, _ := root.GetObjectKey(int(it.position))
	return objKey
}

// LazyResults provides lazy evaluation of query results
type LazyResults struct {
	tree     *cluster.ClusterTree
	iterator *Iterator
	size     int64
}

// NewLazyResults creates a new lazy results container
func NewLazyResults(tree *cluster.ClusterTree) *LazyResults {
	root, err := tree.GetRootCluster()
	size := int64(0)
	if err == nil && root != nil {
		size = int64(root.ObjectCount())
	}
	
	return &LazyResults{
		tree:     tree,
		iterator: NewIterator(tree, 0),
		size:     size,
	}
}

// Size returns the total number of results
func (lr *LazyResults) Size() int64 {
	return lr.size
}

// Iterator returns a new iterator for the results
func (lr *LazyResults) Iterator() *Iterator {
	return NewIterator(lr.tree, 0)
}

// Get returns the element at the specified index without loading all results
func (lr *LazyResults) Get(index int64) (keys.ObjKey, bool) {
	if index < 0 || index >= lr.size {
		return keys.ObjKey(0), false
	}
	
	root, err := lr.tree.GetRootCluster()
	if err != nil || root == nil {
		return keys.ObjKey(0), false
	}
	
	objKey, err := root.GetObjectKey(int(index))
	if err != nil {
		return keys.ObjKey(0), false
	}
	
	return objKey, true
}

// First returns the first element
func (lr *LazyResults) First() (keys.ObjKey, bool) {
	if lr.size == 0 {
		return keys.ObjKey(0), false
	}
	return lr.Get(0)
}

// Last returns the last element
func (lr *LazyResults) Last() (keys.ObjKey, bool) {
	if lr.size == 0 {
		return keys.ObjKey(0), false
	}
	return lr.Get(lr.size - 1)
}

// Slice returns a lazy slice of results
func (lr *LazyResults) Slice(start, end int64) *LazyResults {
	if start < 0 {
		start = 0
	}
	if end > lr.size {
		end = lr.size
	}
	if start >= end {
		return &LazyResults{size: 0}
	}

	// Create a new tree view for the slice
	return &LazyResults{
		tree:     lr.tree,
		iterator: NewIterator(lr.tree, start),
		size:     end - start,
	}
}