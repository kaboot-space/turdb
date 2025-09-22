package cluster

import (
	"fmt"
	"unsafe"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// ClusterHeader represents the header of a cluster stored in file
type ClusterHeader struct {
	ObjectCount uint32      // Number of objects in this cluster
	ColumnCount uint32      // Number of columns in this cluster
	NextCluster storage.Ref // Reference to next cluster (for linked list)
	Reserved    uint32      // Reserved for alignment
}

// NodeType represents different types of cluster nodes
type NodeType int

const (
	NodeTypeLeaf NodeType = iota
	NodeTypeInner
)

// Mixed represents a polymorphic value that can hold different types
type Mixed struct {
	Type  keys.DataType
	Value interface{}
}

// NewMixed creates a new Mixed value
func NewMixed(dataType keys.DataType, value interface{}) *Mixed {
	return &Mixed{
		Type:  dataType,
		Value: value,
	}
}

// FieldValue represents a single field value for batch operations
type FieldValue struct {
	ColKey keys.ColKey
	Value  *Mixed
}

// FieldValues represents a collection of field values for batch operations
type FieldValues struct {
	Values []FieldValue
}

// NewFieldValues creates a new FieldValues collection
func NewFieldValues() *FieldValues {
	return &FieldValues{
		Values: make([]FieldValue, 0),
	}
}

// Add adds a field value to the collection
func (fv *FieldValues) Add(colKey keys.ColKey, value *Mixed) {
	fv.Values = append(fv.Values, FieldValue{
		ColKey: colKey,
		Value:  value,
	})
}

// SplitState represents the state during cluster splitting operations
type SplitState struct {
	LeftCluster  *Cluster
	RightCluster *Cluster
	SplitIndex   int
	PromotedKey  keys.ObjKey
}

// ClusterNode represents the base interface for cluster nodes
type ClusterNode interface {
	GetType() NodeType
	GetObjectCount() int
	GetRef() storage.Ref
	Split() (*SplitState, error)
	Merge(other ClusterNode) error
}

// ClusterNodeInner represents an inner node in the cluster tree
type ClusterNodeInner struct {
	ref        storage.Ref
	header     *ClusterHeader
	children   []storage.Ref
	keys       []keys.ObjKey
	alloc      *core.Allocator
	fileFormat *storage.FileFormat
}

// GetType returns the node type
func (cni *ClusterNodeInner) GetType() NodeType {
	return NodeTypeInner
}

// GetObjectCount returns the number of objects
func (cni *ClusterNodeInner) GetObjectCount() int {
	return len(cni.keys)
}

// GetRef returns the storage reference
func (cni *ClusterNodeInner) GetRef() storage.Ref {
	return cni.ref
}

// Split splits the inner node
func (cni *ClusterNodeInner) Split() (*SplitState, error) {
	if len(cni.keys) < 2 {
		return nil, fmt.Errorf("cannot split node with less than 2 keys")
	}

	splitIndex := len(cni.keys) / 2
	promotedKey := cni.keys[splitIndex]

	// Create left node
	leftNode := &ClusterNodeInner{
		keys:       cni.keys[:splitIndex],
		children:   cni.children[:splitIndex+1],
		alloc:      cni.alloc,
		fileFormat: cni.fileFormat,
	}

	// Create right node
	rightNode := &ClusterNodeInner{
		keys:       cni.keys[splitIndex+1:],
		children:   cni.children[splitIndex+1:],
		alloc:      cni.alloc,
		fileFormat: cni.fileFormat,
	}

	return &SplitState{
		LeftCluster:  &Cluster{nodeInner: leftNode},
		RightCluster: &Cluster{nodeInner: rightNode},
		SplitIndex:   splitIndex,
		PromotedKey:  promotedKey,
	}, nil
}

// Merge merges with another inner node
func (cni *ClusterNodeInner) Merge(other ClusterNode) error {
	otherInner, ok := other.(*ClusterNodeInner)
	if !ok {
		return fmt.Errorf("cannot merge with different node type")
	}

	cni.keys = append(cni.keys, otherInner.keys...)
	cni.children = append(cni.children, otherInner.children...)

	return nil
}

// Cluster represents a storage cluster containing objects
type Cluster struct {
	ref        storage.Ref
	header     *ClusterHeader
	keys       []keys.ObjKey
	columns    map[keys.ColKey]*ColumnArray
	parent     *ClusterTree
	alloc      *core.Allocator
	fileFormat *storage.FileFormat
	nodeType   NodeType
	nodeInner  *ClusterNodeInner
}

// GetType returns the cluster node type
func (c *Cluster) GetType() NodeType {
	return c.nodeType
}

// GetObjectCount returns the number of objects
func (c *Cluster) GetObjectCount() int {
	return len(c.keys)
}

// GetRef returns the storage reference
func (c *Cluster) GetRef() storage.Ref {
	return c.ref
}

// Split splits the cluster when it becomes too large
func (c *Cluster) Split() (*SplitState, error) {
	if len(c.keys) < 2 {
		return nil, fmt.Errorf("cannot split cluster with less than 2 objects")
	}

	splitIndex := len(c.keys) / 2
	promotedKey := c.keys[splitIndex]

	// Create left cluster
	leftCluster, err := NewCluster(c.alloc, c.fileFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to create left cluster: %w", err)
	}

	// Create right cluster
	rightCluster, err := NewCluster(c.alloc, c.fileFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to create right cluster: %w", err)
	}

	// Copy columns to new clusters
	for colKey := range c.columns {
		leftCluster.AddColumn(colKey)
		rightCluster.AddColumn(colKey)
	}

	// Split keys and data
	leftCluster.keys = c.keys[:splitIndex]
	rightCluster.keys = c.keys[splitIndex:]

	// Split column data
	for colKey, columnArray := range c.columns {
		leftArray := leftCluster.columns[colKey]
		rightArray := rightCluster.columns[colKey]

		// Copy data to left cluster
		for i := 0; i < splitIndex; i++ {
			value, _ := columnArray.array.Get(uint32(i))
			leftArray.array.Set(uint32(i), value)
		}

		// Copy data to right cluster
		for i := splitIndex; i < len(c.keys); i++ {
			value, _ := columnArray.array.Get(uint32(i))
			rightArray.array.Set(uint32(i-splitIndex), value)
		}
	}

	return &SplitState{
		LeftCluster:  leftCluster,
		RightCluster: rightCluster,
		SplitIndex:   splitIndex,
		PromotedKey:  promotedKey,
	}, nil
}

// Merge merges this cluster with another cluster
func (c *Cluster) Merge(other ClusterNode) error {
	otherCluster, ok := other.(*Cluster)
	if !ok {
		return fmt.Errorf("cannot merge with different node type")
	}

	// Merge keys
	c.keys = append(c.keys, otherCluster.keys...)

	// Merge column data
	for colKey, columnArray := range c.columns {
		otherArray, exists := otherCluster.columns[colKey]
		if !exists {
			continue
		}

		// Append data from other cluster
		for i := 0; i < len(otherCluster.keys); i++ {
			value, _ := otherArray.array.Get(uint32(i))
			columnArray.array.Add(value)
		}
	}

	return nil
}

// SetMixedValue sets a mixed value for a specific object and column
func (c *Cluster) SetMixedValue(objIndex int, colKey keys.ColKey, mixed *Mixed) error {
	if objIndex < 0 || objIndex >= len(c.keys) {
		return fmt.Errorf("object index out of bounds")
	}

	columnArray, exists := c.columns[colKey]
	if !exists {
		return fmt.Errorf("column does not exist")
	}

	return columnArray.array.Set(uint32(objIndex), mixed.Value)
}

// GetMixedValue retrieves a mixed value for a specific object and column
func (c *Cluster) GetMixedValue(objIndex int, colKey keys.ColKey) (*Mixed, error) {
	if objIndex < 0 || objIndex >= len(c.keys) {
		return nil, fmt.Errorf("object index out of bounds")
	}

	columnArray, exists := c.columns[colKey]
	if !exists {
		return nil, fmt.Errorf("column does not exist")
	}

	value, err := columnArray.array.Get(uint32(objIndex))
	if err != nil {
		return nil, err
	}

	return NewMixed(columnArray.dataType, value), nil
}

// SetFieldValues sets multiple field values for an object in a batch operation
func (c *Cluster) SetFieldValues(objIndex int, fieldValues *FieldValues) error {
	if objIndex < 0 || objIndex >= len(c.keys) {
		return fmt.Errorf("object index out of bounds")
	}

	for _, fieldValue := range fieldValues.Values {
		if err := c.SetMixedValue(objIndex, fieldValue.ColKey, fieldValue.Value); err != nil {
			return fmt.Errorf("failed to set field %v: %w", fieldValue.ColKey, err)
		}
	}

	return nil
}

// GetFieldValues retrieves multiple field values for an object
func (c *Cluster) GetFieldValues(objIndex int, colKeys []keys.ColKey) (*FieldValues, error) {
	if objIndex < 0 || objIndex >= len(c.keys) {
		return nil, fmt.Errorf("object index out of bounds")
	}

	fieldValues := NewFieldValues()

	for _, colKey := range colKeys {
		mixed, err := c.GetMixedValue(objIndex, colKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get field %v: %w", colKey, err)
		}
		fieldValues.Add(colKey, mixed)
	}

	return fieldValues, nil
}

// ColumnArray represents an array of values for a specific column
type ColumnArray struct {
	colKey   keys.ColKey
	dataType keys.DataType
	nullable bool
	array    *core.Array
}

// ClusterTree manages the tree of clusters
type ClusterTree struct {
	root       *Cluster
	height     int
	alloc      *core.Allocator
	fileFormat *storage.FileFormat
	rootRef    storage.Ref
	maxObjects int // Maximum objects per cluster before splitting
}

// SetMaxObjects sets the maximum number of objects per cluster
func (ct *ClusterTree) SetMaxObjects(max int) {
	ct.maxObjects = max
}

// InsertObjectWithSplit inserts an object with automatic cluster splitting
func (ct *ClusterTree) InsertObjectWithSplit(objKey keys.ObjKey) error {
	root, err := ct.GetRootCluster()
	if err != nil {
		return fmt.Errorf("failed to get root cluster: %w", err)
	}

	// Insert object
	if err := root.InsertObject(objKey); err != nil {
		return err
	}

	// Check if split is needed
	if ct.maxObjects > 0 && root.GetObjectCount() > ct.maxObjects {
		return ct.splitRoot()
	}

	return nil
}

// splitRoot splits the root cluster
func (ct *ClusterTree) splitRoot() error {
	splitState, err := ct.root.Split()
	if err != nil {
		return fmt.Errorf("failed to split root: %w", err)
	}

	// Create new root as inner node
	newRoot, err := NewCluster(ct.alloc, ct.fileFormat)
	if err != nil {
		return fmt.Errorf("failed to create new root: %w", err)
	}

	newRoot.nodeType = NodeTypeInner
	newRoot.nodeInner = &ClusterNodeInner{
		keys:       []keys.ObjKey{splitState.PromotedKey},
		children:   []storage.Ref{splitState.LeftCluster.ref, splitState.RightCluster.ref},
		alloc:      ct.alloc,
		fileFormat: ct.fileFormat,
	}

	ct.root = newRoot
	ct.height++

	return nil
}

// NewCluster creates a new cluster
func NewCluster(alloc *core.Allocator, fileFormat *storage.FileFormat) (*Cluster, error) {
	// Allocate space for cluster header
	headerSize := uint32(unsafe.Sizeof(ClusterHeader{}))
	ref, err := fileFormat.AllocateSpace(headerSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate cluster space: %w", err)
	}

	// Initialize header
	header := &ClusterHeader{
		ObjectCount: 0,
		ColumnCount: 0,
		NextCluster: 0,
		Reserved:    0,
	}

	cluster := &Cluster{
		ref:        ref,
		header:     header,
		keys:       make([]keys.ObjKey, 0),
		columns:    make(map[keys.ColKey]*ColumnArray),
		alloc:      alloc,
		fileFormat: fileFormat,
	}

	// Write header to file
	if err := cluster.writeHeader(); err != nil {
		return nil, fmt.Errorf("failed to write cluster header: %w", err)
	}

	return cluster, nil
}

// writeHeader writes the cluster header to file
func (c *Cluster) writeHeader() error {
	offset := c.fileFormat.RefToOffset(c.ref)
	headerData := (*[16]byte)(unsafe.Pointer(c.header))
	mapper := c.fileFormat.GetMapper()

	// Get pointer to the location and copy data
	ptr := mapper.GetPointer(offset)
	copy((*[16]byte)(ptr)[:], headerData[:])

	return mapper.Sync()
}

// readHeader reads the cluster header from file
func (c *Cluster) readHeader() error {
	offset := c.fileFormat.RefToOffset(c.ref)
	mapper := c.fileFormat.GetMapper()
	headerData := mapper.ReadAt(offset, int(unsafe.Sizeof(ClusterHeader{})))

	if len(headerData) < int(unsafe.Sizeof(ClusterHeader{})) {
		return fmt.Errorf("insufficient data for cluster header")
	}

	c.header = (*ClusterHeader)(unsafe.Pointer(&headerData[0]))
	return nil
}

// NewColumnArray creates a new column array
func NewColumnArray(colKey keys.ColKey, alloc *core.Allocator) *ColumnArray {
	dataType := colKey.GetType()
	nullable := colKey.IsNullable()

	array := core.NewArray(dataType, alloc)

	return &ColumnArray{
		colKey:   colKey,
		dataType: dataType,
		nullable: nullable,
		array:    array,
	}
}

// AddColumn adds a new column to the cluster
func (c *Cluster) AddColumn(colKey keys.ColKey) error {
	if _, exists := c.columns[colKey]; exists {
		return fmt.Errorf("column already exists")
	}

	columnArray := NewColumnArray(colKey, c.alloc)
	if err := columnArray.array.Create(16); err != nil { // Initial capacity of 16
		return err
	}

	c.columns[colKey] = columnArray
	return nil
}

// InsertObject inserts a new object into the cluster
func (c *Cluster) InsertObject(objKey keys.ObjKey) error {
	// Add object key
	c.keys = append(c.keys, objKey)

	// Extend all column arrays to accommodate new object
	for _, columnArray := range c.columns {
		if columnArray != nil {
			if err := columnArray.array.Add(nil); err != nil { // Add null placeholder
				return err
			}
		}
	}

	return nil
}

// SetValue sets a value for a specific object and column
func (c *Cluster) SetValue(objIndex int, colKey keys.ColKey, value interface{}) error {
	if objIndex < 0 || objIndex >= len(c.keys) {
		return fmt.Errorf("object index out of bounds")
	}

	columnArray, exists := c.columns[colKey]
	if !exists {
		return fmt.Errorf("column does not exist")
	}

	return columnArray.array.Set(uint32(objIndex), value)
}

// GetValue retrieves a value for a specific object and column
func (c *Cluster) GetValue(objIndex int, colKey keys.ColKey) (interface{}, error) {
	if objIndex < 0 || objIndex >= len(c.keys) {
		return nil, fmt.Errorf("object index out of bounds")
	}

	columnArray, exists := c.columns[colKey]
	if !exists {
		return nil, fmt.Errorf("column does not exist")
	}

	return columnArray.array.Get(uint32(objIndex))
}

// AddBacklinkValue adds a backlink value to an object's backlink column
func (c *Cluster) AddBacklinkValue(backlinkCol keys.ColKey, objIndex int, backlinkRef keys.ObjLink) error {
	columnArray, exists := c.columns[backlinkCol]
	if !exists {
		return fmt.Errorf("backlink column does not exist")
	}

	if objIndex < 0 || objIndex >= len(c.keys) {
		return fmt.Errorf("object index out of bounds")
	}

	// Get existing backlink set or create new one
	existingValue, err := columnArray.array.Get(uint32(objIndex))
	if err != nil {
		return fmt.Errorf("failed to get existing backlink value: %w", err)
	}

	var linkSet *keys.LinkSet
	if existingValue == nil {
		linkSet = keys.NewLinkSet()
	} else {
		var ok bool
		linkSet, ok = existingValue.(*keys.LinkSet)
		if !ok {
			return fmt.Errorf("existing value is not a LinkSet")
		}
	}

	// Add the new backlink
	linkSet.Add(backlinkRef)

	// Set the updated value
	return columnArray.array.Set(uint32(objIndex), linkSet)
}

// RemoveBacklinkValue removes a backlink value from an object's backlink column
func (c *Cluster) RemoveBacklinkValue(backlinkCol keys.ColKey, objIndex int, backlinkRef keys.ObjLink) error {
	columnArray, exists := c.columns[backlinkCol]
	if !exists {
		return fmt.Errorf("backlink column does not exist")
	}

	if objIndex < 0 || objIndex >= len(c.keys) {
		return fmt.Errorf("object index out of bounds")
	}

	// Get existing backlink set
	existingValue, err := columnArray.array.Get(uint32(objIndex))
	if err != nil {
		return fmt.Errorf("failed to get existing backlink value: %w", err)
	}

	if existingValue == nil {
		return nil // Nothing to remove
	}

	linkSet, ok := existingValue.(*keys.LinkSet)
	if !ok {
		return fmt.Errorf("existing value is not a LinkSet")
	}

	// Remove the backlink
	linkSet.RemoveLink(backlinkRef)

	// Set the updated value (or nil if empty)
	if linkSet.Size() == 0 {
		return columnArray.array.Set(uint32(objIndex), nil)
	}
	return columnArray.array.Set(uint32(objIndex), linkSet)
}

// GetObjectKey returns the object key at the given index
func (c *Cluster) GetObjectKey(objIndex int) (keys.ObjKey, error) {
	if objIndex < 0 || objIndex >= len(c.keys) {
		return 0, fmt.Errorf("object index out of bounds")
	}

	return c.keys[objIndex], nil
}

// FindObject finds the index of an object with the given key
func (c *Cluster) FindObject(objKey keys.ObjKey) int {
	for i, key := range c.keys {
		if key == objKey {
			return i
		}
	}
	return -1
}

// ObjectCount returns the number of objects in the cluster
func (c *Cluster) ObjectCount() int {
	return len(c.keys)
}

// GetColumns returns all column keys
func (c *Cluster) GetColumns() []keys.ColKey {
	columns := make([]keys.ColKey, 0, len(c.columns))
	for colKey := range c.columns {
		columns = append(columns, colKey)
	}
	return columns
}

// RemoveColumn removes a column from the cluster
func (c *Cluster) RemoveColumn(colKey keys.ColKey) error {
	if _, exists := c.columns[colKey]; !exists {
		return fmt.Errorf("column does not exist")
	}

	delete(c.columns, colKey)
	return nil
}

// DeleteObject removes an object from the cluster
func (c *Cluster) DeleteObject(objIndex int) error {
	if objIndex < 0 || objIndex >= len(c.keys) {
		return fmt.Errorf("object index out of bounds")
	}

	// Remove object key
	c.keys = append(c.keys[:objIndex], c.keys[objIndex+1:]...)

	// For now, we'll just mark the position as deleted by setting null values
	// In a full implementation, we would need to implement array compaction
	for _, columnArray := range c.columns {
		if err := columnArray.array.Set(uint32(objIndex), nil); err != nil {
			return err
		}
	}

	return nil
}

// NewClusterTree creates a new cluster tree
func NewClusterTree(alloc *core.Allocator, fileFormat *storage.FileFormat) *ClusterTree {
	return &ClusterTree{
		root:       nil,
		height:     0,
		alloc:      alloc,
		fileFormat: fileFormat,
		rootRef:    0,
	}
}

// GetRootCluster returns the root cluster, creating it if necessary
func (ct *ClusterTree) GetRootCluster() (*Cluster, error) {
	if ct.root == nil {
		var err error
		ct.root, err = NewCluster(ct.alloc, ct.fileFormat)
		if err != nil {
			return nil, fmt.Errorf("failed to create root cluster: %w", err)
		}
		ct.rootRef = ct.root.ref
	}
	return ct.root, nil
}

// InsertObject inserts an object into the appropriate cluster
func (ct *ClusterTree) InsertObject(objKey keys.ObjKey) error {
	// For now, just insert into root cluster
	// In a full implementation, this would manage cluster splitting
	root, err := ct.GetRootCluster()
	if err != nil {
		return fmt.Errorf("failed to get root cluster: %w", err)
	}
	return root.InsertObject(objKey)
}

// FindObject finds an object and returns the cluster and index
func (ct *ClusterTree) FindObject(objKey keys.ObjKey) (*Cluster, int) {
	// For now, just search in root cluster
	// In a full implementation, this would traverse the tree
	root, err := ct.GetRootCluster()
	if err != nil {
		return nil, -1
	}

	index := root.FindObject(objKey)
	if index >= 0 {
		return root, index
	}

	return nil, -1
}

// AddColumn adds a column to all clusters
func (ct *ClusterTree) AddColumn(colKey keys.ColKey) error {
	// For now, just add to root cluster
	// In a full implementation, this would add to all clusters
	root, err := ct.GetRootCluster()
	if err != nil {
		return fmt.Errorf("failed to get root cluster: %w", err)
	}
	return root.AddColumn(colKey)
}

// SetValue sets a value for an object
func (ct *ClusterTree) SetValue(objKey keys.ObjKey, colKey keys.ColKey, value interface{}) error {
	cluster, index := ct.FindObject(objKey)
	if cluster == nil {
		return fmt.Errorf("object not found")
	}

	return cluster.SetValue(index, colKey, value)
}

// GetValue gets a value for an object
func (ct *ClusterTree) GetValue(objKey keys.ObjKey, colKey keys.ColKey) (interface{}, error) {
	cluster, index := ct.FindObject(objKey)
	if cluster == nil {
		return nil, fmt.Errorf("object not found")
	}

	return cluster.GetValue(index, colKey)
}

// RemoveColumn removes a column from all clusters
func (ct *ClusterTree) RemoveColumn(colKey keys.ColKey) error {
	// For now, just remove from root cluster
	// In a full implementation, this would remove from all clusters
	root, err := ct.GetRootCluster()
	if err != nil {
		return fmt.Errorf("failed to get root cluster: %w", err)
	}
	return root.RemoveColumn(colKey)
}

// DeleteObject removes an object from the cluster tree
func (ct *ClusterTree) DeleteObject(objKey keys.ObjKey) error {
	cluster, index := ct.FindObject(objKey)
	if cluster == nil {
		return fmt.Errorf("object not found")
	}

	return cluster.DeleteObject(index)
}
