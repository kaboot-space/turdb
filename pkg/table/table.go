package table

import (
	"fmt"
	"sync"
	"time"

	"github.com/turdb/tur/pkg/cluster"
	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// Column validation constants based on tur-core
const (
	MaxColumnNameLength = 63     // Maximum column name length
	MaxNumColumns       = 0xFFFF // Maximum number of columns per table
)

// TableType represents different types of tables based on tur-core
type TableType int

const (
	TopLevel TableType = iota
	Embedded
	TopLevelAsymmetric
)

// TableState represents the lifecycle state of a table
type TableState int

const (
	StateDetached TableState = iota
	StateAttached
	StateInvalid
)

// TableRef provides safe table access with lifecycle management
type TableRef struct {
	table    *Table
	refCount int32
	isValid  bool
	mutex    sync.RWMutex
}

// TableSpec represents enhanced table specification with schema management
type TableSpec struct {
	Name        string        `json:"name"`
	Type        TableType     `json:"type"`
	Columns     []ColumnSpec  `json:"columns"`
	Indexes     []IndexSpec   `json:"indexes"`
	Version     uint32        `json:"version"`
	Created     int64         `json:"created"`
	Modified    int64         `json:"modified"`
	SchemaHash  uint64        `json:"schema_hash"`
	ParentTable keys.TableKey `json:"parent_table,omitempty"`
	ParentCol   keys.ColKey   `json:"parent_col,omitempty"`
}

// ColumnSpec defines a column specification
type ColumnSpec struct {
	Name        string            `json:"name"`
	Type        keys.DataType     `json:"type"`
	Nullable    bool              `json:"nullable"`
	Key         keys.ColKey       `json:"key"`
	ElementType keys.DataType     `json:"element_type,omitempty"` // For collections
	KeyType     keys.DataType     `json:"key_type,omitempty"`     // For dictionaries
	Indexed     bool              `json:"indexed,omitempty"`      // Whether column is indexed
	Created     int64             `json:"created,omitempty"`      // Creation timestamp
	Attributes  *ColumnAttributes `json:"attributes,omitempty"`   // Column attributes
}

// IndexSpec defines an index specification
type IndexSpec struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// Table represents a database table with enhanced structure integration
type Table struct {
	key         keys.TableKey
	name        string
	spec        *TableSpec
	tableType   TableType
	state       TableState
	clusterTree *cluster.ClusterTree
	columns     map[string]keys.ColKey
	columnSpecs map[keys.ColKey]*ColumnSpec
	group       *storage.Group
	alloc       *core.Allocator
	fileFormat  *storage.FileFormat
	parent      *Table
	parentCol   keys.ColKey
	refs        []*TableRef
	mutex       sync.RWMutex
	isOpen      bool
	primaryKey  keys.ColKey // Primary key column
}

// NewTable creates a new table with the given specification
func NewTable(name string, spec *TableSpec, group *storage.Group, alloc *core.Allocator, fileFormat *storage.FileFormat) (*Table, error) {
	if spec == nil {
		return nil, fmt.Errorf("table specification cannot be nil")
	}

	// Set creation and modification timestamps
	now := time.Now().Unix()
	spec.Created = now
	spec.Modified = now

	// Generate table key using allocator
	ref, err := alloc.Alloc(8) // Allocate 8 bytes for table key
	if err != nil {
		return nil, fmt.Errorf("failed to allocate table key: %w", err)
	}
	tableKey := keys.TableKey(ref)

	// Create cluster tree
	clusterTree := cluster.NewClusterTree(alloc, fileFormat)

	table := &Table{
		key:         tableKey,
		name:        name,
		spec:        spec,
		clusterTree: clusterTree,
		columns:     make(map[string]keys.ColKey),
		columnSpecs: make(map[keys.ColKey]*ColumnSpec),
		group:       group,
		alloc:       alloc,
		fileFormat:  fileFormat,
		isOpen:      true,
	}

	// Add columns from spec
	for _, colSpec := range spec.Columns {
		if err := table.addColumnFromSpec(colSpec); err != nil {
			return nil, fmt.Errorf("failed to add column '%s': %w", colSpec.Name, err)
		}
	}

	return table, nil
}

// LoadTable loads an existing table from storage
func LoadTable(tableKey keys.TableKey, name string, group *storage.Group, alloc *core.Allocator, fileFormat *storage.FileFormat) (*Table, error) {
	// Create cluster tree
	clusterTree := cluster.NewClusterTree(alloc, fileFormat)

	table := &Table{
		key:         tableKey,
		name:        name,
		clusterTree: clusterTree,
		columns:     make(map[string]keys.ColKey),
		columnSpecs: make(map[keys.ColKey]*ColumnSpec),
		group:       group,
		alloc:       alloc,
		fileFormat:  fileFormat,
		isOpen:      true,
	}

	// Load table metadata and columns from storage
	// This would be implemented based on the storage format

	return table, nil
}

// addColumnFromSpec adds a column from a column specification
func (t *Table) addColumnFromSpec(colSpec ColumnSpec) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Generate column key if not provided
	if colSpec.Key == 0 {
		colSpec.Key = keys.GenerateColKeySimple(colSpec.Type, colSpec.Nullable)
	}

	// Validate column specification
	if err := t.validateColumnSpec(&colSpec); err != nil {
		return err
	}

	// Add to cluster tree
	if err := t.clusterTree.AddColumn(colSpec.Key); err != nil {
		return fmt.Errorf("failed to add column to cluster: %w", err)
	}

	// Store column mapping
	t.columns[colSpec.Name] = colSpec.Key
	t.columnSpecs[colSpec.Key] = &colSpec

	return nil
}

// GetKey returns the table key
func (t *Table) GetKey() keys.TableKey {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.key
}

// GetName returns the table name
func (t *Table) GetName() string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.name
}

// IsOpen returns whether the table is open
func (t *Table) IsOpen() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.isOpen
}

// Close closes the table
func (t *Table) Close() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.isOpen = false
}

// AddColumn adds a new column to the table
func (t *Table) AddColumn(name string, dataType keys.DataType, nullable bool) (keys.ColKey, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return 0, fmt.Errorf("table is not open")
	}

	// Check for duplicate column name
	if _, exists := t.columns[name]; exists {
		return 0, fmt.Errorf("column '%s' already exists", name)
	}

	// Generate column key using allocator
	_, err := t.alloc.Alloc(8) // Allocate 8 bytes for column key
	if err != nil {
		return 0, fmt.Errorf("failed to allocate column key: %w", err)
	}

	// Create column attributes
	attrs := keys.NewColumnAttrMask()
	if nullable {
		attrs.Set(keys.ColAttrNullable)
	}

	// Generate column key with proper attributes
	colKey := keys.GenerateColKey(dataType, attrs)

	// Create column specification
	colSpec := &ColumnSpec{
		Name:     name,
		Type:     dataType,
		Nullable: nullable,
		Key:      colKey,
	}

	// Validate column specification
	if err := t.validateColumnSpec(colSpec); err != nil {
		return 0, err
	}

	// Update modification timestamp
	t.spec.Modified = time.Now().UnixNano() / 1000000 // Convert to milliseconds

	// Store column mapping
	t.columns[name] = colKey
	t.columnSpecs[colKey] = colSpec

	// Add column to cluster tree
	return colKey, t.clusterTree.AddColumn(colKey)
}

// RemoveColumn removes a column from the table
func (t *Table) RemoveColumn(name string) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	colKey, exists := t.columns[name]
	if !exists {
		return fmt.Errorf("column %s not found", name)
	}

	if err := t.clusterTree.RemoveColumn(colKey); err != nil {
		return fmt.Errorf("failed to remove column from cluster: %w", err)
	}

	delete(t.columns, name)
	delete(t.columnSpecs, colKey)

	// Update modification timestamp
	t.spec.Modified = time.Now().Unix()

	return nil
}

// GetColumn returns the column key for a given column name
func (t *Table) GetColumn(name string) (keys.ColKey, bool) {
	colKey, exists := t.columns[name]
	return colKey, exists
}

// GetColumns returns all column keys
func (t *Table) GetColumns() []keys.ColKey {
	columns := make([]keys.ColKey, 0, len(t.columns))
	for _, colKey := range t.columns {
		columns = append(columns, colKey)
	}
	return columns
}

// CreateObject creates a new object in the table
func (t *Table) CreateObject() (keys.ObjKey, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return 0, fmt.Errorf("table is closed")
	}

	// Generate object key using allocator
	ref, err := t.alloc.Alloc(8) // Allocate 8 bytes for object key
	if err != nil {
		return 0, fmt.Errorf("failed to allocate object key: %w", err)
	}
	objKey := keys.ObjKey(ref)

	if err := t.clusterTree.InsertObject(objKey); err != nil {
		return 0, fmt.Errorf("failed to insert object: %w", err)
	}

	return objKey, nil
}

// GetObject retrieves an object by its key
func (t *Table) GetObject(objKey keys.ObjKey) (*Object, error) {
	return &Object{
		key:         objKey,
		table:       t,
		clusterTree: t.clusterTree,
	}, nil
}

// DeleteObject deletes an object from the table
func (t *Table) DeleteObject(objKey keys.ObjKey) error {
	return t.clusterTree.DeleteObject(objKey)
}

// Object represents a row/object in the table
type Object struct {
	key         keys.ObjKey
	table       *Table
	clusterTree *cluster.ClusterTree
}

// GetKey returns the object key
func (o *Object) GetKey() keys.ObjKey {
	return o.key
}

// Set sets a value for a column
func (o *Object) Set(colKey keys.ColKey, value interface{}) error {
	return o.clusterTree.SetValue(o.key, colKey, value)
}

// Get gets a value for a column
func (o *Object) Get(colKey keys.ColKey) (interface{}, error) {
	return o.clusterTree.GetValue(o.key, colKey)
}

// GetInt gets an integer value
func (o *Object) GetInt(colKey keys.ColKey) (int64, error) {
	value, err := o.Get(colKey)
	if err != nil {
		return 0, err
	}
	if intVal, ok := value.(int64); ok {
		return intVal, nil
	}
	return 0, fmt.Errorf("value is not an integer")
}

// GetString gets a string value
func (o *Object) GetString(colKey keys.ColKey) (string, error) {
	value, err := o.Get(colKey)
	if err != nil {
		return "", err
	}
	if strVal, ok := value.(string); ok {
		return strVal, nil
	}
	return "", fmt.Errorf("value is not a string")
}

// GetBool gets a boolean value
func (o *Object) GetBool(colKey keys.ColKey) (bool, error) {
	value, err := o.Get(colKey)
	if err != nil {
		return false, err
	}
	if boolVal, ok := value.(bool); ok {
		return boolVal, nil
	}
	return false, fmt.Errorf("value is not a boolean")
}

// GetObjectCount returns the number of objects in the table
func (t *Table) GetObjectCount() uint64 {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// Get count from cluster tree
	root, err := t.clusterTree.GetRootCluster()
	if err != nil {
		return 0
	}
	return uint64(root.ObjectCount())
}

// ObjectExists checks if an object exists in the table
func (t *Table) ObjectExists(objKey keys.ObjKey) bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	cluster, index := t.clusterTree.FindObject(objKey)
	return cluster != nil && index >= 0
}

// SetValue sets a value for an object's column by column name
func (t *Table) SetValue(objKey keys.ObjKey, columnName string, value interface{}) error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if !t.isOpen {
		return fmt.Errorf("table is not open")
	}

	// Get column key
	colKey, exists := t.columns[columnName]
	if !exists {
		return fmt.Errorf("column '%s' not found", columnName)
	}

	// Validate value type
	if err := t.validateValue(colKey, value); err != nil {
		return err
	}

	// Set value in cluster tree
	return t.clusterTree.SetValue(objKey, colKey, value)
}

// GetValue retrieves a value for an object's column by column name
func (t *Table) GetValue(objKey keys.ObjKey, columnName string) (interface{}, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if !t.isOpen {
		return nil, fmt.Errorf("table is not open")
	}

	// Get column key
	colKey, exists := t.columns[columnName]
	if !exists {
		return nil, fmt.Errorf("column '%s' not found", columnName)
	}

	// Get value from cluster tree
	return t.clusterTree.GetValue(objKey, colKey)
}

// GetSpec returns the table specification
func (t *Table) GetSpec() *TableSpec {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.spec
}

// UpdateSpec updates the table specification
func (t *Table) UpdateSpec(spec *TableSpec) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return fmt.Errorf("table is not open")
	}

	// Update modification timestamp
	spec.Modified = time.Now().UnixNano() / 1000000 // Convert to milliseconds

	t.spec = spec
	return nil
}

// validateValue validates that a value is compatible with the column specification
func (t *Table) validateValue(colKey keys.ColKey, value interface{}) error {
	colSpec, exists := t.columnSpecs[colKey]
	if !exists {
		return fmt.Errorf("column specification not found")
	}

	if value == nil {
		if !colSpec.Nullable {
			return fmt.Errorf("column '%s' is not nullable", colSpec.Name)
		}
		return nil
	}

	switch colSpec.Type {
	case keys.TypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string value for column '%s'", colSpec.Name)
		}
	case keys.TypeInt:
		switch value.(type) {
		case int, int64, uint64:
			// OK
		default:
			return fmt.Errorf("expected integer value for column '%s'", colSpec.Name)
		}
	case keys.TypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean value for column '%s'", colSpec.Name)
		}
	case keys.TypeFloat:
		if _, ok := value.(float32); !ok {
			return fmt.Errorf("expected float32 value for column '%s'", colSpec.Name)
		}
	case keys.TypeDouble:
		switch value.(type) {
		case float64, float32:
			// OK
		default:
			return fmt.Errorf("expected float value for column '%s'", colSpec.Name)
		}
	case keys.TypeLink:
		if _, ok := value.(keys.ObjKey); !ok {
			return fmt.Errorf("expected ObjKey value for column '%s'", colSpec.Name)
		}
	case keys.TypeTypedLink:
		if _, ok := value.(keys.TypedLink); !ok {
			return fmt.Errorf("expected TypedLink value for column '%s'", colSpec.Name)
		}
	default:
		return fmt.Errorf("unsupported data type: %v for column '%s'", colSpec.Type, colSpec.Name)
	}

	return nil
}

// Attach attaches the table to its parent group with lifecycle management
func (t *Table) Attach() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.state == StateAttached {
		return nil
	}

	if t.state == StateInvalid {
		return fmt.Errorf("cannot attach invalid table")
	}

	// Perform attachment logic
	t.state = StateAttached
	return nil
}

// Detach detaches the table from its parent group
func (t *Table) Detach() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.state == StateDetached {
		return nil
	}

	// Invalidate all references
	for _, ref := range t.refs {
		ref.invalidate()
	}
	t.refs = nil

	t.state = StateDetached
	return nil
}

// GetTableRef creates a new table reference for safe access
func (t *Table) GetTableRef() *TableRef {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	ref := &TableRef{
		table:   t,
		isValid: t.state == StateAttached,
	}

	t.refs = append(t.refs, ref)
	return ref
}

// GetType returns the table type
func (t *Table) GetType() TableType {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.tableType
}

// GetState returns the current table state
func (t *Table) GetState() TableState {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.state
}

// SetParent sets the parent table for embedded tables
func (t *Table) SetParent(parent *Table, parentCol keys.ColKey) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.tableType != Embedded {
		return fmt.Errorf("only embedded tables can have parents")
	}

	t.parent = parent
	t.parentCol = parentCol
	t.spec.ParentTable = parent.key
	t.spec.ParentCol = parentCol

	return nil
}

// GetParent returns the parent table for embedded tables
func (t *Table) GetParent() (*Table, keys.ColKey) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.parent, t.parentCol
}

// ValidateSchema validates the table schema against constraints
func (t *Table) ValidateSchema() error {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// Validate column specifications
	for _, colSpec := range t.columnSpecs {
		if err := t.validateColumnSpec(colSpec); err != nil {
			return fmt.Errorf("invalid column spec for %s: %w", colSpec.Name, err)
		}
	}

	// Validate table type constraints
	switch t.tableType {
	case Embedded:
		if t.parent == nil {
			return fmt.Errorf("embedded table must have a parent")
		}
	case TopLevel, TopLevelAsymmetric:
		if t.parent != nil {
			return fmt.Errorf("top-level table cannot have a parent")
		}
	}

	return nil
}

// validateColumnSpec validates a column specification
func (t *Table) validateColumnSpec(colSpec *ColumnSpec) error {
	if colSpec.Name == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	// Validate column name length
	if len(colSpec.Name) > MaxColumnNameLength {
		return fmt.Errorf("column name '%s' exceeds maximum length of %d characters", colSpec.Name, MaxColumnNameLength)
	}

	// Validate column count limit
	if len(t.columnSpecs) >= MaxNumColumns {
		return fmt.Errorf("table '%s' exceeds maximum number of columns (%d)", t.name, MaxNumColumns)
	}

	if colSpec.Key == 0 {
		return fmt.Errorf("column key cannot be zero")
	}

	// Validate data type
	if !isValidDataType(colSpec.Type) {
		return fmt.Errorf("invalid data type: %v", colSpec.Type)
	}

	// Validate collection element types
	if keys.IsCollectionType(colSpec.Type) {
		if colSpec.Type == keys.TypeDict {
			if colSpec.KeyType == 0 {
				return fmt.Errorf("dictionary column '%s' must specify key type", colSpec.Name)
			}
			if !isValidDataType(colSpec.KeyType) {
				return fmt.Errorf("invalid dictionary key type for column '%s': %v", colSpec.Name, colSpec.KeyType)
			}
		}
		if colSpec.ElementType == 0 {
			return fmt.Errorf("collection column '%s' must specify element type", colSpec.Name)
		}
		if !isValidDataType(colSpec.ElementType) {
			return fmt.Errorf("invalid collection element type for column '%s': %v", colSpec.Name, colSpec.ElementType)
		}
	}

	return nil
}

// isValidDataType checks if a data type is valid
func isValidDataType(dataType keys.DataType) bool {
	switch dataType {
	case keys.TypeInt, keys.TypeBool, keys.TypeString, keys.TypeBinary,
		keys.TypeMixed, keys.TypeTimestamp, keys.TypeFloat, keys.TypeDouble,
		keys.TypeDecimal, keys.TypeLink, keys.TypeObjectID, keys.TypeTypedLink,
		keys.TypeUUID, keys.TypeList, keys.TypeSet, keys.TypeDict:
		return true
	default:
		return false
	}
}

// UpdateSchemaHash updates the schema hash after modifications
func (t *Table) UpdateSchemaHash() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Simple hash calculation based on column specs
	hash := uint64(0)
	for _, colSpec := range t.columnSpecs {
		hash ^= uint64(colSpec.Key)
		hash ^= uint64(colSpec.Type)
	}

	t.spec.SchemaHash = hash
	// Use nanosecond precision for more reliable timestamp updates
	t.spec.Modified = time.Now().UnixNano() / 1000000 // Convert to milliseconds
}

// TableRef methods

// IsValid returns whether the table reference is still valid
func (ref *TableRef) IsValid() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.isValid
}

// GetTable returns the underlying table if the reference is valid
func (ref *TableRef) GetTable() (*Table, error) {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()

	if !ref.isValid {
		return nil, fmt.Errorf("table reference is invalid")
	}

	return ref.table, nil
}

// invalidate marks the reference as invalid
func (ref *TableRef) invalidate() {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.isValid = false
}

// GetPrimaryKeyColumn returns the primary key column, following tur-core pattern
func (t *Table) GetPrimaryKeyColumn() keys.ColKey {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.primaryKey
}

// SetPrimaryKeyColumn sets the primary key column, following tur-core pattern
func (t *Table) SetPrimaryKeyColumn(colKey keys.ColKey) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return fmt.Errorf("table is not open")
	}

	// Validate that the column exists
	found := false
	for _, existingColKey := range t.columns {
		if existingColKey == colKey {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("column key %d not found in table", colKey)
	}

	// Validate that the column type is suitable for primary key
	colSpec, exists := t.columnSpecs[colKey]
	if !exists {
		return fmt.Errorf("column specification not found for key %d", colKey)
	}

	// Primary keys cannot be nullable
	if colSpec.Nullable {
		return fmt.Errorf("primary key column cannot be nullable")
	}

	// Update primary key
	t.primaryKey = colKey
	t.spec.Modified = time.Now().Unix()

	return nil
}

// HasPrimaryKey returns true if the table has a primary key set
func (t *Table) HasPrimaryKey() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.primaryKey != 0
}

// GetPrimaryKeyColumnName returns the name of the primary key column
func (t *Table) GetPrimaryKeyColumnName() (string, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if t.primaryKey == 0 {
		return "", fmt.Errorf("no primary key set")
	}

	// Find column name by key
	for name, colKey := range t.columns {
		if colKey == t.primaryKey {
			return name, nil
		}
	}

	return "", fmt.Errorf("primary key column name not found")
}

// AddColumnList adds a list column to the table, following tur-core pattern
func (t *Table) AddColumnList(name string, elementType keys.DataType, nullable bool) (keys.ColKey, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return 0, fmt.Errorf("table is not open")
	}

	// Check if column already exists
	if _, exists := t.columns[name]; exists {
		return 0, fmt.Errorf("column '%s' already exists", name)
	}

	// Create column attributes with list flag
	attrs := keys.NewColumnAttrMask()
	attrs.Set(keys.ColAttrList)
	if nullable {
		attrs.Set(keys.ColAttrNullable)
	}

	// Generate new column key for list type using element type with list attributes
	colKey := keys.GenerateColKey(elementType, attrs)

	// Create table-level column attributes
	tableAttrs := NewColumnAttributes(ColAttrList)
	if nullable {
		tableAttrs.SetAttribute(ColAttrNullable)
	}
	tableAttrs.SetElementType(elementType)

	// Create column specification
	colSpec := &ColumnSpec{
		Name:        name,
		Type:        elementType, // Store element type, not collection type
		ElementType: elementType,
		Nullable:    nullable,
		Indexed:     false,
		Created:     time.Now().Unix(),
		Attributes:  tableAttrs,
	}

	// Add to table
	t.columns[name] = colKey
	t.columnSpecs[colKey] = colSpec
	t.spec.Modified = time.Now().Unix()

	return colKey, nil
}

// AddColumnSet adds a set column to the table, following tur-core pattern
func (t *Table) AddColumnSet(name string, elementType keys.DataType, nullable bool) (keys.ColKey, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return 0, fmt.Errorf("table is not open")
	}

	// Check if column already exists
	if _, exists := t.columns[name]; exists {
		return 0, fmt.Errorf("column '%s' already exists", name)
	}

	// Create column attributes with set flag
	attrs := keys.NewColumnAttrMask()
	attrs.Set(keys.ColAttrSet)
	if nullable {
		attrs.Set(keys.ColAttrNullable)
	}

	// Generate new column key for set type using element type with set attributes
	colKey := keys.GenerateColKey(elementType, attrs)

	// Create table-level column attributes
	tableAttrs := NewColumnAttributes(ColAttrSet)
	if nullable {
		tableAttrs.SetAttribute(ColAttrNullable)
	}
	tableAttrs.SetElementType(elementType)

	// Create column specification
	colSpec := &ColumnSpec{
		Name:        name,
		Type:        elementType, // Store element type, not collection type
		ElementType: elementType,
		Nullable:    nullable,
		Indexed:     false,
		Created:     time.Now().Unix(),
		Attributes:  tableAttrs,
	}

	// Add to table
	t.columns[name] = colKey
	t.columnSpecs[colKey] = colSpec
	t.spec.Modified = time.Now().Unix()

	return colKey, nil
}

// AddColumnDictionary adds a dictionary column to the table, following tur-core pattern
func (t *Table) AddColumnDictionary(name string, keyType, valueType keys.DataType, nullable bool) (keys.ColKey, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return 0, fmt.Errorf("table is not open")
	}

	// Check if column already exists
	if _, exists := t.columns[name]; exists {
		return 0, fmt.Errorf("column '%s' already exists", name)
	}

	// Create column attributes with dictionary flag
	attrs := keys.NewColumnAttrMask()
	attrs.Set(keys.ColAttrDictionary)
	if nullable {
		attrs.Set(keys.ColAttrNullable)
	}

	// Generate new column key for dictionary type using value type with dictionary attributes
	colKey := keys.GenerateColKey(valueType, attrs)

	// Create table-level column attributes
	tableAttrs := NewColumnAttributes(ColAttrDict)
	if nullable {
		tableAttrs.SetAttribute(ColAttrNullable)
	}
	tableAttrs.SetElementType(valueType)
	tableAttrs.SetKeyType(keyType)

	// Create column specification with both key and value types
	colSpec := &ColumnSpec{
		Name:        name,
		Type:        valueType, // Store value type, not collection type
		ElementType: valueType,
		KeyType:     keyType,
		Nullable:    nullable,
		Indexed:     false,
		Created:     time.Now().Unix(),
		Attributes:  tableAttrs,
	}

	// Add to table
	t.columns[name] = colKey
	t.columnSpecs[colKey] = colSpec
	t.spec.Modified = time.Now().Unix()

	return colKey, nil
}

// GetCollectionElementType returns the element type for collection columns
func (t *Table) GetCollectionElementType(colKey keys.ColKey) (keys.DataType, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	colSpec, exists := t.columnSpecs[colKey]
	if !exists {
		return 0, fmt.Errorf("column not found")
	}

	// Check if the column is a collection using ColKey attributes, not the stored Type
	if !colKey.IsCollection() {
		return 0, fmt.Errorf("column is not a collection type")
	}

	return colSpec.ElementType, nil
}

// GetDictionaryKeyType returns the key type for dictionary columns
func (t *Table) GetDictionaryKeyType(colKey keys.ColKey) (keys.DataType, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	colSpec, exists := t.columnSpecs[colKey]
	if !exists {
		return 0, fmt.Errorf("column not found")
	}

	// Check if the column is a dictionary using ColKey attributes, not the stored Type
	if !colKey.IsDictionary() {
		return 0, fmt.Errorf("column is not a dictionary type")
	}

	return colSpec.KeyType, nil
}

// SetColumnAttributes sets attributes for a column
func (t *Table) SetColumnAttributes(colKey keys.ColKey, attrs *ColumnAttributes) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	spec, exists := t.columnSpecs[colKey]
	if !exists {
		return fmt.Errorf("column with key %v not found", colKey)
	}

	// Validate attributes
	if err := attrs.ValidateAttributes(); err != nil {
		return fmt.Errorf("invalid attributes: %w", err)
	}

	spec.Attributes = attrs
	t.columnSpecs[colKey] = spec
	t.spec.Modified = time.Now().Unix()

	return nil
}

// GetColumnAttributes returns attributes for a column
func (t *Table) GetColumnAttributes(colKey keys.ColKey) (*ColumnAttributes, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	spec, exists := t.columnSpecs[colKey]
	if !exists {
		return nil, fmt.Errorf("column with key %v not found", colKey)
	}

	if spec.Attributes == nil {
		return NewColumnAttributes(ColAttrNone), nil
	}

	return spec.Attributes, nil
}

// SetColumnAttribute sets a specific attribute for a column
func (t *Table) SetColumnAttribute(colKey keys.ColKey, attr ColumnAttrMask) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	spec, exists := t.columnSpecs[colKey]
	if !exists {
		return fmt.Errorf("column with key %v not found", colKey)
	}

	// Check for primary key on nullable column before setting
	if attr == ColAttrPrimaryKey && spec.Nullable {
		return fmt.Errorf("primary key column cannot be nullable")
	}

	if spec.Attributes == nil {
		// Initialize attributes based on column's nullable property
		if spec.Nullable {
			spec.Attributes = NewColumnAttributes(ColAttrNullable)
		} else {
			spec.Attributes = NewColumnAttributes(ColAttrNone)
		}
	}

	spec.Attributes.SetAttribute(attr)

	// Validate after setting
	if err := spec.Attributes.ValidateAttributes(); err != nil {
		spec.Attributes.ClearAttribute(attr) // Rollback
		return fmt.Errorf("invalid attribute combination: %w", err)
	}

	t.columnSpecs[colKey] = spec
	t.spec.Modified = time.Now().Unix()

	return nil
}

// ClearColumnAttribute clears a specific attribute for a column
func (t *Table) ClearColumnAttribute(colKey keys.ColKey, attr ColumnAttrMask) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	spec, exists := t.columnSpecs[colKey]
	if !exists {
		return fmt.Errorf("column with key %v not found", colKey)
	}

	if spec.Attributes != nil {
		spec.Attributes.ClearAttribute(attr)
		t.columnSpecs[colKey] = spec
		t.spec.Modified = time.Now().Unix()
	}

	return nil
}

// HasColumnAttribute checks if a column has a specific attribute
func (t *Table) HasColumnAttribute(colKey keys.ColKey, attr ColumnAttrMask) bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	spec, exists := t.columnSpecs[colKey]
	if !exists || spec.Attributes == nil {
		return false
	}

	return spec.Attributes.HasAttribute(attr)
}

// GetIndexedColumns returns all columns with indexed attribute
func (t *Table) GetIndexedColumns() []keys.ColKey {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var indexed []keys.ColKey
	for colKey, spec := range t.columnSpecs {
		if spec.Attributes != nil && spec.Attributes.IsIndexed() {
			indexed = append(indexed, colKey)
		}
	}

	return indexed
}

// GetUniqueColumns returns all columns with unique constraint
func (t *Table) GetUniqueColumns() []keys.ColKey {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var unique []keys.ColKey
	for colKey, spec := range t.columnSpecs {
		if spec.Attributes != nil && spec.Attributes.IsUnique() {
			unique = append(unique, colKey)
		}
	}

	return unique
}

// GetCollectionColumns returns all collection columns
func (t *Table) GetCollectionColumns() []keys.ColKey {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var collections []keys.ColKey
	for colKey, spec := range t.columnSpecs {
		// Check both attributes and data type for collection columns
		if (spec.Attributes != nil && spec.Attributes.IsCollection()) ||
			keys.IsCollectionType(spec.Type) {
			collections = append(collections, colKey)
		}
	}

	return collections
}

// GetAllObjectKeys returns all object keys in the table
func (t *Table) GetAllObjectKeys() ([]keys.ObjKey, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if !t.isOpen {
		return nil, fmt.Errorf("table is not open")
	}

	// Get root cluster
	rootCluster, err := t.clusterTree.GetRootCluster()
	if err != nil {
		return nil, fmt.Errorf("failed to get root cluster: %w", err)
	}

	// Get object count
	objCount := rootCluster.ObjectCount()
	if objCount == 0 {
		return []keys.ObjKey{}, nil
	}

	// Collect all object keys
	objKeys := make([]keys.ObjKey, 0, objCount)
	for i := 0; i < objCount; i++ {
		objKey, err := rootCluster.GetObjectKey(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get object key at index %d: %w", i, err)
		}
		objKeys = append(objKeys, objKey)
	}

	return objKeys, nil
}

// ... existing code ...
