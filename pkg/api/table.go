package api

import (
	"fmt"

	"github.com/turdb/tur/pkg/keys"
)

// Table represents a database table
type Table struct {
	key        keys.TableKey
	name       string
	columns    map[string]keys.ColKey
	tur        *Tur
	nextColKey uint32
	nextObjKey uint64
}

// Object represents a database object/row
type Object struct {
	key   keys.ObjKey
	table *Table
}

// GetKey returns the table key
func (t *Table) GetKey() keys.TableKey {
	return t.key
}

// GetName returns the table name
func (t *Table) GetName() string {
	return t.name
}

// AddColumn adds a new column to the table
func (t *Table) AddColumn(name string, dataType keys.DataType, nullable bool) (keys.ColKey, error) {
	if !t.tur.isOpen {
		return 0, fmt.Errorf("tur is not open")
	}

	// Check if column already exists
	if _, exists := t.columns[name]; exists {
		return 0, fmt.Errorf("column '%s' already exists", name)
	}

	// Create column key
	var attrs keys.ColumnAttrMask
	if nullable {
		attrs.Set(keys.ColAttrNullable)
	}
	colKey := keys.NewColKey(t.nextColKey, dataType, attrs, 0)
	t.nextColKey++

	// Add to table's column map
	t.columns[name] = colKey

	// Add column to cluster tree
	if err := t.tur.clusterTree.AddColumn(colKey); err != nil {
		return 0, fmt.Errorf("failed to add column to cluster: %w", err)
	}

	return colKey, nil
}

// GetColumn returns the column key for a given column name
func (t *Table) GetColumn(name string) (keys.ColKey, error) {
	colKey, exists := t.columns[name]
	if !exists {
		return 0, fmt.Errorf("column '%s' not found", name)
	}
	return colKey, nil
}

// GetColumns returns all column names
func (t *Table) GetColumns() []string {
	columns := make([]string, 0, len(t.columns))
	for name := range t.columns {
		columns = append(columns, name)
	}
	return columns
}

// CreateObject creates a new object in the table
func (t *Table) CreateObject() (*Object, error) {
	if !t.tur.isOpen {
		return nil, fmt.Errorf("tur is not open")
	}

	// Generate object key
	objKey := keys.NewObjKey(t.nextObjKey)
	t.nextObjKey++

	// Insert object into cluster tree
	if err := t.tur.clusterTree.InsertObject(objKey); err != nil {
		return nil, fmt.Errorf("failed to insert object: %w", err)
	}

	return &Object{
		key:   objKey,
		table: t,
	}, nil
}

// GetObject retrieves an object by key
func (t *Table) GetObject(objKey keys.ObjKey) (*Object, error) {
	if !t.tur.isOpen {
		return nil, fmt.Errorf("tur is not open")
	}

	// Check if object exists in cluster tree
	cluster, index := t.tur.clusterTree.FindObject(objKey)
	if cluster == nil || index < 0 {
		return nil, fmt.Errorf("object not found")
	}

	return &Object{
		key:   objKey,
		table: t,
	}, nil
}

// Object methods

// GetKey returns the object key
func (o *Object) GetKey() keys.ObjKey {
	return o.key
}

// GetTable returns the table this object belongs to
func (o *Object) GetTable() *Table {
	return o.table
}

// Set sets a value for the given column
func (o *Object) Set(columnName string, value interface{}) error {
	if !o.table.tur.isOpen {
		return fmt.Errorf("tur is not open")
	}

	// Get column key
	colKey, err := o.table.GetColumn(columnName)
	if err != nil {
		return err
	}

	// Validate value type
	if err := o.validateValue(colKey, value); err != nil {
		return err
	}

	// Set value in cluster tree
	return o.table.tur.clusterTree.SetValue(o.key, colKey, value)
}

// Get retrieves a value for the given column
func (o *Object) Get(columnName string) (interface{}, error) {
	if !o.table.tur.isOpen {
		return nil, fmt.Errorf("tur is not open")
	}

	// Get column key
	colKey, err := o.table.GetColumn(columnName)
	if err != nil {
		return nil, err
	}

	// Get value from cluster tree
	return o.table.tur.clusterTree.GetValue(o.key, colKey)
}

// GetString retrieves a string value
func (o *Object) GetString(columnName string) (string, error) {
	value, err := o.Get(columnName)
	if err != nil {
		return "", err
	}

	if value == nil {
		return "", nil
	}

	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("column '%s' is not a string", columnName)
	}

	return str, nil
}

// GetInt retrieves an integer value
func (o *Object) GetInt(columnName string) (int64, error) {
	value, err := o.Get(columnName)
	if err != nil {
		return 0, err
	}

	if value == nil {
		return 0, nil
	}

	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("column '%s' is not an integer", columnName)
	}
}

// GetBool retrieves a boolean value
func (o *Object) GetBool(columnName string) (bool, error) {
	value, err := o.Get(columnName)
	if err != nil {
		return false, err
	}

	if value == nil {
		return false, nil
	}

	b, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("column '%s' is not a boolean", columnName)
	}

	return b, nil
}

// GetFloat retrieves a float value
func (o *Object) GetFloat(columnName string) (float64, error) {
	value, err := o.Get(columnName)
	if err != nil {
		return 0, err
	}

	if value == nil {
		return 0, nil
	}

	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("column '%s' is not a float", columnName)
	}
}

// IsNull checks if a column value is null
func (o *Object) IsNull(columnName string) (bool, error) {
	value, err := o.Get(columnName)
	if err != nil {
		return false, err
	}

	return value == nil, nil
}

// validateValue validates that a value is compatible with the column type
func (o *Object) validateValue(colKey keys.ColKey, value interface{}) error {
	if value == nil {
		if !colKey.IsNullable() {
			return fmt.Errorf("column is not nullable")
		}
		return nil
	}

	dataType := colKey.GetType()

	switch dataType {
	case keys.TypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string value")
		}
	case keys.TypeInt:
		switch value.(type) {
		case int, int64, uint64:
			// OK
		default:
			return fmt.Errorf("expected integer value")
		}
	case keys.TypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean value")
		}
	case keys.TypeFloat:
		if _, ok := value.(float32); !ok {
			return fmt.Errorf("expected float32 value")
		}
	case keys.TypeDouble:
		switch value.(type) {
		case float64, float32:
			// OK
		default:
			return fmt.Errorf("expected float value")
		}
	case keys.TypeLink:
		if _, ok := value.(keys.ObjKey); !ok {
			return fmt.Errorf("expected ObjKey value")
		}
	case keys.TypeTypedLink:
		if _, ok := value.(keys.TypedLink); !ok {
			return fmt.Errorf("expected TypedLink value")
		}
	default:
		return fmt.Errorf("unsupported data type: %v", dataType)
	}

	return nil
}
