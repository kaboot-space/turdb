package core

import (
	"fmt"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// Operation represents a database operation type
type Operation int

const (
	OpInsert Operation = iota
	OpUpdate
	OpDelete
	OpSelect
)

// OperationResult represents the result of a database operation
type OperationResult struct {
	Success      bool
	RowsAffected int64
	Data         []map[string]interface{}
	Error        error
}

// InsertOperation represents an insert operation
type InsertOperation struct {
	TableName string
	Values    map[string]interface{}
}

// UpdateOperation represents an update operation
type UpdateOperation struct {
	TableName string
	Values    map[string]interface{}
	Where     map[string]interface{}
}

// DeleteOperation represents a delete operation
type DeleteOperation struct {
	TableName string
	Where     map[string]interface{}
}

// SelectOperation represents a select operation
type SelectOperation struct {
	TableName string
	Columns   []string
	Where     map[string]interface{}
	Limit     int64
	Offset    int64
}

// OperationExecutor handles execution of database operations
type OperationExecutor struct {
	allocator  *Allocator
	fileFormat *storage.FileFormat
}

// NewOperationExecutor creates a new operation executor
func NewOperationExecutor(allocator *Allocator, fileFormat *storage.FileFormat) *OperationExecutor {
	return &OperationExecutor{
		allocator:  allocator,
		fileFormat: fileFormat,
	}
}

// ExecuteInsert executes an insert operation
func (oe *OperationExecutor) ExecuteInsert(op *InsertOperation) *OperationResult {
	result := &OperationResult{
		Success:      false,
		RowsAffected: 0,
		Data:         nil,
	}

	// Validate operation
	if op.TableName == "" {
		result.Error = fmt.Errorf("table name is required")
		return result
	}

	if len(op.Values) == 0 {
		result.Error = fmt.Errorf("values are required for insert")
		return result
	}

	// TODO: Implement actual insert logic
	// This would involve:
	// 1. Validate table exists
	// 2. Validate column types
	// 3. Insert into appropriate cluster/column arrays
	// 4. Update indexes

	result.Success = true
	result.RowsAffected = 1
	return result
}

// ExecuteUpdate executes an update operation
func (oe *OperationExecutor) ExecuteUpdate(op *UpdateOperation) *OperationResult {
	result := &OperationResult{
		Success:      false,
		RowsAffected: 0,
		Data:         nil,
	}

	// Validate operation
	if op.TableName == "" {
		result.Error = fmt.Errorf("table name is required")
		return result
	}

	if len(op.Values) == 0 {
		result.Error = fmt.Errorf("values are required for update")
		return result
	}

	// TODO: Implement actual update logic
	// This would involve:
	// 1. Find matching rows using where clause
	// 2. Update values in column arrays
	// 3. Update indexes if needed

	result.Success = true
	result.RowsAffected = 0 // Would be actual count
	return result
}

// ExecuteDelete executes a delete operation
func (oe *OperationExecutor) ExecuteDelete(op *DeleteOperation) *OperationResult {
	result := &OperationResult{
		Success:      false,
		RowsAffected: 0,
		Data:         nil,
	}

	// Validate operation
	if op.TableName == "" {
		result.Error = fmt.Errorf("table name is required")
		return result
	}

	// TODO: Implement actual delete logic
	// This would involve:
	// 1. Find matching rows using where clause
	// 2. Mark rows as deleted or remove from column arrays
	// 3. Update indexes

	result.Success = true
	result.RowsAffected = 0 // Would be actual count
	return result
}

// ExecuteSelect executes a select operation
func (oe *OperationExecutor) ExecuteSelect(op *SelectOperation) *OperationResult {
	result := &OperationResult{
		Success:      false,
		RowsAffected: 0,
		Data:         make([]map[string]interface{}, 0),
	}

	// Validate operation
	if op.TableName == "" {
		result.Error = fmt.Errorf("table name is required")
		return result
	}

	// TODO: Implement actual select logic
	// This would involve:
	// 1. Find matching rows using where clause
	// 2. Project requested columns
	// 3. Apply limit and offset
	// 4. Return result set

	result.Success = true
	result.RowsAffected = int64(len(result.Data))
	return result
}

// ValidateValue validates a value against a column type
func ValidateValue(value interface{}, colKey keys.ColKey) error {
	if value == nil {
		if !colKey.IsNullable() {
			return fmt.Errorf("null value not allowed for non-nullable column")
		}
		return nil
	}

	dataType := colKey.GetType()

	switch dataType {
	case keys.TypeInt:
		switch value.(type) {
		case int, int8, int16, int32, int64:
			return nil
		default:
			return fmt.Errorf("expected integer value, got %T", value)
		}
	case keys.TypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean value, got %T", value)
		}
	case keys.TypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string value, got %T", value)
		}
	case keys.TypeFloat:
		switch value.(type) {
		case float32, float64:
			return nil
		default:
			return fmt.Errorf("expected float value, got %T", value)
		}
	case keys.TypeDouble:
		if _, ok := value.(float64); !ok {
			return fmt.Errorf("expected double value, got %T", value)
		}
	default:
		return fmt.Errorf("unsupported data type: %v", dataType)
	}

	return nil
}

// ConvertValue converts a value to the appropriate type for a column
func ConvertValue(value interface{}, colKey keys.ColKey) (interface{}, error) {
	if value == nil {
		if !colKey.IsNullable() {
			return nil, fmt.Errorf("null value not allowed for non-nullable column")
		}
		return nil, nil
	}

	dataType := colKey.GetType()

	switch dataType {
	case keys.TypeInt:
		switch v := value.(type) {
		case int:
			return int64(v), nil
		case int8:
			return int64(v), nil
		case int16:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		case float32:
			return int64(v), nil
		case float64:
			return int64(v), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to integer", value)
		}
	case keys.TypeBool:
		if v, ok := value.(bool); ok {
			return v, nil
		}
		return nil, fmt.Errorf("cannot convert %T to boolean", value)
	case keys.TypeString:
		if v, ok := value.(string); ok {
			return v, nil
		}
		return fmt.Sprintf("%v", value), nil
	case keys.TypeFloat:
		switch v := value.(type) {
		case float32:
			return v, nil
		case float64:
			return float32(v), nil
		case int:
			return float32(v), nil
		case int64:
			return float32(v), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to float", value)
		}
	case keys.TypeDouble:
		switch v := value.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to double", value)
		}
	default:
		return nil, fmt.Errorf("unsupported data type: %v", dataType)
	}
}
