package core

import (
	"fmt"
	"strconv"
	"time"

	"github.com/turdb/tur/pkg/keys"
)

// DataType represents the supported data types in the database
type DataType = keys.DataType

// Type constants for convenience
const (
	TypeInt    = keys.TypeInt
	TypeBool   = keys.TypeBool
	TypeString = keys.TypeString
	TypeFloat  = keys.TypeFloat
	TypeDouble = keys.TypeDouble
)

// Additional collection type constants
const (
	TypeList = keys.TypeList
	TypeSet  = keys.TypeSet
	TypeDict = keys.TypeDict
)

// CollectionType represents the type of collection
type CollectionType int

const (
	CollectionTypeList CollectionType = iota
	CollectionTypeSet
	CollectionTypeDictionary
)

// CollectionInfo holds metadata about collection types
type CollectionInfo struct {
	Type        CollectionType
	ElementType DataType
	KeyType     DataType // For dictionaries
	Name        string
}

// GetCollectionInfo returns information about a collection type
func GetCollectionInfo(collectionType CollectionType, elementType DataType, keyType DataType) CollectionInfo {
	switch collectionType {
	case CollectionTypeList:
		return CollectionInfo{
			Type:        CollectionTypeList,
			ElementType: elementType,
			Name:        "List",
		}
	case CollectionTypeSet:
		return CollectionInfo{
			Type:        CollectionTypeSet,
			ElementType: elementType,
			Name:        "Set",
		}
	case CollectionTypeDictionary:
		return CollectionInfo{
			Type:        CollectionTypeDictionary,
			ElementType: elementType,
			KeyType:     keyType,
			Name:        "Dictionary",
		}
	default:
		return CollectionInfo{Name: "Unknown"}
	}
}

// IsCollectionType returns true if the data type is a collection type
func IsCollectionType(dataType DataType) bool {
	return dataType == TypeList || dataType == TypeSet || dataType == TypeDict
}

// TypeInfo contains metadata about a data type
type TypeInfo struct {
	Type     DataType
	Size     int // Size in bytes (0 for variable length)
	Name     string
	Nullable bool
}

// GetTypeInfo returns information about a data type
func GetTypeInfo(dataType DataType) TypeInfo {
	switch dataType {
	case TypeInt:
		return TypeInfo{
			Type: TypeInt,
			Size: 8,
			Name: "INTEGER",
		}
	case TypeBool:
		return TypeInfo{
			Type: TypeBool,
			Size: 1,
			Name: "BOOLEAN",
		}
	case TypeString:
		return TypeInfo{
			Type: TypeString,
			Size: 0, // Variable length
			Name: "STRING",
		}
	case TypeFloat:
		return TypeInfo{
			Type: TypeFloat,
			Size: 4,
			Name: "FLOAT",
		}
	case TypeDouble:
		return TypeInfo{
			Type: TypeDouble,
			Size: 8,
			Name: "DOUBLE",
		}
	default:
		return TypeInfo{
			Type: dataType,
			Size: 0,
			Name: "UNKNOWN",
		}
	}
}

// IsFixedSize returns true if the data type has a fixed size
func IsFixedSize(dataType DataType) bool {
	info := GetTypeInfo(dataType)
	return info.Size > 0
}

// GetTypeSize returns the size in bytes for a data type
func GetTypeSize(dataType DataType) int {
	return GetTypeInfo(dataType).Size
}

// TypeConverter handles type conversions and validations
type TypeConverter struct{}

// NewTypeConverter creates a new type converter
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{}
}

// ConvertToType converts a value to the specified data type
func (tc *TypeConverter) ConvertToType(value interface{}, targetType DataType) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch targetType {
	case TypeInt:
		return tc.convertToInt(value)
	case TypeBool:
		return tc.convertToBool(value)
	case TypeString:
		return tc.convertToString(value)
	case TypeFloat:
		return tc.convertToFloat(value)
	case TypeDouble:
		return tc.convertToDouble(value)
	default:
		return nil, fmt.Errorf("unsupported target type: %v", targetType)
	}
}

// convertToInt converts value to int64
func (tc *TypeConverter) convertToInt(value interface{}) (int64, error) {
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
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed, nil
		}
		return 0, fmt.Errorf("cannot convert string '%s' to integer", v)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to integer", value)
	}
}

// convertToBool converts value to bool
func (tc *TypeConverter) convertToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		return v != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		return v != 0, nil
	case float32:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case string:
		if parsed, err := strconv.ParseBool(v); err == nil {
			return parsed, nil
		}
		// Try parsing as number
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed != 0, nil
		}
		return false, fmt.Errorf("cannot convert string '%s' to boolean", v)
	default:
		return false, fmt.Errorf("cannot convert %T to boolean", value)
	}
}

// convertToString converts value to string
func (tc *TypeConverter) convertToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v), nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32:
		return fmt.Sprintf("%g", v), nil
	case float64:
		return fmt.Sprintf("%g", v), nil
	case bool:
		return strconv.FormatBool(v), nil
	case time.Time:
		return v.Format(time.RFC3339), nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

// convertToFloat converts value to float32
func (tc *TypeConverter) convertToFloat(value interface{}) (float32, error) {
	switch v := value.(type) {
	case float32:
		return v, nil
	case float64:
		return float32(v), nil
	case int, int8, int16, int32, int64:
		return float32(v.(int64)), nil
	case uint, uint8, uint16, uint32, uint64:
		return float32(v.(uint64)), nil
	case string:
		if parsed, err := strconv.ParseFloat(v, 32); err == nil {
			return float32(parsed), nil
		}
		return 0, fmt.Errorf("cannot convert string '%s' to float", v)
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float", value)
	}
}

// convertToDouble converts value to float64
func (tc *TypeConverter) convertToDouble(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int, int8, int16, int32, int64:
		return float64(v.(int64)), nil
	case uint, uint8, uint16, uint32, uint64:
		return float64(v.(uint64)), nil
	case string:
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed, nil
		}
		return 0, fmt.Errorf("cannot convert string '%s' to double", v)
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to double", value)
	}
}

// ValidateType validates if a value is compatible with a data type
func (tc *TypeConverter) ValidateType(value interface{}, dataType DataType) error {
	if value == nil {
		return nil // Null values are handled separately
	}

	switch dataType {
	case TypeInt:
		_, err := tc.convertToInt(value)
		return err
	case TypeBool:
		_, err := tc.convertToBool(value)
		return err
	case TypeString:
		_, err := tc.convertToString(value)
		return err
	case TypeFloat:
		_, err := tc.convertToFloat(value)
		return err
	case TypeDouble:
		_, err := tc.convertToDouble(value)
		return err
	default:
		return fmt.Errorf("unsupported data type: %v", dataType)
	}
}

// GetDefaultValue returns the default value for a data type
func GetDefaultValue(dataType DataType) interface{} {
	switch dataType {
	case TypeInt:
		return int64(0)
	case TypeBool:
		return false
	case TypeString:
		return ""
	case TypeFloat:
		return float32(0.0)
	case TypeDouble:
		return float64(0.0)
	default:
		return nil
	}
}

// IsNumericType returns true if the data type is numeric
func IsNumericType(dataType DataType) bool {
	switch dataType {
	case TypeInt, TypeFloat, TypeDouble:
		return true
	default:
		return false
	}
}

// CompareValues compares two values of the same type
func CompareValues(a, b interface{}, dataType DataType) (int, error) {
	if a == nil && b == nil {
		return 0, nil
	}
	if a == nil {
		return -1, nil
	}
	if b == nil {
		return 1, nil
	}

	converter := NewTypeConverter()

	// Convert both values to the target type
	valA, err := converter.ConvertToType(a, dataType)
	if err != nil {
		return 0, fmt.Errorf("failed to convert first value: %w", err)
	}

	valB, err := converter.ConvertToType(b, dataType)
	if err != nil {
		return 0, fmt.Errorf("failed to convert second value: %w", err)
	}

	switch dataType {
	case TypeInt:
		aInt := valA.(int64)
		bInt := valB.(int64)
		if aInt < bInt {
			return -1, nil
		} else if aInt > bInt {
			return 1, nil
		}
		return 0, nil
	case TypeBool:
		aBool := valA.(bool)
		bBool := valB.(bool)
		if !aBool && bBool {
			return -1, nil
		} else if aBool && !bBool {
			return 1, nil
		}
		return 0, nil
	case TypeString:
		aStr := valA.(string)
		bStr := valB.(string)
		if aStr < bStr {
			return -1, nil
		} else if aStr > bStr {
			return 1, nil
		}
		return 0, nil
	case TypeFloat:
		aFloat := valA.(float32)
		bFloat := valB.(float32)
		if aFloat < bFloat {
			return -1, nil
		} else if aFloat > bFloat {
			return 1, nil
		}
		return 0, nil
	case TypeDouble:
		aDouble := valA.(float64)
		bDouble := valB.(float64)
		if aDouble < bDouble {
			return -1, nil
		} else if aDouble > bDouble {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported data type for comparison: %v", dataType)
	}
}
