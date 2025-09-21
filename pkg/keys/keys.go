package keys

import (
	"encoding/binary"
	"fmt"
)

// TableKey represents a table identifier
type TableKey uint32

// ColKey represents a column identifier with embedded type and nullability information
type ColKey uint64

// ObjKey represents an object identifier
type ObjKey uint64

// DataType enum values based on C++ implementation
// Values must match tur-core for file format compatibility
type DataType uint8

const (
	TypeInt       DataType = 0
	TypeBool      DataType = 1
	TypeString    DataType = 2
	TypeBinary    DataType = 4
	TypeMixed     DataType = 6
	TypeTimestamp DataType = 8
	TypeFloat     DataType = 9
	TypeDouble    DataType = 10
	TypeDecimal   DataType = 11
	TypeLink      DataType = 12
	TypeObjectID  DataType = 15
	TypeTypedLink DataType = 16
	TypeUUID      DataType = 17
	TypeList      DataType = 18
	TypeSet       DataType = 19
	TypeDict      DataType = 20
)

// Key generation and manipulation functions

// NewTableKey creates a new table key
func NewTableKey(value uint32) TableKey {
	return TableKey(value)
}

// NewColKey creates a new column key with type and nullability information
func NewColKey(dataType DataType, index uint32, nullable bool) ColKey {
	var key uint64 = uint64(index) & 0x00000000FFFFFFFF

	// Set type in bits 56-61
	key |= (uint64(dataType) & 0x3F) << 56

	// Set nullable flag in bit 63
	if nullable {
		key |= 0x8000000000000000
	}

	return ColKey(key)
}

// NewObjKey creates a new object key
func NewObjKey(value uint64) ObjKey {
	return ObjKey(value)
}

// ColKey methods

// IsNullable returns true if the column allows null values
func (ck ColKey) IsNullable() bool {
	return (ck & 0x8000000000000000) != 0
}

// GetType returns the data type of the column
func (ck ColKey) GetType() DataType {
	return DataType((ck >> 56) & 0x3F)
}

// GetIndex returns the column index
func (ck ColKey) GetIndex() uint32 {
	return uint32(ck & 0x00000000FFFFFFFF)
}

// GetBacklinkIndex returns the backlink index (bits 32-47)
func (ck ColKey) GetBacklinkIndex() uint16 {
	return uint16((ck >> 32) & 0xFFFF)
}

// SetBacklinkIndex sets the backlink index
func (ck ColKey) SetBacklinkIndex(index uint16) ColKey {
	// Clear existing backlink index and set new one
	cleared := ck & 0xFFFF0000FFFFFFFF
	return cleared | (ColKey(index) << 32)
}

// IsBacklink returns true if this is a backlink column
func (ck ColKey) IsBacklink() bool {
	return ck.GetBacklinkIndex() != 0
}

// ObjKey methods

// GetValue returns the raw value of the object key
func (ok ObjKey) GetValue() uint64 {
	return uint64(ok)
}

// IsUnresolved returns true if this is an unresolved object key
func (ok ObjKey) IsUnresolved() bool {
	return ok == 0
}

// TableKey methods

// GetValue returns the raw value of the table key
func (tk TableKey) GetValue() uint32 {
	return uint32(tk)
}

// IsValid returns true if this is a valid table key
func (tk TableKey) IsValid() bool {
	return tk != 0
}

// String methods for debugging

func (tk TableKey) String() string {
	return fmt.Sprintf("TableKey(%d)", uint32(tk))
}

func (ck ColKey) String() string {
	return fmt.Sprintf("ColKey(type:%v, index:%d, nullable:%v)",
		ck.GetType(), ck.GetIndex(), ck.IsNullable())
}

func (ok ObjKey) String() string {
	return fmt.Sprintf("ObjKey(%d)", uint64(ok))
}

// String returns the string representation of the data type
func (dt DataType) String() string {
	switch dt {
	case TypeInt:
		return "int"
	case TypeBool:
		return "bool"
	case TypeString:
		return "string"
	case TypeBinary:
		return "binary"
	case TypeMixed:
		return "mixed"
	case TypeTimestamp:
		return "timestamp"
	case TypeFloat:
		return "float"
	case TypeDouble:
		return "double"
	case TypeDecimal:
		return "decimal"
	case TypeLink:
		return "link"
	case TypeObjectID:
		return "objectid"
	case TypeTypedLink:
		return "typedlink"
	case TypeUUID:
		return "uuid"
	case TypeList:
		return "list"
	case TypeSet:
		return "set"
	case TypeDict:
		return "dict"
	default:
		return "unknown"
	}
}

// IsCollectionType returns true if the data type is a collection type
func IsCollectionType(dataType DataType) bool {
	return dataType == TypeList || dataType == TypeSet || dataType == TypeDict
}

// IsLinkType returns true if the data type is a link type
func IsLinkType(dataType DataType) bool {
	return dataType == TypeLink || dataType == TypeTypedLink
}

// Key generation functions

var (
	nextTableKey uint32 = 1
	nextObjKey   uint64 = 1
	nextColIndex uint32 = 1
)

// GenerateTableKey generates a new unique table key
func GenerateTableKey() TableKey {
	key := nextTableKey
	nextTableKey++
	return TableKey(key)
}

// GenerateObjKey generates a new unique object key
func GenerateObjKey() ObjKey {
	key := nextObjKey
	nextObjKey++
	return ObjKey(key)
}

// GenerateColKey generates a new unique column key with type and nullability
func GenerateColKey(dataType DataType, nullable bool) ColKey {
	index := nextColIndex
	nextColIndex++
	return NewColKey(dataType, index, nullable)
}

// Key encoding/decoding utilities for advanced features

// EncodeTableKey encodes a TableKey to bytes
func (tk TableKey) Encode() []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(tk))
	return buf
}

// DecodeTableKey decodes bytes to a TableKey
func DecodeTableKey(data []byte) (TableKey, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("insufficient data for TableKey: need 4 bytes, got %d", len(data))
	}
	return TableKey(binary.LittleEndian.Uint32(data)), nil
}

// EncodeColKey encodes a ColKey to bytes
func (ck ColKey) Encode() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(ck))
	return buf
}

// DecodeColKey decodes bytes to a ColKey
func DecodeColKey(data []byte) (ColKey, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data for ColKey: need 8 bytes, got %d", len(data))
	}
	return ColKey(binary.LittleEndian.Uint64(data)), nil
}

// EncodeObjKey encodes an ObjKey to bytes
func (ok ObjKey) Encode() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(ok))
	return buf
}

// DecodeObjKey decodes bytes to an ObjKey
func DecodeObjKey(data []byte) (ObjKey, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data for ObjKey: need 8 bytes, got %d", len(data))
	}
	return ObjKey(binary.LittleEndian.Uint64(data)), nil
}

// KeyPair represents a key-value pair for advanced key operations
type KeyPair struct {
	Key   interface{} // Can be TableKey, ColKey, or ObjKey
	Value interface{} // Associated value
}

// KeyRange represents a range of keys for batch operations
type KeyRange struct {
	Start interface{} // Starting key
	End   interface{} // Ending key
	Type  string      // Key type: "table", "column", or "object"
}

// ValidateKeyRange validates that a key range is properly formed
func ValidateKeyRange(kr KeyRange) error {
	if kr.Start == nil || kr.End == nil {
		return fmt.Errorf("key range start and end cannot be nil")
	}

	switch kr.Type {
	case "table":
		start, ok1 := kr.Start.(TableKey)
		end, ok2 := kr.End.(TableKey)
		if !ok1 || !ok2 {
			return fmt.Errorf("table key range must contain TableKey types")
		}
		if start > end {
			return fmt.Errorf("table key range start (%d) cannot be greater than end (%d)", start, end)
		}
	case "column":
		start, ok1 := kr.Start.(ColKey)
		end, ok2 := kr.End.(ColKey)
		if !ok1 || !ok2 {
			return fmt.Errorf("column key range must contain ColKey types")
		}
		if start > end {
			return fmt.Errorf("column key range start (%d) cannot be greater than end (%d)", start, end)
		}
	case "object":
		start, ok1 := kr.Start.(ObjKey)
		end, ok2 := kr.End.(ObjKey)
		if !ok1 || !ok2 {
			return fmt.Errorf("object key range must contain ObjKey types")
		}
		if start > end {
			return fmt.Errorf("object key range start (%d) cannot be greater than end (%d)", start, end)
		}
	default:
		return fmt.Errorf("invalid key range type: %s (must be 'table', 'column', or 'object')", kr.Type)
	}

	return nil
}
