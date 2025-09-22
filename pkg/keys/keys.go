package keys

import (
	"encoding/binary"
	"fmt"
)

// TableKey represents a table identifier
type TableKey uint32

// ColKey represents a column key with embedded index, type, attributes, and tag
// Bit layout: [tag:34][attrs:8][type:6][index:16]
type ColKey int64

// ObjKey represents an object identifier
type ObjKey uint64

// DataType enum values
type DataType uint8

const (
	// Primitive types
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

	// Collection types
	TypeList DataType = 19
	TypeSet  DataType = 20
	TypeDict DataType = 21

	// Deprecated types (for migration compatibility only)
	TypeOldTable    DataType = 5
	TypeOldDateTime DataType = 7
)

// Key generation and manipulation functions

// NewTableKey creates a new table key
func NewTableKey(value uint32) TableKey {
	return TableKey(value)
}

const (
	// ColKey constants
	ColKeyNullValue int64 = 0x7FFFFFFFFFFFFFFF // Free top bit
)

// NewColKey creates a new column key with the specified parameters
func NewColKey(index uint32, dataType DataType, attrs ColumnAttrMask, tag uint32) ColKey {
	// Bit layout: [tag:34][attrs:8][type:6][index:16]
	return ColKey((int64(index) & 0xFFFF) |
		((int64(dataType) & 0x3F) << 16) |
		((int64(attrs.Value()) & 0xFF) << 22) |
		((int64(tag) & 0xFFFFFFFF) << 30))
}

// NewObjKey creates a new object key
func NewObjKey(value uint64) ObjKey {
	return ObjKey(value)
}

// ColKey methods

// IsNullable returns true if the column allows null values
func (ck ColKey) IsNullable() bool {
	return ck.GetAttrs().Test(ColAttrNullable)
}

// IsList returns true if the column is a list collection
func (ck ColKey) IsList() bool {
	return ck.GetAttrs().Test(ColAttrList)
}

// IsSet returns true if the column is a set collection
func (ck ColKey) IsSet() bool {
	return ck.GetAttrs().Test(ColAttrSet)
}

// IsDictionary returns true if the column is a dictionary collection
func (ck ColKey) IsDictionary() bool {
	return ck.GetAttrs().Test(ColAttrDictionary)
}

// IsCollection returns true if the column is any type of collection
func (ck ColKey) IsCollection() bool {
	return ck.GetAttrs().Test(ColAttrCollection)
}

// GetType returns the data type of the column
func (ck ColKey) GetType() DataType {
	return DataType((ck >> 16) & 0x3F)
}

// GetIndex returns the column index
func (ck ColKey) GetIndex() uint32 {
	return uint32(ck & 0xFFFF)
}

// GetAttrs returns the column attributes
func (ck ColKey) GetAttrs() ColumnAttrMask {
	return NewColumnAttrMaskWithValue(int((ck >> 22) & 0xFF))
}

// GetTag returns the column tag
func (ck ColKey) GetTag() uint32 {
	return uint32((ck >> 30) & 0xFFFFFFFF)
}

// IsValid returns true if the ColKey is valid (not null)
func (ck ColKey) IsValid() bool {
	return ck != ColKey(ColKeyNullValue)
}

// Equals returns true if two ColKeys are equal
func (ck ColKey) Equals(other ColKey) bool {
	return ck == other
}

// Compare returns -1, 0, or 1 if ck is less than, equal to, or greater than other
func (ck ColKey) Compare(other ColKey) int {
	if ck < other {
		return -1
	} else if ck > other {
		return 1
	}
	return 0
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

// Compare returns -1, 0, or 1 if ok is less than, equal to, or greater than other
func (ok ObjKey) Compare(other ObjKey) int {
	if ok < other {
		return -1
	} else if ok > other {
		return 1
	}
	return 0
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
		return "Int"
	case TypeBool:
		return "Bool"
	case TypeString:
		return "String"
	case TypeBinary:
		return "Binary"
	case TypeMixed:
		return "Mixed"
	case TypeTimestamp:
		return "Timestamp"
	case TypeFloat:
		return "Float"
	case TypeDouble:
		return "Double"
	case TypeDecimal:
		return "Decimal"
	case TypeLink:
		return "Link"
	case TypeObjectID:
		return "ObjectID"
	case TypeTypedLink:
		return "TypedLink"
	case TypeUUID:
		return "UUID"
	case TypeOldTable:
		return "OldTable"
	case TypeOldDateTime:
		return "OldDateTime"
	default:
		return "Unknown"
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

// IsValidType returns true if the data type is valid (not deprecated)
func IsValidType(dataType DataType) bool {
	switch dataType {
	case TypeInt, TypeBool, TypeString, TypeBinary, TypeMixed, TypeTimestamp,
		TypeFloat, TypeDouble, TypeDecimal, TypeLink, TypeObjectID,
		TypeTypedLink, TypeUUID, TypeList, TypeSet, TypeDict:
		return true
	case TypeOldTable, TypeOldDateTime:
		return false // Deprecated types
	default:
		return false
	}
}

// Key generation functions

var (
	nextTableKey uint32 = 1
	nextObjKey   uint64 = 1
	nextColIndex uint32 = 1
)

// GenerateTableKey generates a new unique table key
func GenerateTableKey() TableKey {
	key := NewTableKey(nextTableKey)
	nextTableKey++
	return key
}

// GenerateObjKey generates a new unique object key
func GenerateObjKey() ObjKey {
	key := NewObjKey(nextObjKey)
	nextObjKey++
	return key
}

// GenerateColKey generates a new unique column key with attributes
func GenerateColKey(dataType DataType, attrs ColumnAttrMask) ColKey {
	key := NewColKey(nextColIndex, dataType, attrs, 0)
	nextColIndex++
	return key
}

// GenerateColKeySimple generates a new unique column key with nullable flag
func GenerateColKeySimple(dataType DataType, nullable bool) ColKey {
	attrs := NewColumnAttrMask()
	if nullable {
		attrs.Set(ColAttrNullable)
	}
	return GenerateColKey(dataType, attrs)
}

// Key encoding/decoding functions

// Encode methods
func (tk TableKey) Encode() []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(tk))
	return buf
}

func DecodeTableKey(data []byte) (TableKey, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("insufficient data for TableKey")
	}
	return TableKey(binary.LittleEndian.Uint32(data)), nil
}

func (ck ColKey) Encode() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(ck))
	return buf
}

func DecodeColKey(data []byte) (ColKey, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data for ColKey")
	}
	return ColKey(binary.LittleEndian.Uint64(data)), nil
}

func (ok ObjKey) Encode() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(ok))
	return buf
}

func DecodeObjKey(data []byte) (ObjKey, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data for ObjKey")
	}
	return ObjKey(binary.LittleEndian.Uint64(data)), nil
}

// KeyPair represents a key-value pair
type KeyPair struct {
	Key   interface{} // Can be TableKey, ColKey, or ObjKey
	Value interface{} // Associated value
}

// KeyRange represents a range of keys
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
			return fmt.Errorf("invalid table key types in range")
		}
		if start > end {
			return fmt.Errorf("start key cannot be greater than end key")
		}
	case "column":
		start, ok1 := kr.Start.(ColKey)
		end, ok2 := kr.End.(ColKey)
		if !ok1 || !ok2 {
			return fmt.Errorf("invalid column key types in range")
		}
		if start > end {
			return fmt.Errorf("start key cannot be greater than end key")
		}
	case "object":
		start, ok1 := kr.Start.(ObjKey)
		end, ok2 := kr.End.(ObjKey)
		if !ok1 || !ok2 {
			return fmt.Errorf("invalid object key types in range")
		}
		if start > end {
			return fmt.Errorf("start key cannot be greater than end key")
		}
	default:
		return fmt.Errorf("unsupported key range type: %s", kr.Type)
	}

	return nil
}
