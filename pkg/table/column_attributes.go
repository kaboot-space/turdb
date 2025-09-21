package table

import (
	"fmt"

	"github.com/turdb/tur/pkg/keys"
)

// ColumnAttrMask represents column attributes bitmask following tur-core pattern
type ColumnAttrMask uint64

const (
	// Column attribute flags
	ColAttrNone        ColumnAttrMask = 0
	ColAttrIndexed     ColumnAttrMask = 1 << 0  // Column is indexed
	ColAttrUnique      ColumnAttrMask = 1 << 1  // Column has unique constraint
	ColAttrPrimaryKey  ColumnAttrMask = 1 << 2  // Column is primary key
	ColAttrNullable    ColumnAttrMask = 1 << 3  // Column allows null values
	ColAttrAutoIncr    ColumnAttrMask = 1 << 4  // Column is auto-increment
	ColAttrList        ColumnAttrMask = 1 << 5  // Column is a list
	ColAttrSet         ColumnAttrMask = 1 << 6  // Column is a set
	ColAttrDict        ColumnAttrMask = 1 << 7  // Column is a dictionary
	ColAttrBacklink    ColumnAttrMask = 1 << 8  // Column is a backlink
	ColAttrStrongLinks ColumnAttrMask = 1 << 9  // Column has strong links
	ColAttrWeakLinks   ColumnAttrMask = 1 << 10 // Column has weak links
	ColAttrFullText    ColumnAttrMask = 1 << 11 // Column supports full-text search
	ColAttrEncrypted   ColumnAttrMask = 1 << 12 // Column is encrypted
)

// ColumnAttributes holds column attribute information
type ColumnAttributes struct {
	mask        ColumnAttrMask
	elementType keys.DataType // For collections
	keyType     keys.DataType // For dictionaries
	targetTable keys.TableKey // For links
}

// NewColumnAttributes creates new column attributes
func NewColumnAttributes(mask ColumnAttrMask) *ColumnAttributes {
	return &ColumnAttributes{
		mask: mask,
	}
}

// HasAttribute checks if the column has a specific attribute
func (ca *ColumnAttributes) HasAttribute(attr ColumnAttrMask) bool {
	return (ca.mask & attr) != 0
}

// SetAttribute sets a specific attribute
func (ca *ColumnAttributes) SetAttribute(attr ColumnAttrMask) {
	ca.mask |= attr
}

// ClearAttribute clears a specific attribute
func (ca *ColumnAttributes) ClearAttribute(attr ColumnAttrMask) {
	ca.mask &^= attr
}

// GetMask returns the attribute mask
func (ca *ColumnAttributes) GetMask() ColumnAttrMask {
	return ca.mask
}

// IsIndexed returns true if column is indexed
func (ca *ColumnAttributes) IsIndexed() bool {
	return ca.HasAttribute(ColAttrIndexed)
}

// IsUnique returns true if column has unique constraint
func (ca *ColumnAttributes) IsUnique() bool {
	return ca.HasAttribute(ColAttrUnique)
}

// IsPrimaryKey returns true if column is primary key
func (ca *ColumnAttributes) IsPrimaryKey() bool {
	return ca.HasAttribute(ColAttrPrimaryKey)
}

// IsNullable returns true if column allows null values
func (ca *ColumnAttributes) IsNullable() bool {
	return ca.HasAttribute(ColAttrNullable)
}

// IsCollection returns true if column is a collection type
func (ca *ColumnAttributes) IsCollection() bool {
	return ca.HasAttribute(ColAttrList) || ca.HasAttribute(ColAttrSet) || ca.HasAttribute(ColAttrDict)
}

// IsList returns true if column is a list
func (ca *ColumnAttributes) IsList() bool {
	return ca.HasAttribute(ColAttrList)
}

// IsSet returns true if column is a set
func (ca *ColumnAttributes) IsSet() bool {
	return ca.HasAttribute(ColAttrSet)
}

// IsDictionary returns true if column is a dictionary
func (ca *ColumnAttributes) IsDictionary() bool {
	return ca.HasAttribute(ColAttrDict)
}

// IsBacklink returns true if column is a backlink
func (ca *ColumnAttributes) IsBacklink() bool {
	return ca.HasAttribute(ColAttrBacklink)
}

// SetElementType sets the element type for collections
func (ca *ColumnAttributes) SetElementType(elementType keys.DataType) {
	ca.elementType = elementType
}

// GetElementType returns the element type for collections
func (ca *ColumnAttributes) GetElementType() keys.DataType {
	return ca.elementType
}

// SetKeyType sets the key type for dictionaries
func (ca *ColumnAttributes) SetKeyType(keyType keys.DataType) {
	ca.keyType = keyType
}

// GetKeyType returns the key type for dictionaries
func (ca *ColumnAttributes) GetKeyType() keys.DataType {
	return ca.keyType
}

// SetTargetTable sets the target table for links
func (ca *ColumnAttributes) SetTargetTable(tableKey keys.TableKey) {
	ca.targetTable = tableKey
}

// GetTargetTable returns the target table for links
func (ca *ColumnAttributes) GetTargetTable() keys.TableKey {
	return ca.targetTable
}

// ValidateAttributes validates the attribute combination
func (ca *ColumnAttributes) ValidateAttributes() error {
	// Primary key cannot be nullable
	if ca.IsPrimaryKey() && ca.IsNullable() {
		return fmt.Errorf("primary key column cannot be nullable")
	}

	// Only one collection type can be set
	collectionCount := 0
	if ca.IsList() {
		collectionCount++
	}
	if ca.IsSet() {
		collectionCount++
	}
	if ca.IsDictionary() {
		collectionCount++
	}
	if collectionCount > 1 {
		return fmt.Errorf("column cannot have multiple collection types")
	}

	// Dictionary must have key type set
	if ca.IsDictionary() && ca.keyType == 0 {
		return fmt.Errorf("dictionary column must have key type set")
	}

	// Collections must have element type set
	if ca.IsCollection() && ca.elementType == 0 {
		return fmt.Errorf("collection column must have element type set")
	}

	return nil
}

// String returns string representation of attributes
func (ca *ColumnAttributes) String() string {
	attrs := []string{}

	if ca.IsIndexed() {
		attrs = append(attrs, "indexed")
	}
	if ca.IsUnique() {
		attrs = append(attrs, "unique")
	}
	if ca.IsPrimaryKey() {
		attrs = append(attrs, "primary_key")
	}
	if ca.IsNullable() {
		attrs = append(attrs, "nullable")
	}
	if ca.IsList() {
		attrs = append(attrs, "list")
	}
	if ca.IsSet() {
		attrs = append(attrs, "set")
	}
	if ca.IsDictionary() {
		attrs = append(attrs, "dictionary")
	}
	if ca.IsBacklink() {
		attrs = append(attrs, "backlink")
	}

	if len(attrs) == 0 {
		return "none"
	}

	result := ""
	for i, attr := range attrs {
		if i > 0 {
			result += ", "
		}
		result += attr
	}
	return result
}
