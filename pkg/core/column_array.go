package core

import (
	"fmt"
	"unsafe"

	"github.com/turdb/tur/pkg/keys"
)

// ColumnArrayHeader represents the header of a column array
type ColumnArrayHeader struct {
	Magic    [4]byte // Column array magic "CARR"
	ColKey   keys.ColKey
	Size     uint32
	Capacity uint32
	DataRef  RefType // Reference to actual data array
	NullsRef RefType // Reference to nulls bitmap (if applicable)
	Reserved uint32
}

var ColumnArrayMagic = [4]byte{0x43, 0x41, 0x52, 0x52} // "CARR"

const ColumnArrayHeaderSize = unsafe.Sizeof(ColumnArrayHeader{})

// ColumnArray represents a column-based array optimized for tur Core
type ColumnArray struct {
	header     *ColumnArrayHeader
	dataArray  *Array
	nullsArray *Array
	ref        RefType
	allocator  *Allocator
	colKey     keys.ColKey
}

// NewColumnArray creates a new column array
func NewColumnArray(colKey keys.ColKey, allocator *Allocator) *ColumnArray {
	return &ColumnArray{
		colKey:    colKey,
		allocator: allocator,
	}
}

// Create creates a new column array in the file
func (ca *ColumnArray) Create(initialCapacity uint32) error {
	// Allocate space for header
	ref, err := ca.allocator.Alloc(uint32(ColumnArrayHeaderSize))
	if err != nil {
		return fmt.Errorf("failed to allocate column array header: %w", err)
	}

	ca.ref = ref

	// Create data array
	dataType := ca.colKey.GetType()
	ca.dataArray = NewArray(dataType, ca.allocator)
	ca.dataArray.isInner = true

	if err := ca.dataArray.Create(initialCapacity); err != nil {
		ca.allocator.Free(ref, uint32(ColumnArrayHeaderSize))
		return fmt.Errorf("failed to create data array: %w", err)
	}

	// Create nulls array if column is nullable
	var nullsRef RefType = 0
	if ca.colKey.IsNullable() {
		ca.nullsArray = NewArray(keys.TypeBool, ca.allocator)
		ca.nullsArray.isInner = true

		if err := ca.nullsArray.Create(initialCapacity); err != nil {
			ca.allocator.Free(ref, uint32(ColumnArrayHeaderSize))
			return fmt.Errorf("failed to create nulls array: %w", err)
		}
		nullsRef = ca.nullsArray.GetRef()
	}

	// Initialize header
	header := ColumnArrayHeader{
		Magic:    ColumnArrayMagic,
		ColKey:   ca.colKey,
		Size:     0,
		Capacity: initialCapacity,
		DataRef:  ca.dataArray.GetRef(),
		NullsRef: nullsRef,
	}

	// Get allocated data and set header
	headerData := ca.allocator.GetData(ref, int(ColumnArrayHeaderSize))
	if len(headerData) < int(ColumnArrayHeaderSize) {
		return fmt.Errorf("insufficient space for column array header")
	}

	ca.header = (*ColumnArrayHeader)(unsafe.Pointer(&headerData[0]))
	*ca.header = header

	return nil
}

// InitFromRef initializes column array from an existing reference
func (ca *ColumnArray) InitFromRef(ref RefType) error {
	ca.ref = ref

	// Read header
	headerData := ca.allocator.GetData(ref, int(ColumnArrayHeaderSize))
	if len(headerData) < int(ColumnArrayHeaderSize) {
		return fmt.Errorf("insufficient data for column array header")
	}

	ca.header = (*ColumnArrayHeader)(unsafe.Pointer(&headerData[0]))

	// Verify magic
	if ca.header.Magic != ColumnArrayMagic {
		return fmt.Errorf("invalid column array magic")
	}

	ca.colKey = ca.header.ColKey

	// Initialize data array
	ca.dataArray = NewArray(ca.colKey.GetType(), ca.allocator)
	ca.dataArray.isInner = true
	if err := ca.dataArray.InitFromRef(ca.header.DataRef); err != nil {
		return fmt.Errorf("failed to initialize data array: %w", err)
	}

	// Initialize nulls array if present
	if ca.header.NullsRef != 0 {
		ca.nullsArray = NewArray(keys.TypeBool, ca.allocator)
		ca.nullsArray.isInner = true
		if err := ca.nullsArray.InitFromRef(ca.header.NullsRef); err != nil {
			return fmt.Errorf("failed to initialize nulls array: %w", err)
		}
	}

	return nil
}

// Size returns the number of elements in the column array
func (ca *ColumnArray) Size() uint32 {
	if ca.header == nil {
		return 0
	}
	return ca.header.Size
}

// Capacity returns the allocated capacity
func (ca *ColumnArray) Capacity() uint32 {
	if ca.header == nil {
		return 0
	}
	return ca.header.Capacity
}

// GetRef returns the reference to this column array
func (ca *ColumnArray) GetRef() RefType {
	return ca.ref
}

// GetColKey returns the column key
func (ca *ColumnArray) GetColKey() keys.ColKey {
	return ca.colKey
}

// Get retrieves a value at the specified index
func (ca *ColumnArray) Get(index uint32) (interface{}, error) {
	if index >= ca.Size() {
		return nil, fmt.Errorf("index out of bounds")
	}

	// Check if value is null
	if ca.nullsArray != nil {
		isNull, err := ca.nullsArray.Get(index)
		if err != nil {
			return nil, err
		}
		if isNull.(bool) {
			return nil, nil
		}
	}

	return ca.dataArray.Get(index)
}

// Set sets a value at the specified index
func (ca *ColumnArray) Set(index uint32, value interface{}) error {
	if index >= ca.Size() {
		return fmt.Errorf("index out of bounds")
	}

	// Handle null values
	if value == nil {
		if !ca.colKey.IsNullable() {
			return fmt.Errorf("column does not support null values")
		}
		if ca.nullsArray != nil {
			return ca.nullsArray.Set(index, true)
		}
		return fmt.Errorf("nulls array not initialized")
	}

	// Set null flag to false if nulls array exists
	if ca.nullsArray != nil {
		if err := ca.nullsArray.Set(index, false); err != nil {
			return err
		}
	}

	return ca.dataArray.Set(index, value)
}

// Add appends a value to the column array
func (ca *ColumnArray) Add(value interface{}) error {
	// Handle null values
	if value == nil {
		if !ca.colKey.IsNullable() {
			return fmt.Errorf("column does not support null values")
		}
		if ca.nullsArray != nil {
			if err := ca.nullsArray.Add(true); err != nil {
				return err
			}
		}
		// Add placeholder value to data array
		var placeholder interface{}
		switch ca.colKey.GetType() {
		case keys.TypeInt:
			placeholder = int64(0)
		case keys.TypeBool:
			placeholder = false
		case keys.TypeString:
			placeholder = ""
		default:
			placeholder = int64(0)
		}
		if err := ca.dataArray.Add(placeholder); err != nil {
			return err
		}
	} else {
		// Set null flag to false if nulls array exists
		if ca.nullsArray != nil {
			if err := ca.nullsArray.Add(false); err != nil {
				return err
			}
		}
		if err := ca.dataArray.Add(value); err != nil {
			return err
		}
	}

	// Update size
	ca.header.Size++
	return nil
}
