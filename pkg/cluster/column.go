package cluster

import (
	"fmt"
	"unsafe"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// ColumnArrayHeader represents the header of a column array in cluster storage
type ColumnArrayHeader struct {
	Magic    [4]byte     // Column array magic "CARR"
	ColKey   keys.ColKey // Column key with type and nullability info
	Size     uint32      // Number of elements
	Capacity uint32      // Allocated capacity
	DataRef  storage.Ref // Reference to actual data array
	NullsRef storage.Ref // Reference to nulls bitmap (if applicable)
	Reserved uint32      // Reserved for alignment
}

var ClusterColumnArrayMagic = [4]byte{0x43, 0x41, 0x52, 0x52} // "CARR"

const ClusterColumnArrayHeaderSize = unsafe.Sizeof(ColumnArrayHeader{})

// ClusterColumnArray represents a column array optimized for cluster storage
type ClusterColumnArray struct {
	header     *ColumnArrayHeader
	dataArray  *core.Array
	nullsArray *core.Array
	ref        storage.Ref
	alloc      *core.Allocator
	fileFormat *storage.FileFormat
	colKey     keys.ColKey
}

// NewClusterColumnArray creates a new cluster column array
func NewClusterColumnArray(colKey keys.ColKey, alloc *core.Allocator, fileFormat *storage.FileFormat) *ClusterColumnArray {
	return &ClusterColumnArray{
		colKey:     colKey,
		alloc:      alloc,
		fileFormat: fileFormat,
	}
}

// Create creates a new column array in the file
func (cca *ClusterColumnArray) Create(initialCapacity uint32) error {
	// Allocate space for header
	headerSize := uint32(ClusterColumnArrayHeaderSize)
	ref, err := cca.fileFormat.AllocateSpace(headerSize)
	if err != nil {
		return fmt.Errorf("failed to allocate column array header: %w", err)
	}

	cca.ref = ref

	// Create data array
	dataType := cca.colKey.GetType()
	cca.dataArray = core.NewArray(dataType, cca.alloc)

	if err := cca.dataArray.Create(initialCapacity); err != nil {
		return fmt.Errorf("failed to create data array: %w", err)
	}

	// Create nulls array if column is nullable
	var nullsRef storage.Ref = 0
	if cca.colKey.IsNullable() {
		cca.nullsArray = core.NewArray(keys.TypeBool, cca.alloc)

		if err := cca.nullsArray.Create(initialCapacity); err != nil {
			return fmt.Errorf("failed to create nulls array: %w", err)
		}
		nullsRef = storage.Ref(cca.nullsArray.GetRef())
	}

	// Initialize header
	header := ColumnArrayHeader{
		Magic:    ClusterColumnArrayMagic,
		ColKey:   cca.colKey,
		Size:     0,
		Capacity: initialCapacity,
		DataRef:  storage.Ref(cca.dataArray.GetRef()),
		NullsRef: nullsRef,
		Reserved: 0,
	}

	// Write header to file
	if err := cca.writeHeader(&header); err != nil {
		return fmt.Errorf("failed to write column array header: %w", err)
	}

	return nil
}

// writeHeader writes the column array header to file
func (cca *ClusterColumnArray) writeHeader(header *ColumnArrayHeader) error {
	offset := cca.fileFormat.RefToOffset(cca.ref)
	headerData := (*[ClusterColumnArrayHeaderSize]byte)(unsafe.Pointer(header))
	mapper := cca.fileFormat.GetMapper()

	// Get pointer to the location and copy data
	ptr := mapper.GetPointer(offset)
	copy((*[ClusterColumnArrayHeaderSize]byte)(ptr)[:], headerData[:])

	cca.header = (*ColumnArrayHeader)(ptr)
	return mapper.Sync()
}

// InitFromRef initializes column array from an existing reference
func (cca *ClusterColumnArray) InitFromRef(ref storage.Ref) error {
	cca.ref = ref

	// Read header
	offset := cca.fileFormat.RefToOffset(ref)
	mapper := cca.fileFormat.GetMapper()
	ptr := mapper.GetPointer(offset)
	cca.header = (*ColumnArrayHeader)(ptr)

	// Verify magic
	if cca.header.Magic != ClusterColumnArrayMagic {
		return fmt.Errorf("invalid column array magic")
	}

	cca.colKey = cca.header.ColKey

	// Initialize data array
	cca.dataArray = core.NewArray(cca.colKey.GetType(), cca.alloc)
	if err := cca.dataArray.InitFromRef(core.RefType(cca.header.DataRef)); err != nil {
		return fmt.Errorf("failed to initialize data array: %w", err)
	}

	// Initialize nulls array if present
	if cca.header.NullsRef != 0 {
		cca.nullsArray = core.NewArray(keys.TypeBool, cca.alloc)
		if err := cca.nullsArray.InitFromRef(core.RefType(cca.header.NullsRef)); err != nil {
			return fmt.Errorf("failed to initialize nulls array: %w", err)
		}
	}

	return nil
}

// Size returns the number of elements in the column array
func (cca *ClusterColumnArray) Size() uint32 {
	if cca.header == nil {
		return 0
	}
	return cca.header.Size
}

// Capacity returns the allocated capacity
func (cca *ClusterColumnArray) Capacity() uint32 {
	if cca.header == nil {
		return 0
	}
	return cca.header.Capacity
}

// GetRef returns the reference to this column array
func (cca *ClusterColumnArray) GetRef() storage.Ref {
	return cca.ref
}

// GetColKey returns the column key
func (cca *ClusterColumnArray) GetColKey() keys.ColKey {
	return cca.colKey
}

// Get retrieves a value at the specified index
func (cca *ClusterColumnArray) Get(index uint32) (interface{}, error) {
	if index >= cca.Size() {
		return nil, fmt.Errorf("index out of bounds")
	}

	// Check if value is null
	if cca.nullsArray != nil {
		isNull, err := cca.nullsArray.Get(index)
		if err != nil {
			return nil, err
		}
		if isNull.(bool) {
			return nil, nil
		}
	}

	return cca.dataArray.Get(index)
}

// Set sets a value at the specified index
func (cca *ClusterColumnArray) Set(index uint32, value interface{}) error {
	if index >= cca.Size() {
		return fmt.Errorf("index out of bounds")
	}

	// Handle null values
	if value == nil {
		if !cca.colKey.IsNullable() {
			return fmt.Errorf("column does not support null values")
		}
		if cca.nullsArray != nil {
			return cca.nullsArray.Set(index, true)
		}
		return fmt.Errorf("nulls array not initialized")
	}

	// Set null flag to false if nulls array exists
	if cca.nullsArray != nil {
		if err := cca.nullsArray.Set(index, false); err != nil {
			return err
		}
	}

	return cca.dataArray.Set(index, value)
}

// Add appends a value to the column array
func (cca *ClusterColumnArray) Add(value interface{}) error {
	// Handle null values
	if value == nil {
		if !cca.colKey.IsNullable() {
			return fmt.Errorf("column does not support null values")
		}
		if cca.nullsArray != nil {
			if err := cca.nullsArray.Add(true); err != nil {
				return err
			}
		}
		// Add placeholder value to data array
		var placeholder interface{}
		switch cca.colKey.GetType() {
		case keys.TypeInt:
			placeholder = int64(0)
		case keys.TypeBool:
			placeholder = false
		case keys.TypeString:
			placeholder = ""
		case keys.TypeFloat:
			placeholder = float32(0)
		case keys.TypeDouble:
			placeholder = float64(0)
		default:
			placeholder = int64(0)
		}
		if err := cca.dataArray.Add(placeholder); err != nil {
			return err
		}
	} else {
		// Set null flag to false if nulls array exists
		if cca.nullsArray != nil {
			if err := cca.nullsArray.Add(false); err != nil {
				return err
			}
		}
		if err := cca.dataArray.Add(value); err != nil {
			return err
		}
	}

	// Update size
	cca.header.Size++
	return nil
}

// Remove removes an element at the specified index
func (cca *ClusterColumnArray) Remove(index uint32) error {
	if index >= cca.Size() {
		return fmt.Errorf("index out of bounds")
	}

	// This is a simplified remove - in a real implementation,
	// you would need to shift elements or use a more sophisticated approach
	// For now, we'll mark as null if nullable, otherwise error
	if cca.colKey.IsNullable() && cca.nullsArray != nil {
		if err := cca.nullsArray.Set(index, true); err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("remove not supported for non-nullable columns")
}
