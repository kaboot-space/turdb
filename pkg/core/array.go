package core

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/turdb/tur/pkg/keys"
)

// ArrayHeader represents the header of an array in the file
type ArrayHeader struct {
	Magic    [4]byte // Array magic number "ARRY"
	Type     uint8   // Data type
	Size     uint32  // Number of elements
	Capacity uint32  // Allocated capacity
	HasNulls uint8   // Whether array supports nulls
	Width    uint8   // Element width in bytes (for fixed-width types)
	Reserved uint16  // Padding
}

var ArrayMagic = [4]byte{0x41, 0x52, 0x52, 0x59} // "ARRY"

const ArrayHeaderSize = unsafe.Sizeof(ArrayHeader{})

// Array represents a basic array structure in Tur
type Array struct {
	header    *ArrayHeader
	data      []byte
	nulls     []bool
	ref       RefType
	allocator *Allocator
	dataType  keys.DataType
	isInner   bool // Whether this is an inner array (part of a larger structure)
}

// NewArray creates a new array
func NewArray(dataType keys.DataType, allocator *Allocator) *Array {
	return &Array{
		dataType:  dataType,
		allocator: allocator,
		nulls:     make([]bool, 0),
	}
}

// Create creates a new array in the file
func (a *Array) Create(initialCapacity uint32) error {
	elementSize := a.getElementSize()
	totalSize := ArrayHeaderSize + uintptr(initialCapacity)*uintptr(elementSize)

	// Add space for null mask if needed
	if a.supportsNulls() {
		nullMaskSize := (initialCapacity + 7) / 8 // One bit per element
		totalSize += uintptr(nullMaskSize)
	}

	// Allocate space
	ref, err := a.allocator.Alloc(uint32(totalSize))
	if err != nil {
		return err
	}

	a.ref = ref

	// Initialize header
	elementSize = a.getElementSize()
	header := ArrayHeader{
		Magic:    ArrayMagic,
		Type:     uint8(a.dataType),
		Size:     0,
		Capacity: initialCapacity,
		HasNulls: 0,
		Width:    uint8(elementSize),
	}

	if a.supportsNulls() {
		header.HasNulls = 1
	}

	// Get the allocated data first
	a.data = a.allocator.GetData(ref, int(totalSize))
	if a.data == nil {
		// Debug information to understand why GetData failed
		offset := a.allocator.fileFormat.RefToOffset(RefType(ref))
		mapper := a.allocator.fileFormat.GetMapper()
		mapperSize := mapper.Size()
		mapperData := mapper.GetData()
		return fmt.Errorf("failed to get allocated data: ref=%d, offset=%d, totalSize=%d, mapperSize=%d, mapperData=nil:%t",
			ref, offset, totalSize, mapperSize, mapperData == nil)
	}
	if len(a.data) < int(totalSize) {
		return fmt.Errorf("allocated data size mismatch: got %d, expected %d", len(a.data), totalSize)
	}

	// Initialize header in the allocated space
	a.header = (*ArrayHeader)(unsafe.Pointer(&a.data[0]))
	*a.header = header

	return nil
}

// InitFromRef initializes array from an existing reference
func (a *Array) InitFromRef(ref RefType) error {
	a.ref = ref

	// Read header first
	headerData := a.allocator.GetData(ref, int(ArrayHeaderSize))
	if len(headerData) < int(ArrayHeaderSize) {
		return fmt.Errorf("insufficient data for array header")
	}

	header := (*ArrayHeader)(unsafe.Pointer(&headerData[0]))

	// Validate magic
	if header.Magic != ArrayMagic {
		return fmt.Errorf("invalid array magic")
	}

	a.dataType = keys.DataType(header.Type)

	// Calculate total size
	elementSize := a.getElementSize()
	totalSize := ArrayHeaderSize + uintptr(header.Capacity)*uintptr(elementSize)

	if header.HasNulls != 0 {
		nullMaskSize := (header.Capacity + 7) / 8
		totalSize += uintptr(nullMaskSize)
	}

	// Read full data
	a.data = a.allocator.GetData(ref, int(totalSize))
	a.header = (*ArrayHeader)(unsafe.Pointer(&a.data[0]))

	// Initialize null mask if present
	if header.HasNulls != 0 {
		a.nulls = make([]bool, header.Size)
		nullMaskOffset := ArrayHeaderSize + uintptr(header.Capacity)*uintptr(elementSize)
		a.loadNullMask(nullMaskOffset)
	}

	return nil
}

// Size returns the number of elements in the array
func (a *Array) Size() uint32 {
	if a.header == nil {
		return 0
	}
	return a.header.Size
}

// Capacity returns the allocated capacity
func (a *Array) Capacity() uint32 {
	if a.header == nil {
		return 0
	}
	return a.header.Capacity
}

// GetRef returns the array reference
func (a *Array) GetRef() RefType {
	return a.ref
}

// Get retrieves an element at the given index
func (a *Array) Get(index uint32) (interface{}, error) {
	if a.header == nil {
		return nil, fmt.Errorf("array not initialized")
	}

	if index >= a.header.Size {
		return nil, fmt.Errorf("index out of bounds")
	}

	// Check if null
	if a.header.HasNulls != 0 && index < uint32(len(a.nulls)) && a.nulls[index] {
		return nil, nil
	}

	return a.getValueAt(index)
}

// Set sets an element at the given index
func (a *Array) Set(index uint32, value interface{}) error {
	if a.header == nil {
		return fmt.Errorf("array not initialized")
	}

	if index >= a.header.Capacity {
		return fmt.Errorf("index exceeds capacity")
	}

	// Handle null values
	if value == nil {
		if !a.supportsNulls() {
			return fmt.Errorf("array does not support null values")
		}
		return a.setNull(index)
	}

	// Clear null flag if set
	if a.header.HasNulls != 0 && index < uint32(len(a.nulls)) {
		a.nulls[index] = false
		a.saveNullMask()
	}

	// Set the value
	if err := a.setValueAt(index, value); err != nil {
		return err
	}

	// Update size if needed
	if index >= a.header.Size {
		a.header.Size = index + 1
	}

	return nil
}

// Add appends an element to the array
func (a *Array) Add(value interface{}) error {
	if a.header == nil {
		return fmt.Errorf("array not initialized")
	}

	if a.header.Size >= a.header.Capacity {
		if err := a.grow(); err != nil {
			return err
		}
	}

	return a.Set(a.header.Size, value)
}

// getElementSize returns the size of each element in bytes
func (a *Array) getElementSize() int {
	switch a.dataType {
	case keys.TypeBool:
		return 1
	case keys.TypeInt:
		return 8
	case keys.TypeFloat:
		return 4
	case keys.TypeDouble:
		return 8
	case keys.TypeString, keys.TypeBinary:
		return 8 // Store as reference
	default:
		return 8 // Default to 8 bytes
	}
}

// supportsNulls returns true if this array type supports null values
func (a *Array) supportsNulls() bool {
	// Most types support nulls except primitives in some cases
	return true
}

// getValueAt retrieves the raw value at the given index
func (a *Array) getValueAt(index uint32) (interface{}, error) {
	elementSize := a.getElementSize()
	dataOffset := ArrayHeaderSize + uintptr(index)*uintptr(elementSize)

	if dataOffset+uintptr(elementSize) > uintptr(len(a.data)) {
		return nil, fmt.Errorf("data offset out of bounds")
	}

	dataSlice := a.data[dataOffset : dataOffset+uintptr(elementSize)]

	switch a.dataType {
	case keys.TypeBool:
		return dataSlice[0] != 0, nil
	case keys.TypeInt:
		return int64(binary.LittleEndian.Uint64(dataSlice)), nil
	case keys.TypeFloat:
		bits := binary.LittleEndian.Uint32(dataSlice)
		return *(*float32)(unsafe.Pointer(&bits)), nil
	case keys.TypeDouble:
		bits := binary.LittleEndian.Uint64(dataSlice)
		return *(*float64)(unsafe.Pointer(&bits)), nil
	case keys.TypeString:
		// String stored as reference to string table
		ref := RefType(binary.LittleEndian.Uint64(dataSlice))
		return a.loadString(ref)
	case keys.TypeLink:
		return keys.ObjKey(binary.LittleEndian.Uint64(dataSlice)), nil
	case keys.TypeTypedLink:
		// TypedLink stored as string reference
		ref := RefType(binary.LittleEndian.Uint64(dataSlice))
		encodedStr, err := a.loadString(ref)
		if err != nil {
			return nil, err
		}
		return keys.DecodeTypedLink([]byte(encodedStr))
	default:
		return binary.LittleEndian.Uint64(dataSlice), nil
	}
}

// setValueAt sets the raw value at the given index
func (a *Array) setValueAt(index uint32, value interface{}) error {
	// Check if array is properly initialized
	if a.data == nil {
		return fmt.Errorf("array data is not initialized")
	}
	if a.header == nil {
		return fmt.Errorf("array header is not initialized")
	}

	elementSize := a.getElementSize()
	dataOffset := ArrayHeaderSize + uintptr(index)*uintptr(elementSize)

	// Ensure we have enough space in the slice - use consistent bounds checking
	if int(dataOffset)+elementSize > len(a.data) {
		return fmt.Errorf("data offset out of bounds: offset=%d, elementSize=%d, dataLen=%d",
			dataOffset, elementSize, len(a.data))
	}

	// Get the slice starting from the correct offset with proper bounds
	if int(dataOffset) >= len(a.data) {
		return fmt.Errorf("data offset exceeds buffer: offset=%d, dataLen=%d", dataOffset, len(a.data))
	}

	dataPtr := a.data[dataOffset : dataOffset+uintptr(elementSize)]

	switch a.dataType {
	case keys.TypeBool:
		if len(dataPtr) < 1 {
			return fmt.Errorf("insufficient buffer for bool")
		}
		if v, ok := value.(bool); ok {
			if v {
				dataPtr[0] = 1
			} else {
				dataPtr[0] = 0
			}
		} else {
			return fmt.Errorf("invalid type for bool array")
		}
	case keys.TypeInt:
		if len(dataPtr) < 8 {
			return fmt.Errorf("insufficient buffer for int64")
		}
		if v, ok := value.(int64); ok {
			binary.LittleEndian.PutUint64(dataPtr, uint64(v))
		} else if v, ok := value.(int); ok {
			binary.LittleEndian.PutUint64(dataPtr, uint64(v))
		} else {
			return fmt.Errorf("invalid type for int array")
		}
	case keys.TypeFloat:
		if len(dataPtr) < 4 {
			return fmt.Errorf("insufficient buffer for float32")
		}
		if v, ok := value.(float32); ok {
			bits := *(*uint32)(unsafe.Pointer(&v))
			binary.LittleEndian.PutUint32(dataPtr, bits)
		} else {
			return fmt.Errorf("invalid type for float array")
		}
	case keys.TypeDouble:
		if len(dataPtr) < 8 {
			return fmt.Errorf("insufficient buffer for float64")
		}
		if v, ok := value.(float64); ok {
			bits := *(*uint64)(unsafe.Pointer(&v))
			binary.LittleEndian.PutUint64(dataPtr, bits)
		} else {
			return fmt.Errorf("invalid type for double array")
		}
	case keys.TypeString:
		if len(dataPtr) < 8 {
			return fmt.Errorf("insufficient buffer for string reference")
		}
		if v, ok := value.(string); ok {
			ref, err := a.storeString(v)
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(dataPtr, uint64(ref))
		} else {
			return fmt.Errorf("invalid type for string array")
		}
	case keys.TypeLink:
		if len(dataPtr) < 8 {
			return fmt.Errorf("insufficient buffer for link")
		}
		if v, ok := value.(keys.ObjKey); ok {
			binary.LittleEndian.PutUint64(dataPtr, uint64(v))
		} else {
			return fmt.Errorf("invalid type for link array")
		}
	case keys.TypeTypedLink:
		if len(dataPtr) < 8 {
			return fmt.Errorf("insufficient buffer for typed link")
		}
		if v, ok := value.(keys.TypedLink); ok {
			// Encode TypedLink to bytes and store as string reference
			encoded := v.Encode()
			ref, err := a.storeString(string(encoded))
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(dataPtr, uint64(ref))
		} else {
			return fmt.Errorf("invalid type for typedlink array")
		}
	default:
		if len(dataPtr) < 8 {
			return fmt.Errorf("insufficient buffer for default type")
		}
		if v, ok := value.(uint64); ok {
			binary.LittleEndian.PutUint64(dataPtr, v)
		} else {
			return fmt.Errorf("unsupported type")
		}
	}

	return nil
}

// setNull sets an element to null
func (a *Array) setNull(index uint32) error {
	if !a.supportsNulls() {
		return fmt.Errorf("array does not support nulls")
	}

	// Extend nulls array if needed
	for uint32(len(a.nulls)) <= index {
		a.nulls = append(a.nulls, false)
	}

	a.nulls[index] = true
	a.saveNullMask()

	// Update size if needed
	if index >= a.header.Size {
		a.header.Size = index + 1
	}

	return nil
}

// loadNullMask loads the null mask from the data
func (a *Array) loadNullMask(offset uintptr) {
	size := a.header.Size
	a.nulls = make([]bool, size)

	for i := uint32(0); i < size; i++ {
		byteIndex := i / 8
		bitIndex := i % 8
		if offset+uintptr(byteIndex) < uintptr(len(a.data)) {
			bit := (a.data[offset+uintptr(byteIndex)] >> bitIndex) & 1
			a.nulls[i] = bit != 0
		}
	}
}

// saveNullMask saves the null mask to the data
func (a *Array) saveNullMask() {
	if a.header.HasNulls == 0 {
		return
	}

	elementSize := a.getElementSize()
	nullMaskOffset := ArrayHeaderSize + uintptr(a.header.Capacity)*uintptr(elementSize)

	// Clear null mask
	nullMaskSize := (a.header.Capacity + 7) / 8
	for i := uintptr(0); i < uintptr(nullMaskSize); i++ {
		if nullMaskOffset+i < uintptr(len(a.data)) {
			a.data[nullMaskOffset+i] = 0
		}
	}

	// Set bits for null values
	for i, isNull := range a.nulls {
		if isNull {
			byteIndex := i / 8
			bitIndex := i % 8
			if nullMaskOffset+uintptr(byteIndex) < uintptr(len(a.data)) {
				a.data[nullMaskOffset+uintptr(byteIndex)] |= 1 << bitIndex
			}
		}
	}
}

// grow increases the array capacity
func (a *Array) grow() error {
	newCapacity := a.header.Capacity * 2
	if newCapacity == 0 {
		newCapacity = 16
	}

	// Calculate new data size
	elementSize := a.getElementSize()
	newDataSize := uint32(ArrayHeaderSize) + newCapacity*uint32(elementSize)

	// Allocate new space
	newRef, err := a.allocator.Alloc(newDataSize)
	if err != nil {
		return fmt.Errorf("failed to allocate new array space: %w", err)
	}

	// Get new data buffer
	newData := a.allocator.GetData(newRef, int(newDataSize))
	if len(newData) < int(newDataSize) {
		a.allocator.Free(newRef, newDataSize)
		return fmt.Errorf("insufficient space for new array")
	}

	// Copy header
	oldHeaderSize := int(ArrayHeaderSize)
	copy(newData[:oldHeaderSize], a.data[:oldHeaderSize])

	// Copy existing data
	oldDataSize := a.header.Size * uint32(elementSize)
	if oldDataSize > 0 {
		copy(newData[oldHeaderSize:oldHeaderSize+int(oldDataSize)],
			a.data[oldHeaderSize:oldHeaderSize+int(oldDataSize)])
	}

	// Free old space
	if a.ref != 0 {
		oldSize := uint32(ArrayHeaderSize) + a.header.Capacity*uint32(elementSize)
		a.allocator.Free(a.ref, oldSize)
	}

	// Update array
	a.ref = newRef
	a.data = newData
	a.header = (*ArrayHeader)(unsafe.Pointer(&newData[0]))
	a.header.Capacity = newCapacity

	return nil
}

// loadString loads a string from a reference
func (a *Array) loadString(ref RefType) (string, error) {
	if ref == 0 {
		return "", nil // Empty string for 0 ref
	}

	// Read length first (4 bytes)
	lengthData := a.allocator.GetData(ref, 4)
	if len(lengthData) < 4 {
		return "", fmt.Errorf("insufficient data for string length")
	}

	// Decode length (little endian)
	strLen := uint32(lengthData[0]) |
		(uint32(lengthData[1]) << 8) |
		(uint32(lengthData[2]) << 16) |
		(uint32(lengthData[3]) << 24)

	if strLen == 0 {
		return "", nil
	}

	// Read the string data
	totalSize := 4 + int(strLen)
	data := a.allocator.GetData(ref, totalSize)
	if len(data) < totalSize {
		return "", fmt.Errorf("insufficient data for string content")
	}

	return string(data[4 : 4+strLen]), nil
}

// storeString stores a string and returns a reference
func (a *Array) storeString(s string) (RefType, error) {
	if len(s) == 0 {
		return 0, nil // Return 0 ref for empty strings
	}

	// Store as: [length:4][data:length] (no null terminator needed)
	strLen := uint32(len(s))
	totalSize := 4 + strLen

	ref, err := a.allocator.Alloc(totalSize)
	if err != nil {
		return 0, fmt.Errorf("failed to allocate string space: %w", err)
	}

	// Prepare data buffer: length (4 bytes) + string data
	data := make([]byte, totalSize)

	// Write length (4 bytes, little endian)
	data[0] = byte(strLen)
	data[1] = byte(strLen >> 8)
	data[2] = byte(strLen >> 16)
	data[3] = byte(strLen >> 24)

	// Write string data
	if strLen > 0 {
		copy(data[4:], []byte(s))
	}

	// Write to file
	err = a.allocator.WriteData(ref, data)
	if err != nil {
		a.allocator.Free(ref, totalSize)
		return 0, fmt.Errorf("failed to write string data: %w", err)
	}

	return ref, nil
}
