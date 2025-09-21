package storage

import (
	"fmt"
	"os"
	"unsafe"
)

// Magic numbers for Tur file format
var (
	TurMagic = [4]byte{0x54, 0x2d, 0x44, 0x42} // "T-DB"
)

// FileHeader represents the Tur database file header
type FileHeader struct {
	Magic      [4]byte // File format magic number
	Version    uint32  // File format version
	PageSize   uint32  // Page size in bytes
	NumPages   uint64  // Number of pages in file
	RootRef    uint64  // Reference to root node
	GroupRef   uint64  // Reference to Group structure
	TableCount uint32  // Number of tables in the database
	TxnLogRef  uint64  // Reference to transaction log
	Reserved1  uint64  // Reserved for future use
	Reserved2  uint64  // Reserved for future use
}

// FileFormat manages file format operations
type FileFormat struct {
	mapper *FileMapper
}

// NewFileFormat creates a new file format manager
func NewFileFormat(filepath string) (*FileFormat, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	mapper, err := NewFileMapper(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create file mapper: %w", err)
	}

	return &FileFormat{
		mapper: mapper,
	}, nil
}

// Initialize initializes a new Tur database file
func (ff *FileFormat) Initialize(pageSize uint32) error {
	// Create initial header
	header := FileHeader{
		Magic:      TurMagic,
		Version:    1,
		PageSize:   pageSize,
		NumPages:   1,
		RootRef:    0,
		GroupRef:   0,
		TableCount: 0,
		TxnLogRef:  0,
		Reserved1:  0,
		Reserved2:  0,
	}

	// Create a page-sized initial file
	initialSize := int64(pageSize)
	if err := ff.mapper.file.Truncate(initialSize); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Update file size in mapper
	ff.mapper.size = initialSize

	// Write header to file
	headerBytes := (*[unsafe.Sizeof(FileHeader{})]byte)(unsafe.Pointer(&header))[:]
	if _, err := ff.mapper.file.WriteAt(headerBytes, 0); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Sync to ensure header is written
	if err := ff.mapper.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// Open opens an existing Tur file
func (ff *FileFormat) Open() error {
	if err := ff.mapper.Map(); err != nil {
		return fmt.Errorf("failed to map file: %w", err)
	}

	// Validate header
	header := ff.mapper.GetHeader()
	if header == nil {
		return fmt.Errorf("file too small to contain header")
	}

	if header.Magic != TurMagic {
		return fmt.Errorf("invalid magic number: expected %v, got %v", TurMagic, header.Magic)
	}

	return nil
}

// GetHeader returns the file header
func (ff *FileFormat) GetHeader() *FileHeader {
	return ff.mapper.GetHeader()
}

// GetMapper returns the file mapper
func (ff *FileFormat) GetMapper() *FileMapper {
	return ff.mapper
}

// Close closes the file format
func (ff *FileFormat) Close() error {
	return ff.mapper.Close()
}

// ValidateHeader validates the file header
func (ff *FileFormat) ValidateHeader() error {
	header := ff.GetHeader()
	if header == nil {
		return fmt.Errorf("header is nil")
	}

	// Check magic number
	if header.Magic != TurMagic {
		return fmt.Errorf("invalid magic number")
	}

	// Check version
	if header.Version != 1 {
		return fmt.Errorf("unsupported version: %d", header.Version)
	}

	// Check page size
	if header.PageSize == 0 || header.PageSize&(header.PageSize-1) != 0 {
		return fmt.Errorf("invalid page size: %d", header.PageSize)
	}

	return nil
}

// UpdateHeader updates the file header
func (ff *FileFormat) UpdateHeader(header *FileHeader) error {
	if ff.mapper.GetHeader() == nil {
		return fmt.Errorf("no mapped header to update")
	}

	// Copy to mapped header
	*ff.mapper.GetHeader() = *header

	return ff.mapper.Sync()
}

// Ref represents a reference to a node in the file
// UpdateGroupRef updates the group reference in the header
func (ff *FileFormat) UpdateGroupRef(groupRef uint64) error {
	header := ff.GetHeader()
	if header == nil {
		return fmt.Errorf("header is nil")
	}

	header.GroupRef = groupRef
	return ff.UpdateHeader(header)
}

// UpdateTableCount updates the table count in the header
func (ff *FileFormat) UpdateTableCount(count uint32) error {
	header := ff.GetHeader()
	if header == nil {
		return fmt.Errorf("header is nil")
	}

	header.TableCount = count
	return ff.UpdateHeader(header)
}

// GetGroupRef returns the group reference from the header
func (ff *FileFormat) GetGroupRef() uint64 {
	header := ff.GetHeader()
	if header == nil {
		return 0
	}
	return header.GroupRef
}

// GetTableCount returns the table count from the header
func (ff *FileFormat) GetTableCount() uint32 {
	header := ff.GetHeader()
	if header == nil {
		return 0
	}
	return header.TableCount
}

// Ref represents a reference to a node in the file
type Ref uint64

// RefToOffset converts a reference to a file offset
func (ff *FileFormat) RefToOffset(ref Ref) int64 {
	// In Tur, refs are typically byte offsets
	return int64(ref)
}

// OffsetToRef converts a file offset to a reference
func (ff *FileFormat) OffsetToRef(offset int64) Ref {
	return Ref(offset)
}

// AllocateSpace allocates space in the file and returns a reference
func (ff *FileFormat) AllocateSpace(size uint32) (Ref, error) {
	header := ff.mapper.GetHeader()
	if header == nil {
		return 0, fmt.Errorf("no header available")
	}

	// Simple allocation: append to end of file
	currentSize := int64(header.NumPages) * int64(header.PageSize)

	// Align size to page boundary
	pageSize := int64(header.PageSize)
	alignedSize := ((int64(size) + pageSize - 1) / pageSize) * pageSize

	newSize := currentSize + alignedSize

	// Extend file if necessary
	if err := ff.mapper.file.Truncate(newSize); err != nil {
		return 0, fmt.Errorf("failed to extend file: %w", err)
	}

	// Update mapper size
	ff.mapper.size = newSize

	// Remap the file to include the new space
	if err := ff.mapper.Unmap(); err != nil {
		return 0, fmt.Errorf("failed to unmap: %w", err)
	}
	if err := ff.mapper.Map(); err != nil {
		return 0, fmt.Errorf("failed to remap: %w", err)
	}

	// Update header
	header = ff.mapper.GetHeader() // Get header from remapped memory
	header.NumPages = uint64(newSize / pageSize)

	return Ref(currentSize), nil
}
