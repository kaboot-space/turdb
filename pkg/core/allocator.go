package core

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/turdb/tur/pkg/storage"
)

// RefType represents a reference to data in the file
type RefType = storage.Ref

// FreeBlock represents a free memory block
type FreeBlock struct {
	ref  RefType
	size uint32
}

// Allocator manages memory allocation within the database file
type Allocator struct {
	fileFormat *storage.FileFormat
	freeList   []FreeBlock
	mutex      sync.Mutex
	alignment  uint32 // Memory alignment requirement
}

// NewAllocator creates a new allocator
func NewAllocator(fileFormat *storage.FileFormat) *Allocator {
	return &Allocator{
		fileFormat: fileFormat,
		freeList:   make([]FreeBlock, 0),
		alignment:  8, // Default 8-byte alignment
	}
}

// Alloc allocates a block of memory and returns a reference
func (a *Allocator) Alloc(size uint32) (RefType, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Align size to alignment boundary
	alignedSize := (size + a.alignment - 1) &^ (a.alignment - 1)

	// Try to find a suitable free block
	for i, block := range a.freeList {
		if block.size >= alignedSize {
			// Remove from free list
			a.freeList = append(a.freeList[:i], a.freeList[i+1:]...)

			// If block is significantly larger, split it
			if block.size > alignedSize+a.alignment {
				remainingBlock := FreeBlock{
					ref:  RefType(uint64(block.ref) + uint64(alignedSize)),
					size: block.size - alignedSize,
				}
				a.freeList = append(a.freeList, remainingBlock)
			}

			return block.ref, nil
		}
	}

	// Allocate new space
	ref, err := a.fileFormat.AllocateSpace(alignedSize)
	if err != nil {
		return 0, fmt.Errorf("failed to allocate space: %w", err)
	}

	return RefType(ref), nil
}

// Free adds a reference to the free list
func (a *Allocator) Free(ref RefType, size uint32) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	block := FreeBlock{
		ref:  ref,
		size: size,
	}

	// Try to coalesce with adjacent blocks
	a.freeList = append(a.freeList, block)
	a.coalesceBlocks()
}

// coalesceBlocks merges adjacent free blocks to reduce fragmentation
func (a *Allocator) coalesceBlocks() {
	if len(a.freeList) < 2 {
		return
	}

	// Sort blocks by reference address for easier coalescing
	for i := 0; i < len(a.freeList)-1; i++ {
		for j := i + 1; j < len(a.freeList); j++ {
			if a.freeList[i].ref > a.freeList[j].ref {
				a.freeList[i], a.freeList[j] = a.freeList[j], a.freeList[i]
			}
		}
	}

	// Merge adjacent blocks
	merged := make([]FreeBlock, 0, len(a.freeList))
	current := a.freeList[0]

	for i := 1; i < len(a.freeList); i++ {
		next := a.freeList[i]
		// Check if blocks are adjacent
		if uint64(current.ref)+uint64(current.size) == uint64(next.ref) {
			// Merge blocks
			current.size += next.size
		} else {
			merged = append(merged, current)
			current = next
		}
	}
	merged = append(merged, current)
	a.freeList = merged
}

// Translate converts a reference to a memory pointer
func (a *Allocator) Translate(ref RefType) unsafe.Pointer {
	offset := a.fileFormat.RefToOffset(storage.Ref(ref))
	return a.fileFormat.GetMapper().GetPointer(offset)
}

// GetData returns data at the given reference
func (a *Allocator) GetData(ref RefType, size int) []byte {
	offset := a.fileFormat.RefToOffset(storage.Ref(ref))
	mapper := a.fileFormat.GetMapper()

	// Check bounds
	if offset < 0 || offset+int64(size) > mapper.Size() {
		return nil
	}

	// Return a slice directly from the mapped memory
	data := mapper.GetData()
	if data == nil {
		return nil
	}

	return data[offset : offset+int64(size)]
}

// WriteData writes data at the given reference
func (a *Allocator) WriteData(ref RefType, data []byte) error {
	offset := a.fileFormat.RefToOffset(storage.Ref(ref))
	mapper := a.fileFormat.GetMapper()

	if offset < 0 || offset+int64(len(data)) > mapper.Size() {
		return fmt.Errorf("write out of bounds")
	}

	copy(mapper.GetData()[offset:], data)
	return nil
}
