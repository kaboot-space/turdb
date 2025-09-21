package core

import "sync"

// SlabAlloc provides slab allocation for frequently used object sizes
type SlabAlloc struct {
	allocator *Allocator
	slabSizes []uint32
	slabPools [][]RefType
	mutex     sync.Mutex
}

// NewSlabAlloc creates a new slab allocator
func NewSlabAlloc(allocator *Allocator) *SlabAlloc {
	// Common sizes for tur objects
	sizes := []uint32{16, 32, 64, 128, 256, 512, 1024, 2048, 4096}

	return &SlabAlloc{
		allocator: allocator,
		slabSizes: sizes,
		slabPools: make([][]RefType, len(sizes)),
	}
}

// Alloc allocates from appropriate slab
func (sa *SlabAlloc) Alloc(size uint32) (RefType, error) {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()

	// Find appropriate slab
	slabIndex := -1
	for i, slabSize := range sa.slabSizes {
		if size <= slabSize {
			slabIndex = i
			break
		}
	}

	if slabIndex == -1 {
		// Size too large for slab allocation, use regular allocator
		return sa.allocator.Alloc(size)
	}

	// Check if we have a free block in this slab
	if len(sa.slabPools[slabIndex]) > 0 {
		ref := sa.slabPools[slabIndex][len(sa.slabPools[slabIndex])-1]
		sa.slabPools[slabIndex] = sa.slabPools[slabIndex][:len(sa.slabPools[slabIndex])-1]
		return ref, nil
	}

	// Allocate new block
	return sa.allocator.Alloc(sa.slabSizes[slabIndex])
}

// Free returns a block to the appropriate slab
func (sa *SlabAlloc) Free(ref RefType, size uint32) {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()

	// Find appropriate slab
	slabIndex := -1
	for i, slabSize := range sa.slabSizes {
		if size <= slabSize {
			slabIndex = i
			break
		}
	}

	if slabIndex == -1 {
		// Size too large for slab, use regular free
		sa.allocator.Free(ref, size)
		return
	}

	// Add to slab pool
	sa.slabPools[slabIndex] = append(sa.slabPools[slabIndex], ref)
}
