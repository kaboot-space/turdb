package core

import (
	"sync"
	"sync/atomic"

	"github.com/turdb/tur/pkg/storage"
)

// MemRefType represents different types of memory references
type MemRefType int

const (
	MemRefTypeNormal MemRefType = iota
	MemRefTypeArray
	MemRefTypeString
	MemRefTypeBinary
	MemRefTypeTable
	MemRefTypeCluster
)

// MemRefFlags represents memory reference flags
type MemRefFlags uint32

const (
	MemRefFlagNone        MemRefFlags = 0
	MemRefFlagReadOnly    MemRefFlags = 1 << 0
	MemRefFlagWritable    MemRefFlags = 1 << 1
	MemRefFlagMapped      MemRefFlags = 1 << 2
	MemRefFlagCached      MemRefFlags = 1 << 3
	MemRefFlagDirty       MemRefFlags = 1 << 4
	MemRefFlagPinned      MemRefFlags = 1 << 5
	MemRefFlagCompressed  MemRefFlags = 1 << 6
	MemRefFlagEncrypted   MemRefFlags = 1 << 7
)

// MemRef represents an enhanced memory reference with reference counting
type MemRef struct {
	// Core reference data
	ref        storage.Ref    // Storage reference
	addr       uintptr        // Memory address when loaded
	size       uint64         // Size of the referenced data
	offset     uint64         // Offset within the reference
	
	// Type and metadata
	refType    MemRefType     // Type of memory reference
	flags      MemRefFlags    // Reference flags
	version    uint64         // Version for consistency checking
	
	// Reference counting
	refCount   int64          // Atomic reference count
	weakCount  int64          // Weak reference count
	
	// Memory management
	allocator  *Allocator     // Associated allocator
	parent     *MemRef        // Parent reference for hierarchical refs
	children   []*MemRef      // Child references
	
	// Synchronization
	mutex      sync.RWMutex   // Protects mutable fields
	
	// Caching and performance
	lastAccess int64          // Last access timestamp (atomic)
	accessCount int64         // Access count (atomic)
}

// NewMemRef creates a new memory reference
func NewMemRef(ref storage.Ref, size uint64, allocator *Allocator) *MemRef {
	return &MemRef{
		ref:         ref,
		size:        size,
		refType:     MemRefTypeNormal,
		flags:       MemRefFlagNone,
		version:     1,
		refCount:    1,
		weakCount:   0,
		allocator:   allocator,
		children:    make([]*MemRef, 0),
		lastAccess:  0,
		accessCount: 0,
	}
}

// NewMemRefWithType creates a new memory reference with specific type
func NewMemRefWithType(ref storage.Ref, size uint64, refType MemRefType, allocator *Allocator) *MemRef {
	memRef := NewMemRef(ref, size, allocator)
	memRef.refType = refType
	return memRef
}

// AddRef increments the reference count
func (mr *MemRef) AddRef() int64 {
	return atomic.AddInt64(&mr.refCount, 1)
}

// Release decrements the reference count and cleans up if it reaches zero
func (mr *MemRef) Release() int64 {
	newCount := atomic.AddInt64(&mr.refCount, -1)
	if newCount == 0 {
		mr.cleanup()
	}
	return newCount
}

// GetRefCount returns the current reference count
func (mr *MemRef) GetRefCount() int64 {
	return atomic.LoadInt64(&mr.refCount)
}

// AddWeakRef increments the weak reference count
func (mr *MemRef) AddWeakRef() int64 {
	return atomic.AddInt64(&mr.weakCount, 1)
}

// ReleaseWeakRef decrements the weak reference count
func (mr *MemRef) ReleaseWeakRef() int64 {
	return atomic.AddInt64(&mr.weakCount, -1)
}

// GetWeakRefCount returns the current weak reference count
func (mr *MemRef) GetWeakRefCount() int64 {
	return atomic.LoadInt64(&mr.weakCount)
}

// IsNull returns true if the memory reference is null
func (mr *MemRef) IsNull() bool {
	return mr.ref == 0 && mr.addr == 0
}

// GetRef returns the storage reference
func (mr *MemRef) GetRef() storage.Ref {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return mr.ref
}

// SetRef sets the storage reference
func (mr *MemRef) SetRef(ref storage.Ref) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	mr.ref = ref
	mr.incrementVersion()
}

// GetAddr returns the memory address
func (mr *MemRef) GetAddr() uintptr {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	mr.recordAccess()
	return mr.addr
}

// SetAddr sets the memory address
func (mr *MemRef) SetAddr(addr uintptr) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	mr.addr = addr
	mr.incrementVersion()
}

// GetSize returns the size of the referenced data
func (mr *MemRef) GetSize() uint64 {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return mr.size
}

// SetSize sets the size of the referenced data
func (mr *MemRef) SetSize(size uint64) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	mr.size = size
	mr.incrementVersion()
}

// GetOffset returns the current offset
func (mr *MemRef) GetOffset() uint64 {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return mr.offset
}

// SetOffset sets the offset within the reference
func (mr *MemRef) SetOffset(offset uint64) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	mr.offset = offset
	mr.incrementVersion()
}

// GetType returns the memory reference type
func (mr *MemRef) GetType() MemRefType {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return mr.refType
}

// SetType sets the memory reference type
func (mr *MemRef) SetType(refType MemRefType) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	mr.refType = refType
}

// GetFlags returns the memory reference flags
func (mr *MemRef) GetFlags() MemRefFlags {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return mr.flags
}

// SetFlags sets the memory reference flags
func (mr *MemRef) SetFlags(flags MemRefFlags) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	mr.flags = flags
}

// HasFlag checks if a specific flag is set
func (mr *MemRef) HasFlag(flag MemRefFlags) bool {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return (mr.flags & flag) != 0
}

// SetFlag sets a specific flag
func (mr *MemRef) SetFlag(flag MemRefFlags) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	mr.flags |= flag
}

// ClearFlag clears a specific flag
func (mr *MemRef) ClearFlag(flag MemRefFlags) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	mr.flags &^= flag
}

// GetVersion returns the current version
func (mr *MemRef) GetVersion() uint64 {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return mr.version
}

// incrementVersion increments the version (internal use)
func (mr *MemRef) incrementVersion() {
	mr.version++
}

// GetParent returns the parent memory reference
func (mr *MemRef) GetParent() *MemRef {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return mr.parent
}

// SetParent sets the parent memory reference
func (mr *MemRef) SetParent(parent *MemRef) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	
	// Remove from old parent if exists
	if mr.parent != nil {
		mr.parent.removeChild(mr)
	}
	
	mr.parent = parent
	
	// Add to new parent if exists
	if parent != nil {
		parent.addChild(mr)
	}
}

// addChild adds a child reference (internal use)
func (mr *MemRef) addChild(child *MemRef) {
	mr.children = append(mr.children, child)
}

// removeChild removes a child reference (internal use)
func (mr *MemRef) removeChild(child *MemRef) {
	for i, c := range mr.children {
		if c == child {
			mr.children = append(mr.children[:i], mr.children[i+1:]...)
			break
		}
	}
}

// GetChildren returns a copy of child references
func (mr *MemRef) GetChildren() []*MemRef {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	
	children := make([]*MemRef, len(mr.children))
	copy(children, mr.children)
	return children
}

// GetAllocator returns the associated allocator
func (mr *MemRef) GetAllocator() *Allocator {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return mr.allocator
}

// recordAccess records an access for statistics (internal use)
func (mr *MemRef) recordAccess() {
	atomic.AddInt64(&mr.accessCount, 1)
	// Note: In a real implementation, you'd set lastAccess to current timestamp
	// atomic.StoreInt64(&mr.lastAccess, time.Now().UnixNano())
}

// GetAccessCount returns the access count
func (mr *MemRef) GetAccessCount() int64 {
	return atomic.LoadInt64(&mr.accessCount)
}

// GetLastAccess returns the last access timestamp
func (mr *MemRef) GetLastAccess() int64 {
	return atomic.LoadInt64(&mr.lastAccess)
}

// IsValid returns true if the memory reference is valid
func (mr *MemRef) IsValid() bool {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	return !mr.IsNull() && mr.GetRefCount() > 0
}

// Clone creates a copy of the memory reference with incremented ref count
func (mr *MemRef) Clone() *MemRef {
	mr.mutex.RLock()
	defer mr.mutex.RUnlock()
	
	clone := &MemRef{
		ref:         mr.ref,
		addr:        mr.addr,
		size:        mr.size,
		offset:      mr.offset,
		refType:     mr.refType,
		flags:       mr.flags,
		version:     mr.version,
		refCount:    1, // New reference starts with count 1
		weakCount:   0,
		allocator:   mr.allocator,
		parent:      mr.parent,
		children:    make([]*MemRef, 0), // Don't copy children
		lastAccess:  0,
		accessCount: 0,
	}
	
	return clone
}

// cleanup performs cleanup when reference count reaches zero
func (mr *MemRef) cleanup() {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()
	
	// Clean up children
	for _, child := range mr.children {
		child.SetParent(nil)
	}
	mr.children = mr.children[:0]
	
	// Remove from parent
	if mr.parent != nil {
		mr.parent.removeChild(mr)
		mr.parent = nil
	}
	
	// Clear memory address if mapped
	if mr.HasFlag(MemRefFlagMapped) && mr.addr != 0 {
		// In a real implementation, you'd unmap the memory here
		mr.addr = 0
	}
	
	// Mark as invalid
	mr.ref = 0
	mr.size = 0
}

// MemRefManager manages a collection of memory references
type MemRefManager struct {
	refs    map[storage.Ref]*MemRef
	mutex   sync.RWMutex
	alloc   *Allocator
}

// NewMemRefManager creates a new memory reference manager
func NewMemRefManager(allocator *Allocator) *MemRefManager {
	return &MemRefManager{
		refs:  make(map[storage.Ref]*MemRef),
		alloc: allocator,
	}
}

// GetMemRef gets or creates a memory reference
func (mrm *MemRefManager) GetMemRef(ref storage.Ref, size uint64) *MemRef {
	mrm.mutex.Lock()
	defer mrm.mutex.Unlock()
	
	if memRef, exists := mrm.refs[ref]; exists {
		memRef.AddRef()
		return memRef
	}
	
	memRef := NewMemRef(ref, size, mrm.alloc)
	mrm.refs[ref] = memRef
	return memRef
}

// ReleaseMemRef releases a memory reference
func (mrm *MemRefManager) ReleaseMemRef(ref storage.Ref) {
	mrm.mutex.Lock()
	defer mrm.mutex.Unlock()
	
	if memRef, exists := mrm.refs[ref]; exists {
		if memRef.Release() == 0 {
			delete(mrm.refs, ref)
		}
	}
}

// GetRefCount returns the number of managed references
func (mrm *MemRefManager) GetRefCount() int {
	mrm.mutex.RLock()
	defer mrm.mutex.RUnlock()
	return len(mrm.refs)
}

// Clear clears all managed references
func (mrm *MemRefManager) Clear() {
	mrm.mutex.Lock()
	defer mrm.mutex.Unlock()
	
	for ref, memRef := range mrm.refs {
		memRef.cleanup()
		delete(mrm.refs, ref)
	}
}

// WeakMemRef represents a weak reference that doesn't affect reference counting
type WeakMemRef struct {
	target *MemRef
	mutex  sync.RWMutex
}

// NewWeakMemRef creates a new weak memory reference
func NewWeakMemRef(target *MemRef) *WeakMemRef {
	if target != nil {
		target.AddWeakRef()
	}
	
	return &WeakMemRef{
		target: target,
	}
}

// Lock attempts to lock the weak reference and return a strong reference
func (wmr *WeakMemRef) Lock() *MemRef {
	wmr.mutex.RLock()
	defer wmr.mutex.RUnlock()
	
	if wmr.target == nil || wmr.target.GetRefCount() == 0 {
		return nil
	}
	
	wmr.target.AddRef()
	return wmr.target
}

// IsExpired returns true if the weak reference has expired
func (wmr *WeakMemRef) IsExpired() bool {
	wmr.mutex.RLock()
	defer wmr.mutex.RUnlock()
	
	return wmr.target == nil || wmr.target.GetRefCount() == 0
}

// Release releases the weak reference
func (wmr *WeakMemRef) Release() {
	wmr.mutex.Lock()
	defer wmr.mutex.Unlock()
	
	if wmr.target != nil {
		wmr.target.ReleaseWeakRef()
		wmr.target = nil
	}
}