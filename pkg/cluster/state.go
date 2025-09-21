package cluster

import (
	"fmt"
	"sync"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// MemRef represents a memory reference following tur-core pattern
type MemRef struct {
	Ref    storage.Ref // File reference
	Addr   uintptr     // Memory address when loaded
	Size   uint64      // Size of the referenced data
	Offset uint64      // Offset within the reference
}

// NewMemRef creates a new memory reference
func NewMemRef(ref storage.Ref, size uint64) *MemRef {
	return &MemRef{
		Ref:    ref,
		Size:   size,
		Offset: 0,
	}
}

// IsNull returns true if the memory reference is null
func (mr *MemRef) IsNull() bool {
	return mr.Ref == 0 && mr.Addr == 0
}

// GetRef returns the storage reference
func (mr *MemRef) GetRef() storage.Ref {
	return mr.Ref
}

// GetSize returns the size of the referenced data
func (mr *MemRef) GetSize() uint64 {
	return mr.Size
}

// SetOffset sets the offset within the reference
func (mr *MemRef) SetOffset(offset uint64) {
	mr.Offset = offset
}

// GetOffset returns the current offset
func (mr *MemRef) GetOffset() uint64 {
	return mr.Offset
}

// State represents the complex state management for cluster operations
type State struct {
	// Core state fields following tur-core pattern
	splitKey    keys.ObjKey // Key used for splitting operations
	memRef      *MemRef     // Memory reference for the state
	index       int         // Current index position
	depth       int         // Tree depth at this state
	keyOffset   uint64      // Key offset for relative addressing
	parentState *State      // Parent state for hierarchical operations
	childStates []*State    // Child states

	// Cluster-specific state
	cluster    *Cluster    // Associated cluster
	clusterRef storage.Ref // Cluster reference
	nodeType   NodeType    // Type of the node

	// Operation state
	operation StateOperation // Current operation being performed
	flags     StateFlags     // State flags
	context   *StateContext  // Operation context

	// Synchronization
	mutex    sync.RWMutex // State mutex
	refCount int32        // Reference count
	isValid  bool         // Validity flag
}

// StateOperation represents different types of state operations
type StateOperation int

const (
	StateOpNone StateOperation = iota
	StateOpInsert
	StateOpUpdate
	StateOpDelete
	StateOpSplit
	StateOpMerge
	StateOpTraverse
	StateOpSearch
)

// StateFlags represents state flags bitmask
type StateFlags uint32

const (
	StateFlagNone       StateFlags = 0
	StateFlagDirty      StateFlags = 1 << 0 // State has been modified
	StateFlagSplitting  StateFlags = 1 << 1 // State is in splitting operation
	StateFlagMerging    StateFlags = 1 << 2 // State is in merging operation
	StateFlagTraversing StateFlags = 1 << 3 // State is being traversed
	StateFlagLocked     StateFlags = 1 << 4 // State is locked
	StateFlagInvalid    StateFlags = 1 << 5 // State is invalid
	StateFlagCascading  StateFlags = 1 << 6 // State is in cascade operation
	StateFlagLeafCached StateFlags = 1 << 7 // Leaf is cached
)

// StateContext provides context for state operations
type StateContext struct {
	alloc      *core.Allocator
	fileFormat *storage.FileFormat
	treeHeight int
	maxDepth   int
	userData   interface{}
}

// NewState creates a new state with the given parameters
func NewState(cluster *Cluster, memRef *MemRef, index int) *State {
	return &State{
		cluster:     cluster,
		memRef:      memRef,
		index:       index,
		depth:       0,
		keyOffset:   0,
		childStates: make([]*State, 0),
		operation:   StateOpNone,
		flags:       StateFlagNone,
		refCount:    1,
		isValid:     true,
	}
}

// NewStateWithContext creates a new state with context
func NewStateWithContext(cluster *Cluster, memRef *MemRef, index int, context *StateContext) *State {
	state := NewState(cluster, memRef, index)
	state.context = context
	return state
}

// GetSplitKey returns the split key
func (s *State) GetSplitKey() keys.ObjKey {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.splitKey
}

// SetSplitKey sets the split key
func (s *State) SetSplitKey(key keys.ObjKey) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.splitKey = key
	s.setFlag(StateFlagDirty)
}

// GetMemRef returns the memory reference
func (s *State) GetMemRef() *MemRef {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.memRef
}

// SetMemRef sets the memory reference
func (s *State) SetMemRef(memRef *MemRef) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.memRef = memRef
	s.setFlag(StateFlagDirty)
}

// GetIndex returns the current index
func (s *State) GetIndex() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.index
}

// SetIndex sets the current index
func (s *State) SetIndex(index int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.index = index
	s.setFlag(StateFlagDirty)
}

// GetDepth returns the tree depth
func (s *State) GetDepth() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.depth
}

// SetDepth sets the tree depth
func (s *State) SetDepth(depth int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.depth = depth
}

// GetKeyOffset returns the key offset
func (s *State) GetKeyOffset() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.keyOffset
}

// SetKeyOffset sets the key offset for relative addressing
func (s *State) SetKeyOffset(offset uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.keyOffset = offset
	s.setFlag(StateFlagDirty)
}

// GetParentState returns the parent state
func (s *State) GetParentState() *State {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.parentState
}

// SetParentState sets the parent state
func (s *State) SetParentState(parent *State) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.parentState = parent
}

// AddChildState adds a child state
func (s *State) AddChildState(child *State) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.childStates = append(s.childStates, child)
	child.SetParentState(s)
}

// GetChildStates returns all child states
func (s *State) GetChildStates() []*State {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	// Return a copy to avoid race conditions
	children := make([]*State, len(s.childStates))
	copy(children, s.childStates)
	return children
}

// GetCluster returns the associated cluster
func (s *State) GetCluster() *Cluster {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.cluster
}

// SetCluster sets the associated cluster
func (s *State) SetCluster(cluster *Cluster) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.cluster = cluster
	if cluster != nil {
		s.clusterRef = cluster.GetRef()
	}
}

// GetOperation returns the current operation
func (s *State) GetOperation() StateOperation {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.operation
}

// SetOperation sets the current operation
func (s *State) SetOperation(op StateOperation) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.operation = op
	s.setFlag(StateFlagDirty)
}

// HasFlag checks if a specific flag is set
func (s *State) HasFlag(flag StateFlags) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return (s.flags & flag) != 0
}

// SetFlag sets a specific flag
func (s *State) SetFlag(flag StateFlags) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.setFlag(flag)
}

// ClearFlag clears a specific flag
func (s *State) ClearFlag(flag StateFlags) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.flags &^= flag
}

// setFlag sets a flag without locking (internal use)
func (s *State) setFlag(flag StateFlags) {
	s.flags |= flag
}

// IsValid returns true if the state is valid
func (s *State) IsValid() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.isValid && !s.HasFlag(StateFlagInvalid)
}

// Invalidate marks the state as invalid
func (s *State) Invalidate() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.isValid = false
	s.setFlag(StateFlagInvalid)
}

// Clone creates a copy of the state
func (s *State) Clone() *State {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	clone := &State{
		splitKey:    s.splitKey,
		memRef:      s.memRef, // Shallow copy of MemRef
		index:       s.index,
		depth:       s.depth,
		keyOffset:   s.keyOffset,
		cluster:     s.cluster,
		clusterRef:  s.clusterRef,
		nodeType:    s.nodeType,
		operation:   s.operation,
		flags:       s.flags,
		context:     s.context,
		childStates: make([]*State, 0),
		refCount:    1,
		isValid:     s.isValid,
	}

	return clone
}

// Reset resets the state to initial values
func (s *State) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.splitKey = 0
	s.index = 0
	s.depth = 0
	s.keyOffset = 0
	s.operation = StateOpNone
	s.flags = StateFlagNone
	s.childStates = s.childStates[:0] // Clear slice but keep capacity
	s.isValid = true
}

// GetContext returns the state context
func (s *State) GetContext() *StateContext {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.context
}

// SetContext sets the state context
func (s *State) SetContext(context *StateContext) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.context = context
}

// String returns a string representation of the state
func (s *State) String() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return fmt.Sprintf("State{splitKey: %v, index: %d, depth: %d, keyOffset: %d, operation: %v, flags: %v, valid: %v}",
		s.splitKey, s.index, s.depth, s.keyOffset, s.operation, s.flags, s.isValid)
}

// CascadeState represents state for complex cascade operations
type CascadeState struct {
	*State
	cascadeDepth int
	cascadeType  CascadeType
	affectedKeys []keys.ObjKey
}

// CascadeType represents different types of cascade operations
type CascadeType int

const (
	CascadeNone CascadeType = iota
	CascadeDelete
	CascadeUpdate
	CascadeInsert
)

// NewCascadeState creates a new cascade state
func NewCascadeState(baseState *State, cascadeType CascadeType) *CascadeState {
	return &CascadeState{
		State:        baseState,
		cascadeDepth: 0,
		cascadeType:  cascadeType,
		affectedKeys: make([]keys.ObjKey, 0),
	}
}

// GetCascadeDepth returns the cascade depth
func (cs *CascadeState) GetCascadeDepth() int {
	return cs.cascadeDepth
}

// SetCascadeDepth sets the cascade depth
func (cs *CascadeState) SetCascadeDepth(depth int) {
	cs.cascadeDepth = depth
}

// GetCascadeType returns the cascade type
func (cs *CascadeState) GetCascadeType() CascadeType {
	return cs.cascadeType
}

// AddAffectedKey adds a key affected by the cascade operation
func (cs *CascadeState) AddAffectedKey(key keys.ObjKey) {
	cs.affectedKeys = append(cs.affectedKeys, key)
}

// GetAffectedKeys returns all keys affected by the cascade operation
func (cs *CascadeState) GetAffectedKeys() []keys.ObjKey {
	return cs.affectedKeys
}
