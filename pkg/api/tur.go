package api

import (
	"fmt"
	"sync"

	"github.com/turdb/tur/pkg/btree"
	"github.com/turdb/tur/pkg/cluster"
	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// Tur represents the main database interface
type Tur struct {
	fileFormat  *storage.FileFormat
	allocator   *core.Allocator
	slabAlloc   *core.SlabAlloc
	clusterTree *cluster.ClusterTree
	group       *storage.Group
	tables      map[keys.TableKey]*Table
	btrees      map[string]*btree.BTree
	mutex       sync.RWMutex
	isOpen      bool
}

// TurConfig holds configuration for opening a Tur database
type TurConfig struct {
	Path     string
	PageSize uint32
	ReadOnly bool
}

// OpenTur opens or creates a Tur database
func OpenTur(config TurConfig) (*Tur, error) {
	// Set default page size
	if config.PageSize == 0 {
		config.PageSize = 4096
	}

	// Open file format
	fileFormat, err := storage.NewFileFormat(config.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file format: %w", err)
	}

	// Try to open existing file
	err = fileFormat.Open()
	if err != nil {
		// File doesn't exist or is invalid, initialize new one
		if err := fileFormat.Initialize(config.PageSize); err != nil {
			fileFormat.Close()
			return nil, fmt.Errorf("failed to initialize file: %w", err)
		}

		// Open the newly initialized file
		if err := fileFormat.Open(); err != nil {
			fileFormat.Close()
			return nil, fmt.Errorf("failed to open initialized file: %w", err)
		}
	}

	// Validate header
	if err := fileFormat.ValidateHeader(); err != nil {
		fileFormat.Close()
		return nil, fmt.Errorf("invalid file header: %w", err)
	}

	// Create allocator
	allocator := core.NewAllocator(fileFormat)
	slabAlloc := core.NewSlabAlloc(allocator)

	// Create cluster tree
	clusterTree := cluster.NewClusterTree(allocator, fileFormat)

	tur := &Tur{
		fileFormat:  fileFormat,
		allocator:   allocator,
		slabAlloc:   slabAlloc,
		clusterTree: clusterTree,
		tables:      make(map[keys.TableKey]*Table),
		btrees:      make(map[string]*btree.BTree),
		isOpen:      true,
	}

	// Load existing tables from the database file
	if err := tur.loadExistingTables(); err != nil {
		fileFormat.Close()
		return nil, fmt.Errorf("failed to load existing tables: %w", err)
	}

	return tur, nil
}

// Close closes the Tur database
func (r *Tur) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.isOpen {
		return nil
	}

	// Sync all changes to disk
	if err := r.fileFormat.GetMapper().Sync(); err != nil {
		return fmt.Errorf("failed to sync changes: %w", err)
	}

	// Close file format
	if err := r.fileFormat.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	r.isOpen = false
	return nil
}

// IsOpen returns true if the Tur database is open
func (r *Tur) IsOpen() bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.isOpen
}

// CreateTable creates a new table
func (r *Tur) CreateTable(name string) (*Table, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.isOpen {
		return nil, fmt.Errorf("tur is not open")
	}

	// Generate table key
	tableKey := keys.NewTableKey(uint32(len(r.tables) + 1))

	// Check if table already exists
	for _, table := range r.tables {
		if table.name == name {
			return nil, fmt.Errorf("table '%s' already exists", name)
		}
	}

	// Create or get the group
	if r.group == nil {
		// Create new group
		group, err := storage.NewGroup(r.fileFormat)
		if err != nil {
			return nil, fmt.Errorf("failed to create group: %w", err)
		}
		r.group = group

		// Update file header with group reference
		err = r.fileFormat.UpdateGroupRef(uint64(group.GetRef()))
		if err != nil {
			return nil, fmt.Errorf("failed to update group reference: %w", err)
		}
	}

	// Allocate space for table structure (placeholder)
	tableRef, err := r.fileFormat.AllocateSpace(1024) // Placeholder size
	if err != nil {
		return nil, fmt.Errorf("failed to allocate table space: %w", err)
	}

	// Add table to group
	err = r.group.AddTable(name, tableRef, tableKey)
	if err != nil {
		return nil, fmt.Errorf("failed to add table to group: %w", err)
	}

	// Update table count in file header
	err = r.fileFormat.UpdateTableCount(r.group.GetTableCount())
	if err != nil {
		return nil, fmt.Errorf("failed to update table count: %w", err)
	}

	// Create table
	table := &Table{
		key:        tableKey,
		name:       name,
		columns:    make(map[string]keys.ColKey),
		tur:        r,
		nextColKey: 1,
	}

	r.tables[tableKey] = table
	return table, nil
}

// GetTable retrieves a table by name
func (r *Tur) GetTable(name string) (*Table, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if !r.isOpen {
		return nil, fmt.Errorf("tur is not open")
	}

	for _, table := range r.tables {
		if table.name == name {
			return table, nil
		}
	}

	return nil, fmt.Errorf("table '%s' not found", name)
}

// GetTables returns all tables
func (r *Tur) GetTables() []*Table {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	tables := make([]*Table, 0, len(r.tables))
	for _, table := range r.tables {
		tables = append(tables, table)
	}

	return tables
}

// CreateBTree creates a new B+ tree index
func (r *Tur) CreateBTree(name string) (*btree.BTree, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.isOpen {
		return nil, fmt.Errorf("tur is not open")
	}

	if _, exists := r.btrees[name]; exists {
		return nil, fmt.Errorf("btree '%s' already exists", name)
	}

	tree := btree.NewBTree(r.fileFormat)
	r.btrees[name] = tree

	return tree, nil
}

// GetBTree retrieves a B+ tree by name
func (r *Tur) GetBTree(name string) (*btree.BTree, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if !r.isOpen {
		return nil, fmt.Errorf("tur is not open")
	}

	tree, exists := r.btrees[name]
	if !exists {
		return nil, fmt.Errorf("btree '%s' not found", name)
	}

	return tree, nil
}

// Sync flushes all pending changes to disk
func (r *Tur) Sync() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.isOpen {
		return fmt.Errorf("tur is not open")
	}

	return r.fileFormat.GetMapper().Sync()
}

// GetAllocator returns the allocator (for internal use)
func (r *Tur) GetAllocator() *core.Allocator {
	return r.allocator
}

// GetClusterTree returns the cluster tree (for internal use)
func (r *Tur) GetClusterTree() *cluster.ClusterTree {
	return r.clusterTree
}

// Compact performs database compaction
func (r *Tur) Compact() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.isOpen {
		return fmt.Errorf("tur is not open")
	}

	// This is a placeholder for compaction logic
	// In a real implementation, this would reorganize the file to reclaim space
	return fmt.Errorf("compaction not implemented")
}

// GetFileSize returns the current file size
func (r *Tur) GetFileSize() int64 {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if !r.isOpen {
		return 0
	}

	return r.fileFormat.GetMapper().Size()
}

// GetPageSize returns the page size
func (r *Tur) GetPageSize() uint32 {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if !r.isOpen {
		return 0
	}

	header := r.fileFormat.GetHeader()
	if header == nil {
		return 0
	}

	return header.PageSize
}

// GetFileFormat returns the file format (for debugging)
func (r *Tur) GetFileFormat() *storage.FileFormat {
	return r.fileFormat
}

// loadExistingTables loads existing tables from the database file into memory
func (r *Tur) loadExistingTables() error {
	groupRef := r.fileFormat.GetGroupRef()
	if groupRef == 0 {
		return nil
	}

	// Try to load the group from the database file
	group, err := storage.LoadGroup(r.fileFormat, storage.Ref(groupRef))
	if err != nil {
		return fmt.Errorf("failed to load group: %w", err)
	}
	r.group = group

	// Load each table from the group
	tableCount := group.GetTableCount()
	for i := uint32(0); i < tableCount; i++ {
		entry, err := group.GetTableEntry(i)
		if err != nil {
			continue
		}

		// Read table name from the group
		tableName, err := group.ReadString(entry.NameRef)
		if err != nil {
			// Skip this entry if we can't read the name
			continue
		}
		table := &Table{
			name:       tableName,
			key:        entry.TableKey,
			columns:    make(map[string]keys.ColKey),
			tur:        r,
			nextColKey: 0,
			nextObjKey: 0,
		}

		r.tables[entry.TableKey] = table
	}

	return nil
}
