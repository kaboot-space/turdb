package table

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/storage"
)

// TableMetadata represents metadata for a table
type TableMetadata struct {
	TableKey    keys.TableKey `json:"table_key"`
	Name        string        `json:"name"`
	Spec        *TableSpec    `json:"spec"`
	Created     int64         `json:"created"`
	Modified    int64         `json:"modified"`
	Version     uint32        `json:"version"`
	RowCount    uint64        `json:"row_count"`
	ColumnCount uint32        `json:"column_count"`
}

// MetadataManager manages table metadata persistence
type MetadataManager struct {
	group      *storage.Group
	fileFormat *storage.FileFormat
}

// NewMetadataManager creates a new metadata manager
func NewMetadataManager(group *storage.Group, fileFormat *storage.FileFormat) *MetadataManager {
	return &MetadataManager{
		group:      group,
		fileFormat: fileFormat,
	}
}

// SaveTableMetadata saves table metadata to storage
func (mm *MetadataManager) SaveTableMetadata(table *Table) error {
	metadata := &TableMetadata{
		TableKey:    table.key,
		Name:        table.name,
		Spec:        table.spec,
		Created:     table.spec.Created,
		Modified:    table.spec.Modified,
		Version:     table.spec.Version,
		RowCount:    table.GetObjectCount(),
		ColumnCount: uint32(len(table.columns)),
	}

	// Serialize metadata to JSON
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Allocate space and write to storage
	ref, err := mm.fileFormat.AllocateSpace(uint32(len(data)))
	if err != nil {
		return fmt.Errorf("failed to allocate space for metadata: %w", err)
	}

	// Write data using the file mapper
	offset := mm.fileFormat.RefToOffset(ref)
	mapper := mm.fileFormat.GetMapper()
	copy(mapper.GetData()[offset:offset+int64(len(data))], data)

	if err := mapper.Sync(); err != nil {
		return fmt.Errorf("failed to sync metadata: %w", err)
	}

	return nil
}

// LoadTableMetadata loads table metadata from storage
func (mm *MetadataManager) LoadTableMetadata(ref storage.Ref) (*TableMetadata, error) {
	// Read data using the file mapper
	offset := mm.fileFormat.RefToOffset(ref)
	mapper := mm.fileFormat.GetMapper()

	// First read the length to determine how much data to read
	// For simplicity, we'll read a reasonable chunk and unmarshal
	data := mapper.ReadAt(offset, 1024) // Read up to 1KB
	if data == nil {
		return nil, fmt.Errorf("failed to read metadata")
	}

	var metadata TableMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

// UpdateTableMetadata updates existing table metadata
func (mm *MetadataManager) UpdateTableMetadata(table *Table) error {
	// Update modification timestamp
	table.spec.Modified = time.Now().Unix()
	table.spec.Version++

	return mm.SaveTableMetadata(table)
}

// GetTableMetadata returns metadata for a table
func (t *Table) GetMetadata() *TableMetadata {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return &TableMetadata{
		TableKey:    t.key,
		Name:        t.name,
		Spec:        t.spec,
		Created:     t.spec.Created,
		Modified:    t.spec.Modified,
		Version:     t.spec.Version,
		RowCount:    t.GetObjectCount(),
		ColumnCount: uint32(len(t.columns)),
	}
}

// SetMetadata updates table metadata
func (t *Table) SetMetadata(metadata *TableMetadata) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.isOpen {
		return fmt.Errorf("table is not open")
	}

	t.name = metadata.Name
	t.spec = metadata.Spec
	t.spec.Modified = time.Now().Unix()

	return nil
}
