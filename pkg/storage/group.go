package storage

import (
	"fmt"
	"unsafe"

	"github.com/turdb/tur/pkg/keys"
)

// GroupHeader represents the header of a Group structure
type GroupHeader struct {
	TableCount    uint32 // Number of tables in the group
	EntriesRef    uint32 // Reference to table entries array (low 32 bits)
	EntriesRefH   uint32 // Reference to table entries array (high 32 bits)
	MetadataCount uint32 // Number of metadata entries
	MetadataRef   uint32 // Reference to metadata entries array (low 32 bits)
	MetadataRefH  uint32 // Reference to metadata entries array (high 32 bits)
	Reserved      uint32 // Reserved for alignment
}

// TableEntry represents an entry in the table directory
type TableEntry struct {
	NameRef  Ref           // Reference to table name string
	TableRef Ref           // Reference to table structure
	TableKey keys.TableKey // Table key
	Reserved uint32        // Reserved for alignment
}

// MetadataEntry represents an entry in the metadata registry
type MetadataEntry struct {
	KeyRef   Ref    // Reference to metadata key string
	ValueRef Ref    // Reference to metadata value data
	Type     uint32 // Type of metadata entry
	Reserved uint32 // Reserved for alignment
}

// Group represents the top-level database structure containing tables
type Group struct {
	fileFormat *FileFormat
	ref        Ref
	header     *GroupHeader
	tables     []TableEntry
	metadata   []MetadataEntry
}

// NewGroup creates a new Group structure
func NewGroup(fileFormat *FileFormat) (*Group, error) {
	// Allocate space for group header
	headerSize := uint32(unsafe.Sizeof(GroupHeader{}))
	ref, err := fileFormat.AllocateSpace(headerSize)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate space for group: %w", err)
	}

	// Initialize header
	header := &GroupHeader{
		TableCount:    0,
		EntriesRef:    0,
		EntriesRefH:   0,
		MetadataCount: 0,
		MetadataRef:   0,
		MetadataRefH:  0,
		Reserved:      0,
	}

	group := &Group{
		fileFormat: fileFormat,
		ref:        ref,
		header:     header,
		tables:     make([]TableEntry, 0),
		metadata:   make([]MetadataEntry, 0),
	}

	// Write header to file
	if err := group.writeHeader(); err != nil {
		return nil, fmt.Errorf("failed to write group header: %w", err)
	}

	// Update file header with group reference
	if err := fileFormat.UpdateGroupRef(uint64(ref)); err != nil {
		return nil, fmt.Errorf("failed to update group reference: %w", err)
	}

	return group, nil
}

// LoadGroup loads an existing Group from file
func LoadGroup(fileFormat *FileFormat, ref Ref) (*Group, error) {
	group := &Group{
		fileFormat: fileFormat,
		ref:        ref,
	}

	// Read header
	if err := group.readHeader(); err != nil {
		return nil, fmt.Errorf("failed to read group header: %w", err)
	}

	// Read table entries
	if err := group.readTableEntries(); err != nil {
		return nil, fmt.Errorf("failed to read table entries: %w", err)
	}

	// Read metadata entries
	if err := group.readMetadataEntries(); err != nil {
		return nil, fmt.Errorf("failed to read metadata entries: %w", err)
	}

	return group, nil
}

// AddTable adds a new table to the group
func (g *Group) AddTable(name string, tableRef Ref, tableKey keys.TableKey) error {
	// Allocate space for table name
	nameRef, err := g.allocateString(name)
	if err != nil {
		return fmt.Errorf("failed to allocate table name: %w", err)
	}

	// Create table entry
	entry := TableEntry{
		NameRef:  nameRef,
		TableRef: tableRef,
		TableKey: tableKey,
		Reserved: 0,
	}

	// Add to tables slice
	g.tables = append(g.tables, entry)
	g.header.TableCount++

	// Write updated data to file
	if err := g.writeHeader(); err != nil {
		return fmt.Errorf("failed to write group header: %w", err)
	}

	if err := g.writeTableEntries(); err != nil {
		return fmt.Errorf("failed to write table entries: %w", err)
	}

	// Write header again to persist the updated EntriesRef from writeTableEntries
	if err := g.writeHeader(); err != nil {
		return fmt.Errorf("failed to write updated group header: %w", err)
	}

	// Update file header table count
	if err := g.fileFormat.UpdateTableCount(g.header.TableCount); err != nil {
		return fmt.Errorf("failed to update table count: %w", err)
	}

	return nil
}

// GetTableCount returns the number of tables in the group
func (g *Group) GetTableCount() uint32 {
	return g.header.TableCount
}

// GetTableEntry returns a table entry by index
func (g *Group) GetTableEntry(index uint32) (*TableEntry, error) {
	if index >= g.header.TableCount {
		return nil, fmt.Errorf("table index out of range: %d", index)
	}
	return &g.tables[index], nil
}

// FindTableByName finds a table entry by name
func (g *Group) FindTableByName(name string) (*TableEntry, error) {
	for i := range g.tables {
		tableName, err := g.readString(g.tables[i].NameRef)
		if err != nil {
			continue
		}
		if tableName == name {
			return &g.tables[i], nil
		}
	}
	return nil, fmt.Errorf("table not found: %s", name)
}

// writeHeader writes the group header to file
func (g *Group) writeHeader() error {
	offset := g.fileFormat.RefToOffset(g.ref)
	headerBytes := (*[unsafe.Sizeof(GroupHeader{})]byte)(unsafe.Pointer(g.header))[:]

	mapper := g.fileFormat.GetMapper()
	copy(mapper.GetData()[offset:offset+int64(len(headerBytes))], headerBytes)

	return mapper.Sync()
}

// readHeader reads the group header from file
func (g *Group) readHeader() error {
	offset := g.fileFormat.RefToOffset(g.ref)
	headerSize := int(unsafe.Sizeof(GroupHeader{}))

	mapper := g.fileFormat.GetMapper()
	data := mapper.ReadAt(offset, headerSize)
	if data == nil {
		return fmt.Errorf("failed to read group header")
	}

	g.header = (*GroupHeader)(unsafe.Pointer(&data[0]))
	return nil
}

// writeTableEntries writes table entries to file
func (g *Group) writeTableEntries() error {
	if len(g.tables) == 0 {
		// Clear the entries reference if no tables
		g.header.EntriesRef = 0
		g.header.EntriesRefH = 0
		return nil
	}

	entrySize := uint32(unsafe.Sizeof(TableEntry{}))
	totalSize := entrySize * uint32(len(g.tables))

	// Allocate space for table entries
	entriesRef, err := g.fileFormat.AllocateSpace(totalSize)
	if err != nil {
		return fmt.Errorf("failed to allocate space for table entries: %w", err)
	}

	// Store the reference in the header
	g.header.EntriesRef = uint32(entriesRef & 0xFFFFFFFF)
	g.header.EntriesRefH = uint32(entriesRef >> 32)

	offset := g.fileFormat.RefToOffset(entriesRef)
	mapper := g.fileFormat.GetMapper()

	for i, entry := range g.tables {
		entryOffset := offset + int64(i)*int64(entrySize)
		entryBytes := (*[unsafe.Sizeof(TableEntry{})]byte)(unsafe.Pointer(&entry))[:]

		copy(mapper.GetData()[entryOffset:entryOffset+int64(len(entryBytes))], entryBytes)
	}

	return mapper.Sync()
}

// readTableEntries reads table entries from file
func (g *Group) readTableEntries() error {
	if g.header.TableCount == 0 {
		g.tables = make([]TableEntry, 0)
		return nil
	}

	// Reconstruct the entries reference from header
	entriesRef := Ref(g.header.EntriesRef) | (Ref(g.header.EntriesRefH) << 32)
	if entriesRef == 0 {
		g.tables = make([]TableEntry, 0)
		g.header.TableCount = 0
		return nil
	}

	entrySize := int64(unsafe.Sizeof(TableEntry{}))
	totalSize := entrySize * int64(g.header.TableCount)

	entriesOffset := g.fileFormat.RefToOffset(entriesRef)

	mapper := g.fileFormat.GetMapper()
	data := mapper.ReadAt(entriesOffset, int(totalSize))
	if data == nil {
		return fmt.Errorf("failed to read table entries")
	}

	g.tables = make([]TableEntry, g.header.TableCount)
	for i := uint32(0); i < g.header.TableCount; i++ {
		entryOffset := int64(i) * entrySize
		g.tables[i] = *(*TableEntry)(unsafe.Pointer(&data[entryOffset]))
	}

	return nil
}

// allocateString allocates space for a string and returns its reference
func (g *Group) allocateString(s string) (Ref, error) {
	// String format: [length:4][data:length]
	strLen := uint32(len(s))
	totalSize := 4 + strLen

	ref, err := g.fileFormat.AllocateSpace(totalSize)
	if err != nil {
		return 0, fmt.Errorf("failed to allocate string space: %w", err)
	}

	offset := g.fileFormat.RefToOffset(ref)
	mapper := g.fileFormat.GetMapper()

	// Write length directly to mapped memory
	lengthBytes := (*[4]byte)(unsafe.Pointer(&strLen))[:]
	copy(mapper.GetData()[offset:offset+4], lengthBytes)

	// Write string data directly to mapped memory
	if strLen > 0 {
		copy(mapper.GetData()[offset+4:offset+4+int64(strLen)], []byte(s))
	}

	// Sync to ensure data is written to disk
	if err := mapper.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync string data: %w", err)
	}

	return ref, nil
}

// readString reads a string from the given reference
func (g *Group) readString(ref Ref) (string, error) {
	offset := g.fileFormat.RefToOffset(ref)
	mapper := g.fileFormat.GetMapper()

	// Read length
	lengthData := mapper.ReadAt(offset, 4)
	if lengthData == nil {
		return "", fmt.Errorf("failed to read string length")
	}

	strLen := *(*uint32)(unsafe.Pointer(&lengthData[0]))
	if strLen == 0 {
		return "", nil
	}

	// Read string data
	stringData := mapper.ReadAt(offset+4, int(strLen))
	if stringData == nil {
		return "", fmt.Errorf("failed to read string data")
	}

	return string(stringData), nil
}

// GetRef returns the group reference
func (g *Group) GetRef() Ref {
	return g.ref
}

// ReadString reads a string from the given reference (public method)
func (g *Group) ReadString(ref Ref) (string, error) {
	return g.readString(ref)
}

// StoreData stores arbitrary data and returns its reference
func (g *Group) StoreData(data []byte) (Ref, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Data format: [length:4][data:length]
	dataLen := uint32(len(data))
	totalSize := 4 + dataLen

	ref, err := g.fileFormat.AllocateSpace(totalSize)
	if err != nil {
		return 0, fmt.Errorf("failed to allocate data space: %w", err)
	}

	offset := g.fileFormat.RefToOffset(ref)
	mapper := g.fileFormat.GetMapper()

	// Write length directly to mapped memory
	lengthBytes := (*[4]byte)(unsafe.Pointer(&dataLen))[:]
	copy(mapper.GetData()[offset:offset+4], lengthBytes)

	// Write data directly to mapped memory
	copy(mapper.GetData()[offset+4:offset+4+int64(dataLen)], data)

	// Sync to ensure data is written to disk
	if err := mapper.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync data: %w", err)
	}

	return ref, nil
}

// LoadData loads arbitrary data from the given reference
func (g *Group) LoadData(ref Ref) ([]byte, error) {
	if ref == 0 {
		return nil, nil
	}

	offset := g.fileFormat.RefToOffset(ref)
	mapper := g.fileFormat.GetMapper()

	// Read length
	lengthData := mapper.ReadAt(offset, 4)
	if lengthData == nil {
		return nil, fmt.Errorf("failed to read data length")
	}

	dataLen := *(*uint32)(unsafe.Pointer(&lengthData[0]))
	if dataLen == 0 {
		return []byte{}, nil
	}

	// Read data
	data := mapper.ReadAt(offset+4, int(dataLen))
	if data == nil {
		return nil, fmt.Errorf("failed to read data")
	}

	// Return a copy to avoid issues with memory mapping
	result := make([]byte, dataLen)
	copy(result, data)
	return result, nil
}

// AddMetadata adds a metadata entry to the group
func (g *Group) AddMetadata(key string, value []byte, entryType uint32) error {
	// Store key string
	keyRef, err := g.allocateString(key)
	if err != nil {
		return fmt.Errorf("failed to store metadata key: %w", err)
	}

	// Store value data
	valueRef, err := g.StoreData(value)
	if err != nil {
		return fmt.Errorf("failed to store metadata value: %w", err)
	}

	// Create metadata entry
	entry := MetadataEntry{
		KeyRef:   keyRef,
		ValueRef: valueRef,
		Type:     entryType,
		Reserved: 0,
	}

	// Add to metadata slice
	g.metadata = append(g.metadata, entry)
	g.header.MetadataCount = uint32(len(g.metadata))

	// Write metadata entries to file
	return g.writeMetadataEntries()
}

// FindMetadata finds a metadata entry by key
func (g *Group) FindMetadata(key string) ([]byte, error) {
	for _, entry := range g.metadata {
		// Read the key from storage
		entryKey, err := g.readString(entry.KeyRef)
		if err != nil {
			continue
		}

		if entryKey == key {
			// Load and return the value
			return g.LoadData(entry.ValueRef)
		}
	}

	return nil, fmt.Errorf("metadata entry not found: %s", key)
}

// RemoveMetadata removes a metadata entry by key
func (g *Group) RemoveMetadata(key string) error {
	for i, entry := range g.metadata {
		// Read the key from storage
		entryKey, err := g.readString(entry.KeyRef)
		if err != nil {
			continue
		}

		if entryKey == key {
			// Remove from slice
			g.metadata = append(g.metadata[:i], g.metadata[i+1:]...)
			g.header.MetadataCount = uint32(len(g.metadata))

			// Write updated metadata entries to file
			return g.writeMetadataEntries()
		}
	}

	return fmt.Errorf("metadata entry not found: %s", key)
}

// writeMetadataEntries writes metadata entries to file
func (g *Group) writeMetadataEntries() error {
	if len(g.metadata) == 0 {
		g.header.MetadataRef = 0
		g.header.MetadataRefH = 0
		return g.writeHeader()
	}

	entrySize := int64(unsafe.Sizeof(MetadataEntry{}))
	totalSize := entrySize * int64(len(g.metadata))

	// Allocate space for metadata entries
	entriesRef, err := g.fileFormat.AllocateSpace(uint32(totalSize))
	if err != nil {
		return fmt.Errorf("failed to allocate space for metadata entries: %w", err)
	}

	// Store the reference in the header
	g.header.MetadataRef = uint32(entriesRef & 0xFFFFFFFF)
	g.header.MetadataRefH = uint32(entriesRef >> 32)

	offset := g.fileFormat.RefToOffset(entriesRef)
	mapper := g.fileFormat.GetMapper()

	for i, entry := range g.metadata {
		entryOffset := offset + int64(i)*entrySize
		entryBytes := (*[unsafe.Sizeof(MetadataEntry{})]byte)(unsafe.Pointer(&entry))[:]

		copy(mapper.GetData()[entryOffset:entryOffset+int64(len(entryBytes))], entryBytes)
	}

	if err := mapper.Sync(); err != nil {
		return fmt.Errorf("failed to sync metadata entries: %w", err)
	}

	return g.writeHeader()
}

// readMetadataEntries reads metadata entries from file
func (g *Group) readMetadataEntries() error {
	if g.header.MetadataCount == 0 {
		g.metadata = make([]MetadataEntry, 0)
		return nil
	}

	// Reconstruct the entries reference from header
	entriesRef := Ref(g.header.MetadataRef) | (Ref(g.header.MetadataRefH) << 32)
	if entriesRef == 0 {
		g.metadata = make([]MetadataEntry, 0)
		g.header.MetadataCount = 0
		return nil
	}

	entrySize := int64(unsafe.Sizeof(MetadataEntry{}))
	totalSize := entrySize * int64(g.header.MetadataCount)

	entriesOffset := g.fileFormat.RefToOffset(entriesRef)

	mapper := g.fileFormat.GetMapper()
	data := mapper.ReadAt(entriesOffset, int(totalSize))
	if data == nil {
		return fmt.Errorf("failed to read metadata entries")
	}

	g.metadata = make([]MetadataEntry, g.header.MetadataCount)
	for i := uint32(0); i < g.header.MetadataCount; i++ {
		entryOffset := int64(i) * entrySize
		g.metadata[i] = *(*MetadataEntry)(unsafe.Pointer(&data[entryOffset]))
	}

	return nil
}
