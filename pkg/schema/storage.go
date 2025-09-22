package schema

import (
	"fmt"
	"sync"

	"github.com/turdb/tur/pkg/core"
	"github.com/turdb/tur/pkg/storage"
)

// SchemaStorage manages schema persistence and retrieval
type SchemaStorage struct {
	group      *storage.Group
	schemaRef  storage.Ref
	mutex      sync.RWMutex
	cache      map[string]*Schema
	version    uint64
}

// SchemaEntry represents a stored schema entry
type SchemaEntry struct {
	Name      string
	Version   uint64
	SchemaRef storage.Ref
}

// NewSchemaStorage creates a new schema storage manager
func NewSchemaStorage(group *storage.Group) *SchemaStorage {
	return &SchemaStorage{
		group:   group,
		cache:   make(map[string]*Schema),
		version: 0,
	}
}

// StoreSchema persists a schema to storage
func (ss *SchemaStorage) StoreSchema(name string, schema *Schema) error {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	// Serialize schema
	data, err := ss.serializeSchema(schema)
	if err != nil {
		return fmt.Errorf("failed to serialize schema: %w", err)
	}

	// Store in group
	ref, err := ss.group.StoreData(data)
	if err != nil {
		return fmt.Errorf("failed to store schema data: %w", err)
	}

	// Update cache
	ss.cache[name] = schema
	ss.version++

	// Store schema entry
	entry := &SchemaEntry{
		Name:      name,
		Version:   schema.GetVersion(),
		SchemaRef: ref,
	}

	return ss.storeSchemaEntry(name, entry)
}

// LoadSchema retrieves a schema from storage
func (ss *SchemaStorage) LoadSchema(name string) (*Schema, error) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	// Check cache first
	if schema, exists := ss.cache[name]; exists {
		return schema, nil
	}

	// Load schema entry
	entry, err := ss.loadSchemaEntry(name)
	if err != nil {
		return nil, fmt.Errorf("failed to load schema entry: %w", err)
	}

	// Load schema data
	data, err := ss.group.LoadData(entry.SchemaRef)
	if err != nil {
		return nil, fmt.Errorf("failed to load schema data: %w", err)
	}

	// Deserialize schema
	schema, err := ss.deserializeSchema(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %w", err)
	}

	// Update cache
	ss.cache[name] = schema

	return schema, nil
}

// ValidateStoredSchema validates a schema against stored version
func (ss *SchemaStorage) ValidateStoredSchema(name string, schema *Schema) error {
	storedSchema, err := ss.LoadSchema(name)
	if err != nil {
		return fmt.Errorf("failed to load stored schema: %w", err)
	}

	if !ss.schemasEqual(schema, storedSchema) {
		return fmt.Errorf("schema validation failed: stored schema differs from provided schema")
	}

	return nil
}

// GetSchemaVersion returns the current schema version
func (ss *SchemaStorage) GetSchemaVersion() uint64 {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()
	return ss.version
}

// ListSchemas returns all stored schema names
func (ss *SchemaStorage) ListSchemas() ([]string, error) {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()

	var names []string
	for name := range ss.cache {
		names = append(names, name)
	}

	return names, nil
}

// HasSchema checks if a schema exists
func (ss *SchemaStorage) HasSchema(name string) bool {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()
	
	if _, exists := ss.cache[name]; exists {
		return true
	}

	// Check persistent storage
	_, err := ss.loadSchemaEntry(name)
	return err == nil
}

// RemoveSchema removes a schema from storage
func (ss *SchemaStorage) RemoveSchema(name string) error {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()
	
	// Remove from cache
	delete(ss.cache, name)

	// Remove from persistent storage
	return ss.removeSchemaEntry(name)
}

// schemasEqual compares two schemas for equality
func (ss *SchemaStorage) schemasEqual(schema1, schema2 *Schema) bool {
	if schema1 == nil || schema2 == nil {
		return schema1 == schema2
	}

	if schema1.GetVersion() != schema2.GetVersion() {
		return false
	}

	if schema1.Size() != schema2.Size() {
		return false
	}

	for i := 0; i < schema1.Size(); i++ {
		obj1 := schema1.At(i)
		obj2 := schema2.At(i)
		if !ss.objectSchemasEqual(obj1, obj2) {
			return false
		}
	}

	return true
}

// objectSchemasEqual compares two object schemas for equality
func (ss *SchemaStorage) objectSchemasEqual(obj1, obj2 *ObjectSchema) bool {
	if obj1.Name != obj2.Name || obj1.TableType != obj2.TableType {
		return false
	}

	if len(obj1.PersistedProperties) != len(obj2.PersistedProperties) {
		return false
	}

	for i, prop1 := range obj1.PersistedProperties {
		prop2 := obj2.PersistedProperties[i]
		if !ss.propertiesEqual(&prop1, &prop2) {
			return false
		}
	}

	return true
}

// propertiesEqual compares two properties for equality
func (ss *SchemaStorage) propertiesEqual(prop1, prop2 *Property) bool {
	return prop1.Name == prop2.Name &&
		prop1.Type == prop2.Type &&
		prop1.ObjectType == prop2.ObjectType &&
		prop1.IsNullable == prop2.IsNullable &&
		prop1.IsIndexed == prop2.IsIndexed &&
		prop1.IsPrimary == prop2.IsPrimary
}

// serializeSchema converts a schema to byte array for storage
func (ss *SchemaStorage) serializeSchema(schema *Schema) ([]byte, error) {
	// Calculate required size
	size := 8 // version (uint64)
	size += 4 // object count (uint32)

	for i := 0; i < schema.Size(); i++ {
		obj := schema.At(i)
		size += 4 + len(obj.Name)                    // name length + name
		size += 1                                    // table type
		size += 4                                    // property count
		for _, prop := range obj.PersistedProperties {
			size += 4 + len(prop.Name)               // property name
			size += 1                                // data type
			size += 1                                // collection type
			size += 4 + len(prop.ObjectType)        // object type
			size += 1                                // nullable
			size += 1                                // indexed
			size += 1                                // primary key
		}
	}

	// Allocate buffer
	data := make([]byte, size)
	offset := 0

	// Write version
	core.WriteUint64(data[offset:], schema.GetVersion())
	offset += 8

	// Write object count
	core.WriteUint32(data[offset:], uint32(schema.Size()))
	offset += 4

	// Write objects
	for i := 0; i < schema.Size(); i++ {
		obj := schema.At(i)
		
		// Write object name
		core.WriteUint32(data[offset:], uint32(len(obj.Name)))
		offset += 4
		copy(data[offset:], obj.Name)
		offset += len(obj.Name)

		// Write table type
		data[offset] = byte(obj.TableType)
		offset++

		// Write property count
		core.WriteUint32(data[offset:], uint32(len(obj.PersistedProperties)))
		offset += 4

		// Write properties
		for _, prop := range obj.PersistedProperties {
			// Write property name
			core.WriteUint32(data[offset:], uint32(len(prop.Name)))
			offset += 4
			copy(data[offset:], prop.Name)
			offset += len(prop.Name)

			// Write data type
			data[offset] = byte(prop.Type)
			offset++

			// Write collection type (0 for non-collection)
			data[offset] = 0
			offset++

			// Write object type
			core.WriteUint32(data[offset:], uint32(len(prop.ObjectType)))
			offset += 4
			copy(data[offset:], prop.ObjectType)
			offset += len(prop.ObjectType)

			// Write nullable
			if prop.IsNullable {
				data[offset] = 1
			} else {
				data[offset] = 0
			}
			offset++

			// Write indexed
			if prop.IsIndexed {
				data[offset] = 1
			} else {
				data[offset] = 0
			}
			offset++

			// Write primary key
			if prop.IsPrimary {
				data[offset] = 1
			} else {
				data[offset] = 0
			}
			offset++
		}
	}

	return data, nil
}

// deserializeSchema converts byte array back to schema
func (ss *SchemaStorage) deserializeSchema(data []byte) (*Schema, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("invalid schema data: too short")
	}

	offset := 0

	// Read version
	version := core.ReadUint64(data[offset:])
	offset += 8

	// Read object count
	objectCount := core.ReadUint32(data[offset:])
	offset += 4

	// Read objects
	objects := make([]ObjectSchema, objectCount)
	for i := uint32(0); i < objectCount; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("invalid schema data: unexpected end")
		}

		// Read object name
		nameLen := core.ReadUint32(data[offset:])
		offset += 4
		if offset+int(nameLen) > len(data) {
			return nil, fmt.Errorf("invalid schema data: name too long")
		}
		name := string(data[offset : offset+int(nameLen)])
		offset += int(nameLen)

		// Read table type
		if offset >= len(data) {
			return nil, fmt.Errorf("invalid schema data: missing table type")
		}
		tableType := ObjectType(data[offset])
		offset++

		// Read property count
		if offset+4 > len(data) {
			return nil, fmt.Errorf("invalid schema data: missing property count")
		}
		propCount := core.ReadUint32(data[offset:])
		offset += 4

		obj := ObjectSchema{
			Name:                name,
			TableType:           tableType,
			PersistedProperties: make([]Property, propCount),
		}

		// Read properties
		for j := uint32(0); j < propCount; j++ {
			// Read property name
			if offset+4 > len(data) {
				return nil, fmt.Errorf("invalid schema data: missing property name length")
			}
			propNameLen := core.ReadUint32(data[offset:])
			offset += 4
			if offset+int(propNameLen) > len(data) {
				return nil, fmt.Errorf("invalid schema data: property name too long")
			}
			propName := string(data[offset : offset+int(propNameLen)])
			offset += int(propNameLen)

			// Read property attributes
			if offset+2 > len(data) {
				return nil, fmt.Errorf("invalid schema data: missing property type")
			}
			propType := PropertyType(data[offset])
			offset++
			// Skip collection type
			offset++

			// Read object type
			if offset+4 > len(data) {
				return nil, fmt.Errorf("invalid schema data: missing object type length")
			}
			objTypeLen := core.ReadUint32(data[offset:])
			offset += 4
			if offset+int(objTypeLen) > len(data) {
				return nil, fmt.Errorf("invalid schema data: object type too long")
			}
			objectType := string(data[offset : offset+int(objTypeLen)])
			offset += int(objTypeLen)

			// Read flags
			if offset+3 > len(data) {
				return nil, fmt.Errorf("invalid schema data: missing property flags")
			}
			isNullable := data[offset] == 1
			offset++
			isIndexed := data[offset] == 1
			offset++
			isPrimary := data[offset] == 1
			offset++

			obj.PersistedProperties[j] = Property{
				Name:       propName,
				Type:       propType,
				ObjectType: objectType,
				IsNullable: isNullable,
				IsIndexed:  isIndexed,
				IsPrimary:  isPrimary,
			}
		}

		objects[i] = obj
	}

	schema := NewSchema(objects)
	schema.SetVersion(version)
	return schema, nil
}

// storeSchemaEntry stores a schema entry
func (ss *SchemaStorage) storeSchemaEntry(name string, entry *SchemaEntry) error {
	// Create entry data
	var data []byte
	
	// Store name length and name
	nameBytes := []byte(entry.Name)
	data = append(data, byte(len(nameBytes)))
	data = append(data, nameBytes...)
	
	// Store version (8 bytes)
	versionBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		versionBytes[i] = byte(entry.Version >> (i * 8))
	}
	data = append(data, versionBytes...)
	
	// Store schema ref (8 bytes)
	refBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		refBytes[i] = byte(entry.SchemaRef >> (i * 8))
	}
	data = append(data, refBytes...)
	
	// Store schema as metadata entry with type 1 (schema type)
	return ss.group.AddMetadata(name, data, 1)
}

// loadSchemaEntry loads a schema entry
func (ss *SchemaStorage) loadSchemaEntry(name string) (*SchemaEntry, error) {
	// Load schema data from metadata
	schemaData, err := ss.group.FindMetadata(name)
	if err != nil {
		return nil, fmt.Errorf("schema not found: %s", name)
	}

	// Parse the schema entry data
	if len(schemaData) < 17 { // 1 + 8 + 8 minimum
		return nil, fmt.Errorf("invalid schema entry data")
	}

	offset := 0
	// Read name length and name
	nameLen := int(schemaData[offset])
	offset++
	if offset+nameLen > len(schemaData) {
		return nil, fmt.Errorf("invalid schema entry: name too long")
	}
	entryName := string(schemaData[offset : offset+nameLen])
	offset += nameLen

	// Read version (8 bytes)
	var version uint64
	for i := 0; i < 8; i++ {
		version |= uint64(schemaData[offset+i]) << (i * 8)
	}
	offset += 8

	// Read schema ref (8 bytes)
	var schemaRef storage.Ref
	for i := 0; i < 8; i++ {
		schemaRef |= storage.Ref(schemaData[offset+i]) << (i * 8)
	}

	return &SchemaEntry{
		Name:      entryName,
		Version:   version,
		SchemaRef: schemaRef,
	}, nil
}

// removeSchemaEntry removes a schema entry
func (ss *SchemaStorage) removeSchemaEntry(name string) error {
	// Remove schema metadata entry
	return ss.group.RemoveMetadata(name)
}