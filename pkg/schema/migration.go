package schema

import (
	"fmt"
)

// IndexType represents the type of index
type IndexType int

const (
	IndexTypeGeneral IndexType = iota
	IndexTypeFulltext
)

func (it IndexType) String() string {
	switch it {
	case IndexTypeGeneral:
		return "General"
	case IndexTypeFulltext:
		return "Fulltext"
	default:
		return "Unknown"
	}
}

// SchemaChangeKind represents the type of schema change
type SchemaChangeKind int

const (
	SchemaChangeAddTable SchemaChangeKind = iota
	SchemaChangeRemoveTable
	SchemaChangeChangeTableType
	SchemaChangeAddInitialProperties
	SchemaChangeAddProperty
	SchemaChangeRemoveProperty
	SchemaChangeChangePropertyType
	SchemaChangeMakePropertyNullable
	SchemaChangeMakePropertyRequired
	SchemaChangeAddIndex
	SchemaChangeRemoveIndex
	SchemaChangeChangePrimaryKey
)

func (sck SchemaChangeKind) String() string {
	switch sck {
	case SchemaChangeAddTable:
		return "AddTable"
	case SchemaChangeRemoveTable:
		return "RemoveTable"
	case SchemaChangeChangeTableType:
		return "ChangeTableType"
	case SchemaChangeAddInitialProperties:
		return "AddInitialProperties"
	case SchemaChangeAddProperty:
		return "AddProperty"
	case SchemaChangeRemoveProperty:
		return "RemoveProperty"
	case SchemaChangeChangePropertyType:
		return "ChangePropertyType"
	case SchemaChangeMakePropertyNullable:
		return "MakePropertyNullable"
	case SchemaChangeMakePropertyRequired:
		return "MakePropertyRequired"
	case SchemaChangeAddIndex:
		return "AddIndex"
	case SchemaChangeRemoveIndex:
		return "RemoveIndex"
	case SchemaChangeChangePrimaryKey:
		return "ChangePrimaryKey"
	default:
		return "Unknown"
	}
}

// AddTable represents adding a new table to the schema
type AddTable struct {
	Object *ObjectSchema
}

// RemoveTable represents removing a table from the schema
type RemoveTable struct {
	Object *ObjectSchema
}

// ChangeTableType represents changing the type of a table
type ChangeTableType struct {
	Object       *ObjectSchema
	OldTableType ObjectType
	NewTableType ObjectType
}

// AddInitialProperties represents adding initial properties to a table
type AddInitialProperties struct {
	Object *ObjectSchema
}

// AddProperty represents adding a property to a table
type AddProperty struct {
	Object   *ObjectSchema
	Property *Property
}

// RemoveProperty represents removing a property from a table
type RemoveProperty struct {
	Object   *ObjectSchema
	Property *Property
}

// ChangePropertyType represents changing the type of a property
type ChangePropertyType struct {
	Object      *ObjectSchema
	OldProperty *Property
	NewProperty *Property
}

// MakePropertyNullable represents making a property nullable
type MakePropertyNullable struct {
	Object   *ObjectSchema
	Property *Property
}

// MakePropertyRequired represents making a property required
type MakePropertyRequired struct {
	Object   *ObjectSchema
	Property *Property
}

// AddIndex represents adding an index to a property
type AddIndex struct {
	Object   *ObjectSchema
	Property *Property
	Type     IndexType
}

// RemoveIndex represents removing an index from a property
type RemoveIndex struct {
	Object   *ObjectSchema
	Property *Property
}

// ChangePrimaryKey represents changing the primary key of a table
type ChangePrimaryKey struct {
	Object   *ObjectSchema
	Property *Property
}

// SchemaChange represents a single change to the schema
type SchemaChange struct {
	Kind SchemaChangeKind
	Data interface{}
}

// NewAddTableChange creates a new AddTable schema change
func NewAddTableChange(object *ObjectSchema) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeAddTable,
		Data: &AddTable{Object: object},
	}
}

// NewRemoveTableChange creates a new RemoveTable schema change
func NewRemoveTableChange(object *ObjectSchema) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeRemoveTable,
		Data: &RemoveTable{Object: object},
	}
}

// NewChangeTableTypeChange creates a new ChangeTableType schema change
func NewChangeTableTypeChange(object *ObjectSchema, oldType, newType ObjectType) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeChangeTableType,
		Data: &ChangeTableType{
			Object:       object,
			OldTableType: oldType,
			NewTableType: newType,
		},
	}
}

// NewAddInitialPropertiesChange creates a new AddInitialProperties schema change
func NewAddInitialPropertiesChange(object *ObjectSchema) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeAddInitialProperties,
		Data: &AddInitialProperties{Object: object},
	}
}

// NewAddPropertyChange creates a new AddProperty schema change
func NewAddPropertyChange(object *ObjectSchema, property *Property) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeAddProperty,
		Data: &AddProperty{
			Object:   object,
			Property: property,
		},
	}
}

// NewRemovePropertyChange creates a new RemoveProperty schema change
func NewRemovePropertyChange(object *ObjectSchema, property *Property) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeRemoveProperty,
		Data: &RemoveProperty{
			Object:   object,
			Property: property,
		},
	}
}

// NewChangePropertyTypeChange creates a new ChangePropertyType schema change
func NewChangePropertyTypeChange(object *ObjectSchema, oldProperty, newProperty *Property) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeChangePropertyType,
		Data: &ChangePropertyType{
			Object:      object,
			OldProperty: oldProperty,
			NewProperty: newProperty,
		},
	}
}

// NewMakePropertyNullableChange creates a new MakePropertyNullable schema change
func NewMakePropertyNullableChange(object *ObjectSchema, property *Property) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeMakePropertyNullable,
		Data: &MakePropertyNullable{
			Object:   object,
			Property: property,
		},
	}
}

// NewMakePropertyRequiredChange creates a new MakePropertyRequired schema change
func NewMakePropertyRequiredChange(object *ObjectSchema, property *Property) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeMakePropertyRequired,
		Data: &MakePropertyRequired{
			Object:   object,
			Property: property,
		},
	}
}

// NewAddIndexChange creates a new AddIndex schema change
func NewAddIndexChange(object *ObjectSchema, property *Property, indexType IndexType) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeAddIndex,
		Data: &AddIndex{
			Object:   object,
			Property: property,
			Type:     indexType,
		},
	}
}

// NewRemoveIndexChange creates a new RemoveIndex schema change
func NewRemoveIndexChange(object *ObjectSchema, property *Property) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeRemoveIndex,
		Data: &RemoveIndex{
			Object:   object,
			Property: property,
		},
	}
}

// NewChangePrimaryKeyChange creates a new ChangePrimaryKey schema change
func NewChangePrimaryKeyChange(object *ObjectSchema, property *Property) *SchemaChange {
	return &SchemaChange{
		Kind: SchemaChangeChangePrimaryKey,
		Data: &ChangePrimaryKey{
			Object:   object,
			Property: property,
		},
	}
}

// Visit applies a visitor function to the schema change data
func (sc *SchemaChange) Visit(visitor func(interface{}) error) error {
	return visitor(sc.Data)
}

// Equal compares two schema changes for equality
func (sc *SchemaChange) Equal(other *SchemaChange) bool {
	if sc.Kind != other.Kind {
		return false
	}

	switch sc.Kind {
	case SchemaChangeAddTable:
		a := sc.Data.(*AddTable)
		b := other.Data.(*AddTable)
		return a.Object.Equal(b.Object)

	case SchemaChangeRemoveTable:
		a := sc.Data.(*RemoveTable)
		b := other.Data.(*RemoveTable)
		return a.Object.Equal(b.Object)

	case SchemaChangeChangeTableType:
		a := sc.Data.(*ChangeTableType)
		b := other.Data.(*ChangeTableType)
		return a.Object.Equal(b.Object) && a.OldTableType == b.OldTableType && a.NewTableType == b.NewTableType

	case SchemaChangeAddInitialProperties:
		a := sc.Data.(*AddInitialProperties)
		b := other.Data.(*AddInitialProperties)
		return a.Object.Equal(b.Object)

	case SchemaChangeAddProperty:
		a := sc.Data.(*AddProperty)
		b := other.Data.(*AddProperty)
		return a.Object.Equal(b.Object) && a.Property.Equal(b.Property)

	case SchemaChangeRemoveProperty:
		a := sc.Data.(*RemoveProperty)
		b := other.Data.(*RemoveProperty)
		return a.Object.Equal(b.Object) && a.Property.Equal(b.Property)

	case SchemaChangeChangePropertyType:
		a := sc.Data.(*ChangePropertyType)
		b := other.Data.(*ChangePropertyType)
		return a.Object.Equal(b.Object) && a.OldProperty.Equal(b.OldProperty) && a.NewProperty.Equal(b.NewProperty)

	case SchemaChangeMakePropertyNullable:
		a := sc.Data.(*MakePropertyNullable)
		b := other.Data.(*MakePropertyNullable)
		return a.Object.Equal(b.Object) && a.Property.Equal(b.Property)

	case SchemaChangeMakePropertyRequired:
		a := sc.Data.(*MakePropertyRequired)
		b := other.Data.(*MakePropertyRequired)
		return a.Object.Equal(b.Object) && a.Property.Equal(b.Property)

	case SchemaChangeAddIndex:
		a := sc.Data.(*AddIndex)
		b := other.Data.(*AddIndex)
		return a.Object.Equal(b.Object) && a.Property.Equal(b.Property) && a.Type == b.Type

	case SchemaChangeRemoveIndex:
		a := sc.Data.(*RemoveIndex)
		b := other.Data.(*RemoveIndex)
		return a.Object.Equal(b.Object) && a.Property.Equal(b.Property)

	case SchemaChangeChangePrimaryKey:
		a := sc.Data.(*ChangePrimaryKey)
		b := other.Data.(*ChangePrimaryKey)
		return a.Object.Equal(b.Object) && a.Property.Equal(b.Property)

	default:
		return false
	}
}

// String returns a string representation of the schema change
func (sc *SchemaChange) String() string {
	switch sc.Kind {
	case SchemaChangeAddTable:
		data := sc.Data.(*AddTable)
		return fmt.Sprintf("AddTable{Object: %s}", data.Object.Name)

	case SchemaChangeRemoveTable:
		data := sc.Data.(*RemoveTable)
		return fmt.Sprintf("RemoveTable{Object: %s}", data.Object.Name)

	case SchemaChangeChangeTableType:
		data := sc.Data.(*ChangeTableType)
		return fmt.Sprintf("ChangeTableType{Object: %s, Old: %s, New: %s}",
			data.Object.Name, data.OldTableType.String(), data.NewTableType.String())

	case SchemaChangeAddInitialProperties:
		data := sc.Data.(*AddInitialProperties)
		return fmt.Sprintf("AddInitialProperties{Object: %s}", data.Object.Name)

	case SchemaChangeAddProperty:
		data := sc.Data.(*AddProperty)
		return fmt.Sprintf("AddProperty{Object: %s, Property: %s}",
			data.Object.Name, data.Property.Name)

	case SchemaChangeRemoveProperty:
		data := sc.Data.(*RemoveProperty)
		return fmt.Sprintf("RemoveProperty{Object: %s, Property: %s}",
			data.Object.Name, data.Property.Name)

	case SchemaChangeChangePropertyType:
		data := sc.Data.(*ChangePropertyType)
		return fmt.Sprintf("ChangePropertyType{Object: %s, Property: %s, Old: %s, New: %s}",
			data.Object.Name, data.OldProperty.Name, data.OldProperty.Type.String(), data.NewProperty.Type.String())

	case SchemaChangeMakePropertyNullable:
		data := sc.Data.(*MakePropertyNullable)
		return fmt.Sprintf("MakePropertyNullable{Object: %s, Property: %s}",
			data.Object.Name, data.Property.Name)

	case SchemaChangeMakePropertyRequired:
		data := sc.Data.(*MakePropertyRequired)
		return fmt.Sprintf("MakePropertyRequired{Object: %s, Property: %s}",
			data.Object.Name, data.Property.Name)

	case SchemaChangeAddIndex:
		data := sc.Data.(*AddIndex)
		return fmt.Sprintf("AddIndex{Object: %s, Property: %s, Type: %s}",
			data.Object.Name, data.Property.Name, data.Type.String())

	case SchemaChangeRemoveIndex:
		data := sc.Data.(*RemoveIndex)
		return fmt.Sprintf("RemoveIndex{Object: %s, Property: %s}",
			data.Object.Name, data.Property.Name)

	case SchemaChangeChangePrimaryKey:
		data := sc.Data.(*ChangePrimaryKey)
		return fmt.Sprintf("ChangePrimaryKey{Object: %s, Property: %s}",
			data.Object.Name, data.Property.Name)

	default:
		return fmt.Sprintf("UnknownChange{Kind: %s}", sc.Kind.String())
	}
}

// SchemaDiff represents the differences between two schemas
type SchemaDiff struct {
	Changes []SchemaChange
}

// NewSchemaDiff creates a new schema diff
func NewSchemaDiff() *SchemaDiff {
	return &SchemaDiff{
		Changes: make([]SchemaChange, 0),
	}
}

// AddChange adds a schema change to the diff
func (sd *SchemaDiff) AddChange(change *SchemaChange) {
	sd.Changes = append(sd.Changes, *change)
}

// Empty returns true if there are no changes
func (sd *SchemaDiff) Empty() bool {
	return len(sd.Changes) == 0
}

// Size returns the number of changes
func (sd *SchemaDiff) Size() int {
	return len(sd.Changes)
}

// ComputeDifferences computes the differences between two schemas
func ComputeDifferences(oldSchema, newSchema *Schema) *SchemaDiff {
	diff := NewSchemaDiff()

	// Create maps for faster lookup
	oldObjects := make(map[string]*ObjectSchema)
	newObjects := make(map[string]*ObjectSchema)

	for i := 0; i < oldSchema.Size(); i++ {
		obj := oldSchema.At(i)
		oldObjects[obj.Name] = obj
	}

	for i := 0; i < newSchema.Size(); i++ {
		obj := newSchema.At(i)
		newObjects[obj.Name] = obj
	}

	// Find removed tables
	for name, oldObj := range oldObjects {
		if _, exists := newObjects[name]; !exists {
			diff.AddChange(NewRemoveTableChange(oldObj))
		}
	}

	// Find added tables and changed tables
	for name, newObj := range newObjects {
		if oldObj, exists := oldObjects[name]; exists {
			// Table exists in both schemas, check for changes
			computeObjectSchemaDifferences(oldObj, newObj, diff)
		} else {
			// New table
			diff.AddChange(NewAddTableChange(newObj))
			diff.AddChange(NewAddInitialPropertiesChange(newObj))
		}
	}

	return diff
}

// computeObjectSchemaDifferences computes differences between two object schemas
func computeObjectSchemaDifferences(oldObj, newObj *ObjectSchema, diff *SchemaDiff) {
	// Check table type changes
	if oldObj.TableType != newObj.TableType {
		diff.AddChange(NewChangeTableTypeChange(newObj, oldObj.TableType, newObj.TableType))
	}

	// Create property maps for comparison
	oldProps := make(map[string]*Property)
	newProps := make(map[string]*Property)

	for i := range oldObj.PersistedProperties {
		prop := &oldObj.PersistedProperties[i]
		oldProps[prop.Name] = prop
	}

	for i := range newObj.PersistedProperties {
		prop := &newObj.PersistedProperties[i]
		newProps[prop.Name] = prop
	}

	// Find removed properties
	for name, oldProp := range oldProps {
		if _, exists := newProps[name]; !exists {
			diff.AddChange(NewRemovePropertyChange(newObj, oldProp))
		}
	}

	// Find added and changed properties
	for name, newProp := range newProps {
		if oldProp, exists := oldProps[name]; exists {
			// Property exists in both schemas, check for changes
			computePropertyDifferences(oldObj, newObj, oldProp, newProp, diff)
		} else {
			// New property
			diff.AddChange(NewAddPropertyChange(newObj, newProp))
		}
	}

	// Check primary key changes
	if oldObj.PrimaryKey != newObj.PrimaryKey {
		if newObj.PrimaryKey != "" {
			newPkProp := newObj.PropertyForName(newObj.PrimaryKey)
			if newPkProp != nil {
				diff.AddChange(NewChangePrimaryKeyChange(newObj, newPkProp))
			}
		}
	}
}

// computePropertyDifferences computes differences between two properties
func computePropertyDifferences(oldObj, newObj *ObjectSchema, oldProp, newProp *Property, diff *SchemaDiff) {
	// Check type changes
	if oldProp.Type != newProp.Type {
		diff.AddChange(NewChangePropertyTypeChange(newObj, oldProp, newProp))
	}

	// Check nullability changes
	if oldProp.IsNullable != newProp.IsNullable {
		if newProp.IsNullable {
			diff.AddChange(NewMakePropertyNullableChange(newObj, newProp))
		} else {
			diff.AddChange(NewMakePropertyRequiredChange(newObj, newProp))
		}
	}

	// Check index changes
	if oldProp.IsIndexed != newProp.IsIndexed {
		if newProp.IsIndexed {
			diff.AddChange(NewAddIndexChange(newObj, newProp, IndexTypeGeneral))
		} else {
			diff.AddChange(NewRemoveIndexChange(newObj, newProp))
		}
	}

	// Check fulltext index changes
	if oldProp.IsFulltextIndexed != newProp.IsFulltextIndexed {
		if newProp.IsFulltextIndexed {
			diff.AddChange(NewAddIndexChange(newObj, newProp, IndexTypeFulltext))
		} else {
			diff.AddChange(NewRemoveIndexChange(newObj, newProp))
		}
	}
}

// MigrationFunction represents a function that applies schema changes
type MigrationFunction func(oldSchema, newSchema *Schema) error

// Migration represents a schema migration
type Migration struct {
	Version uint64
	Migrate MigrationFunction
}

// MigrationManager manages schema migrations
type MigrationManager struct {
	migrations map[uint64]*Migration
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager() *MigrationManager {
	return &MigrationManager{
		migrations: make(map[uint64]*Migration),
	}
}

// AddMigration adds a migration to the manager
func (mm *MigrationManager) AddMigration(migration *Migration) {
	mm.migrations[migration.Version] = migration
}

// GetMigration gets a migration by version
func (mm *MigrationManager) GetMigration(version uint64) *Migration {
	return mm.migrations[version]
}

// HasMigration checks if a migration exists for the given version
func (mm *MigrationManager) HasMigration(version uint64) bool {
	_, exists := mm.migrations[version]
	return exists
}

// ApplyMigrations applies all migrations from oldVersion to newVersion
func (mm *MigrationManager) ApplyMigrations(oldSchema *Schema, newSchema *Schema, oldVersion, newVersion uint64) error {
	if oldVersion >= newVersion {
		return nil
	}

	currentSchema := oldSchema
	for version := oldVersion + 1; version <= newVersion; version++ {
		migration := mm.GetMigration(version)
		if migration == nil {
			return fmt.Errorf("migration for version %d not found", version)
		}

		if err := migration.Migrate(currentSchema, newSchema); err != nil {
			return fmt.Errorf("migration to version %d failed: %w", version, err)
		}

		currentSchema.SetVersion(version)
	}

	return nil
}