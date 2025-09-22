package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/turdb/tur/pkg/keys"
)

// PropertyType represents the type of a property in the schema
type PropertyType = keys.DataType

// Property represents a single property in an object schema
type Property struct {
	// The internal column name used in the Tur file
	Name string

	// The public name used by the binding to represent the internal column name
	// If empty, the internal and public name are considered to be the same
	PublicName string

	// The data type of the property
	Type PropertyType

	// For link properties, the name of the target object type
	ObjectType string

	// For backlink properties, the name of the origin property
	LinkOriginPropertyName string

	// Whether this property is the primary key
	IsPrimary bool

	// Whether this property is indexed
	IsIndexed bool

	// Whether this property has full-text search enabled
	IsFulltextIndexed bool

	// Whether this property is nullable
	IsNullable bool

	// Whether this property is required (opposite of nullable)
	IsRequired bool

	// Column key for this property (set during table creation)
	ColKey keys.ColKey
}

// ObjectType represents the type of object in the schema
type ObjectType int

const (
	ObjectTypeTopLevel ObjectType = iota
	ObjectTypeEmbedded
	ObjectTypeAsymmetric
)

func (ot ObjectType) String() string {
	switch ot {
	case ObjectTypeTopLevel:
		return "TopLevel"
	case ObjectTypeEmbedded:
		return "Embedded"
	case ObjectTypeAsymmetric:
		return "Asymmetric"
	default:
		return "Unknown"
	}
}

// ObjectSchema represents the schema for a single object type
type ObjectSchema struct {
	// Name of the object type
	Name string

	// Properties that are persisted to disk
	PersistedProperties []Property

	// Properties that are computed and not persisted
	ComputedProperties []Property

	// Name of the primary key property (if any)
	PrimaryKey string

	// Table key for this object schema
	TableKey keys.TableKey

	// Type of object (TopLevel, Embedded, Asymmetric)
	TableType ObjectType

	// Alias name for the object type
	Alias string
}

// NewObjectSchema creates a new object schema
func NewObjectSchema(name string, tableType ObjectType, persistedProperties []Property, computedProperties []Property, alias string) *ObjectSchema {
	return &ObjectSchema{
		Name:                name,
		TableType:           tableType,
		PersistedProperties: persistedProperties,
		ComputedProperties:  computedProperties,
		Alias:               alias,
	}
}

// PropertyForPublicName finds a property by its public name
func (os *ObjectSchema) PropertyForPublicName(publicName string) *Property {
	for i := range os.PersistedProperties {
		prop := &os.PersistedProperties[i]
		if prop.PublicName == publicName || (prop.PublicName == "" && prop.Name == publicName) {
			return prop
		}
	}
	for i := range os.ComputedProperties {
		prop := &os.ComputedProperties[i]
		if prop.PublicName == publicName || (prop.PublicName == "" && prop.Name == publicName) {
			return prop
		}
	}
	return nil
}

// PropertyForName finds a property by its internal name
func (os *ObjectSchema) PropertyForName(name string) *Property {
	for i := range os.PersistedProperties {
		if os.PersistedProperties[i].Name == name {
			return &os.PersistedProperties[i]
		}
	}
	for i := range os.ComputedProperties {
		if os.ComputedProperties[i].Name == name {
			return &os.ComputedProperties[i]
		}
	}
	return nil
}

// PrimaryKeyProperty returns the primary key property if it exists
func (os *ObjectSchema) PrimaryKeyProperty() *Property {
	if os.PrimaryKey == "" {
		return nil
	}
	return os.PropertyForName(os.PrimaryKey)
}

// PropertyIsComputed returns true if the given property is computed
func (os *ObjectSchema) PropertyIsComputed(property *Property) bool {
	for i := range os.ComputedProperties {
		if &os.ComputedProperties[i] == property {
			return true
		}
	}
	return false
}

// SetPrimaryKeyProperty sets the primary key property
func (os *ObjectSchema) SetPrimaryKeyProperty() {
	for i := range os.PersistedProperties {
		if os.PersistedProperties[i].IsPrimary {
			os.PrimaryKey = os.PersistedProperties[i].Name
			return
		}
	}
}

// SchemaValidationMode represents different validation modes for schema
type SchemaValidationMode int

const (
	SchemaValidationModeBasic SchemaValidationMode = iota
	SchemaValidationModeStrict
	SchemaValidationModeRejectEmbeddedOrphans
)

// ObjectSchemaValidationException represents a validation error for an object schema
type ObjectSchemaValidationException struct {
	ObjectName string
	Message    string
}

func (e ObjectSchemaValidationException) Error() string {
	return fmt.Sprintf("Object schema validation error for '%s': %s", e.ObjectName, e.Message)
}

// Validate validates the object schema
func (os *ObjectSchema) Validate(schema *Schema, exceptions *[]ObjectSchemaValidationException, validationMode SchemaValidationMode) {
	// Validate primary key
	if os.PrimaryKey != "" {
		pkProp := os.PrimaryKeyProperty()
		if pkProp == nil {
			*exceptions = append(*exceptions, ObjectSchemaValidationException{
				ObjectName: os.Name,
				Message:    fmt.Sprintf("Primary key property '%s' not found", os.PrimaryKey),
			})
		} else if pkProp.IsNullable {
			*exceptions = append(*exceptions, ObjectSchemaValidationException{
				ObjectName: os.Name,
				Message:    "Primary key property cannot be nullable",
			})
		}
	}

	// Validate properties
	for _, prop := range os.PersistedProperties {
		os.validateProperty(&prop, schema, exceptions, validationMode)
	}

	for _, prop := range os.ComputedProperties {
		os.validateProperty(&prop, schema, exceptions, validationMode)
	}
}

// validateProperty validates a single property
func (os *ObjectSchema) validateProperty(prop *Property, schema *Schema, exceptions *[]ObjectSchemaValidationException, validationMode SchemaValidationMode) {
	// Validate link properties
	if keys.IsLinkType(prop.Type) {
		if prop.ObjectType == "" {
			*exceptions = append(*exceptions, ObjectSchemaValidationException{
				ObjectName: os.Name,
				Message:    fmt.Sprintf("Link property '%s' must specify object type", prop.Name),
			})
		} else if schema.Find(prop.ObjectType) == nil {
			*exceptions = append(*exceptions, ObjectSchemaValidationException{
				ObjectName: os.Name,
				Message:    fmt.Sprintf("Link property '%s' references unknown object type '%s'", prop.Name, prop.ObjectType),
			})
		}
	}

	// Validate backlink properties
	if prop.LinkOriginPropertyName != "" {
		if prop.ObjectType == "" {
			*exceptions = append(*exceptions, ObjectSchemaValidationException{
				ObjectName: os.Name,
				Message:    fmt.Sprintf("Backlink property '%s' must specify object type", prop.Name),
			})
		}
	}

	// Validate property names
	if prop.Name == "" {
		*exceptions = append(*exceptions, ObjectSchemaValidationException{
			ObjectName: os.Name,
			Message:    "Property name cannot be empty",
		})
	}
}

// Schema represents a complete database schema
type Schema struct {
	objectSchemas []ObjectSchema
	version       uint64
}

// NewSchema creates a new schema
func NewSchema(objectSchemas []ObjectSchema) *Schema {
	schema := &Schema{
		objectSchemas: make([]ObjectSchema, len(objectSchemas)),
	}
	copy(schema.objectSchemas, objectSchemas)
	schema.sortSchema()
	return schema
}

// Size returns the number of object schemas
func (s *Schema) Size() int {
	return len(s.objectSchemas)
}

// Empty returns true if the schema has no object schemas
func (s *Schema) Empty() bool {
	return len(s.objectSchemas) == 0
}

// Begin returns an iterator to the beginning of the schema
func (s *Schema) Begin() int {
	return 0
}

// End returns an iterator to the end of the schema
func (s *Schema) End() int {
	return len(s.objectSchemas)
}

// At returns the object schema at the given index
func (s *Schema) At(index int) *ObjectSchema {
	if index < 0 || index >= len(s.objectSchemas) {
		return nil
	}
	return &s.objectSchemas[index]
}

// Find finds an object schema by name
func (s *Schema) Find(name string) *ObjectSchema {
	// Binary search since schemas are sorted by name
	left, right := 0, len(s.objectSchemas)-1
	for left <= right {
		mid := (left + right) / 2
		cmp := strings.Compare(s.objectSchemas[mid].Name, name)
		if cmp == 0 {
			return &s.objectSchemas[mid]
		} else if cmp < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return nil
}

// FindByTableKey finds an object schema by table key
func (s *Schema) FindByTableKey(tableKey keys.TableKey) *ObjectSchema {
	for i := range s.objectSchemas {
		if s.objectSchemas[i].TableKey == tableKey {
			return &s.objectSchemas[i]
		}
	}
	return nil
}

// Validate validates the entire schema
func (s *Schema) Validate(validationMode SchemaValidationMode) error {
	var exceptions []ObjectSchemaValidationException

	for i := range s.objectSchemas {
		s.objectSchemas[i].Validate(s, &exceptions, validationMode)
	}

	if len(exceptions) > 0 {
		var messages []string
		for _, ex := range exceptions {
			messages = append(messages, ex.Error())
		}
		return fmt.Errorf("Schema validation failed:\n%s", strings.Join(messages, "\n"))
	}

	return nil
}

// GetVersion returns the schema version
func (s *Schema) GetVersion() uint64 {
	return s.version
}

// SetVersion sets the schema version
func (s *Schema) SetVersion(version uint64) {
	s.version = version
}

// sortSchema sorts all object schemas by name for faster lookups
func (s *Schema) sortSchema() {
	sort.Slice(s.objectSchemas, func(i, j int) bool {
		return s.objectSchemas[i].Name < s.objectSchemas[j].Name
	})
}

// Equal compares two schemas for equality
func (s *Schema) Equal(other *Schema) bool {
	if s.Size() != other.Size() {
		return false
	}

	for i := 0; i < s.Size(); i++ {
		if !s.objectSchemas[i].Equal(&other.objectSchemas[i]) {
			return false
		}
	}

	return true
}

// Equal compares two object schemas for equality
func (os *ObjectSchema) Equal(other *ObjectSchema) bool {
	if os.Name != other.Name ||
		os.PrimaryKey != other.PrimaryKey ||
		os.TableType != other.TableType ||
		os.Alias != other.Alias ||
		len(os.PersistedProperties) != len(other.PersistedProperties) ||
		len(os.ComputedProperties) != len(other.ComputedProperties) {
		return false
	}

	for i, prop := range os.PersistedProperties {
		if !prop.Equal(&other.PersistedProperties[i]) {
			return false
		}
	}

	for i, prop := range os.ComputedProperties {
		if !prop.Equal(&other.ComputedProperties[i]) {
			return false
		}
	}

	return true
}

// Equal compares two properties for equality
func (p *Property) Equal(other *Property) bool {
	return p.Name == other.Name &&
		p.PublicName == other.PublicName &&
		p.Type == other.Type &&
		p.ObjectType == other.ObjectType &&
		p.LinkOriginPropertyName == other.LinkOriginPropertyName &&
		p.IsPrimary == other.IsPrimary &&
		p.IsIndexed == other.IsIndexed &&
		p.IsFulltextIndexed == other.IsFulltextIndexed &&
		p.IsNullable == other.IsNullable &&
		p.IsRequired == other.IsRequired
}

// String returns a string representation of the schema
func (s *Schema) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Schema (version: %d, objects: %d):\n", s.version, s.Size()))
	for i := 0; i < s.Size(); i++ {
		sb.WriteString(fmt.Sprintf("  %s\n", s.objectSchemas[i].String()))
	}
	return sb.String()
}

// String returns a string representation of the object schema
func (os *ObjectSchema) String() string {
	return fmt.Sprintf("ObjectSchema{Name: %s, Type: %s, Properties: %d, PrimaryKey: %s}",
		os.Name, os.TableType.String(), len(os.PersistedProperties), os.PrimaryKey)
}

// String returns a string representation of the property
func (p *Property) String() string {
	flags := []string{}
	if p.IsPrimary {
		flags = append(flags, "primary")
	}
	if p.IsIndexed {
		flags = append(flags, "indexed")
	}
	if p.IsNullable {
		flags = append(flags, "nullable")
	}
	if p.IsRequired {
		flags = append(flags, "required")
	}

	flagStr := ""
	if len(flags) > 0 {
		flagStr = fmt.Sprintf(" [%s]", strings.Join(flags, ", "))
	}

	return fmt.Sprintf("Property{Name: %s, Type: %s%s}", p.Name, p.Type.String(), flagStr)
}