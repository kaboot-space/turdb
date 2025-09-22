package test

import (
	"testing"

	"github.com/turdb/tur/pkg/keys"
	"github.com/turdb/tur/pkg/schema"
)

func TestMigration(t *testing.T) {
	t.Run("FieldTypeChanges", testFieldTypeChanges)
	t.Run("AddNewFields", testAddNewFields)
	t.Run("DropFields", testDropFields)
	t.Run("TableOperations", testTableOperations)
	t.Run("IndexOperations", testIndexOperations)
	t.Run("NullabilityChanges", testNullabilityChanges)
	t.Run("PrimaryKeyChanges", testPrimaryKeyChanges)
}

func testFieldTypeChanges(t *testing.T) {
	// Create initial schema with string field
	oldSchema := createUserSchemaV1()
	
	// Create new schema with int field (type change)
	newSchema := createUserSchemaV2()
	
	// Compute differences
	diff := schema.ComputeDifferences(oldSchema, newSchema)
	
	if diff.Empty() {
		t.Fatal("Expected schema differences but got none")
	}
	
	// Verify we have a property type change
	found := false
	for _, change := range diff.Changes {
		if change.Kind == schema.SchemaChangeChangePropertyType {
			changeData := change.Data.(*schema.ChangePropertyType)
			if changeData.OldProperty.Name == "age" && 
			   changeData.OldProperty.Type == keys.TypeString &&
			   changeData.NewProperty.Type == keys.TypeInt {
				found = true
				break
			}
		}
	}
	
	if !found {
		t.Error("Expected property type change from string to int for 'age' field")
	}
}

func testAddNewFields(t *testing.T) {
	// Create initial schema
	oldSchema := createUserSchemaV1()
	
	// Create new schema with additional field
	newSchema := createUserSchemaV3()
	
	// Compute differences
	diff := schema.ComputeDifferences(oldSchema, newSchema)
	
	if diff.Empty() {
		t.Fatal("Expected schema differences but got none")
	}
	
	// Verify we have an add property change
	found := false
	for _, change := range diff.Changes {
		if change.Kind == schema.SchemaChangeAddProperty {
			changeData := change.Data.(*schema.AddProperty)
			if changeData.Property.Name == "email" {
				found = true
				break
			}
		}
	}
	
	if !found {
		t.Error("Expected add property change for 'email' field")
	}
}

func testDropFields(t *testing.T) {
	// Create initial schema with multiple fields
	oldSchema := createUserSchemaV3()
	
	// Create new schema with dropped field
	newSchema := createUserSchemaV4()
	
	// Compute differences
	diff := schema.ComputeDifferences(oldSchema, newSchema)
	
	if diff.Empty() {
		t.Fatal("Expected schema differences but got none")
	}
	
	// Verify we have a remove property change
	found := false
	for _, change := range diff.Changes {
		if change.Kind == schema.SchemaChangeRemoveProperty {
			changeData := change.Data.(*schema.RemoveProperty)
			if changeData.Property.Name == "email" {
				found = true
				break
			}
		}
	}
	
	if !found {
		t.Error("Expected remove property change for 'email' field")
	}
}

func testTableOperations(t *testing.T) {
	// Create schema with one table
	oldSchema := createSingleTableSchema()
	
	// Create schema with additional table
	newSchema := createMultiTableSchema()
	
	// Compute differences
	diff := schema.ComputeDifferences(oldSchema, newSchema)
	
	if diff.Empty() {
		t.Fatal("Expected schema differences but got none")
	}
	
	// Verify we have an add table change
	found := false
	for _, change := range diff.Changes {
		if change.Kind == schema.SchemaChangeAddTable {
			changeData := change.Data.(*schema.AddTable)
			if changeData.Object.Name == "Product" {
				found = true
				break
			}
		}
	}
	
	if !found {
		t.Error("Expected add table change for 'Product' table")
	}
}

func testIndexOperations(t *testing.T) {
	// Create schema without index
	oldSchema := createUserSchemaV1()
	
	// Create schema with index
	newSchema := createUserSchemaWithIndex()
	
	// Compute differences
	diff := schema.ComputeDifferences(oldSchema, newSchema)
	
	if diff.Empty() {
		t.Fatal("Expected schema differences but got none")
	}
	
	// Verify we have an add index change
	found := false
	for _, change := range diff.Changes {
		if change.Kind == schema.SchemaChangeAddIndex {
			changeData := change.Data.(*schema.AddIndex)
			if changeData.Property.Name == "name" {
				found = true
				break
			}
		}
	}
	
	if !found {
		t.Error("Expected add index change for 'name' field")
	}
}

func testNullabilityChanges(t *testing.T) {
	// Create schema with nullable field
	oldSchema := createUserSchemaNullable()
	
	// Create schema with required field
	newSchema := createUserSchemaRequired()
	
	// Compute differences
	diff := schema.ComputeDifferences(oldSchema, newSchema)
	
	if diff.Empty() {
		t.Fatal("Expected schema differences but got none")
	}
	
	// Verify we have a make property required change
	found := false
	for _, change := range diff.Changes {
		if change.Kind == schema.SchemaChangeMakePropertyRequired {
			changeData := change.Data.(*schema.MakePropertyRequired)
			if changeData.Property.Name == "name" {
				found = true
				break
			}
		}
	}
	
	if !found {
		t.Error("Expected make property required change for 'name' field")
	}
}

func testPrimaryKeyChanges(t *testing.T) {
	// Create schema with one primary key
	oldSchema := createUserSchemaV1()
	
	// Create schema with different primary key
	newSchema := createUserSchemaNewPK()
	
	// Compute differences
	diff := schema.ComputeDifferences(oldSchema, newSchema)
	
	if diff.Empty() {
		t.Fatal("Expected schema differences but got none")
	}
	
	// Verify we have a change primary key change
	found := false
	for _, change := range diff.Changes {
		if change.Kind == schema.SchemaChangeChangePrimaryKey {
			changeData := change.Data.(*schema.ChangePrimaryKey)
			if changeData.Property.Name == "name" {
				found = true
				break
			}
		}
	}
	
	if !found {
		t.Error("Expected change primary key change to 'name' field")
	}
}

// Helper functions to create test schemas

func createUserSchemaV1() *schema.Schema {
	properties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsRequired: true,
		},
		{
			Name:       "age",
			Type:       keys.TypeString,
			IsNullable: true,
		},
	}
	
	objectSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, properties, nil, "")
	objectSchema.PrimaryKey = "id"
	
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}

func createUserSchemaV2() *schema.Schema {
	properties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsRequired: true,
		},
		{
			Name:       "age",
			Type:       keys.TypeInt, // Changed from string to int
			IsNullable: true,
		},
	}
	
	objectSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, properties, nil, "")
	objectSchema.PrimaryKey = "id"
	
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}

func createUserSchemaV3() *schema.Schema {
	properties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsRequired: true,
		},
		{
			Name:       "age",
			Type:       keys.TypeString,
			IsNullable: true,
		},
		{
			Name:       "email", // New field
			Type:       keys.TypeString,
			IsNullable: true,
		},
	}
	
	objectSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, properties, nil, "")
	objectSchema.PrimaryKey = "id"
	
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}

func createUserSchemaV4() *schema.Schema {
	properties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsRequired: true,
		},
		{
			Name:       "age",
			Type:       keys.TypeString,
			IsNullable: true,
		},
		// email field dropped
	}
	
	objectSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, properties, nil, "")
	objectSchema.PrimaryKey = "id"
	
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}

func createSingleTableSchema() *schema.Schema {
	properties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsRequired: true,
		},
	}
	
	objectSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, properties, nil, "")
	objectSchema.PrimaryKey = "id"
	
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}

func createMultiTableSchema() *schema.Schema {
	userProperties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsRequired: true,
		},
	}
	
	productProperties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "title",
			Type:       keys.TypeString,
			IsRequired: true,
		},
		{
			Name:       "price",
			Type:       keys.TypeDouble,
			IsRequired: true,
		},
	}
	
	userSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, userProperties, nil, "")
	userSchema.PrimaryKey = "id"
	
	productSchema := schema.NewObjectSchema("Product", schema.ObjectTypeTopLevel, productProperties, nil, "")
	productSchema.PrimaryKey = "id"
	
	return schema.NewSchema([]schema.ObjectSchema{*userSchema, *productSchema})
}

func createUserSchemaWithIndex() *schema.Schema {
	properties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsRequired: true,
			IsIndexed:  true, // Added index
		},
		{
			Name:       "age",
			Type:       keys.TypeString,
			IsNullable: true,
		},
	}
	
	objectSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, properties, nil, "")
	objectSchema.PrimaryKey = "id"
	
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}

func createUserSchemaNullable() *schema.Schema {
	properties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsNullable: true, // Nullable
		},
	}
	
	objectSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, properties, nil, "")
	objectSchema.PrimaryKey = "id"
	
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}

func createUserSchemaRequired() *schema.Schema {
	properties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsPrimary:  true,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsRequired: true, // Required
		},
	}
	
	objectSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, properties, nil, "")
	objectSchema.PrimaryKey = "id"
	
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}

func createUserSchemaNewPK() *schema.Schema {
	properties := []schema.Property{
		{
			Name:       "id",
			Type:       keys.TypeInt,
			IsRequired: true,
		},
		{
			Name:       "name",
			Type:       keys.TypeString,
			IsPrimary:  true, // Changed primary key
			IsRequired: true,
		},
		{
			Name:       "age",
			Type:       keys.TypeString,
			IsNullable: true,
		},
	}
	
	objectSchema := schema.NewObjectSchema("User", schema.ObjectTypeTopLevel, properties, nil, "")
	objectSchema.PrimaryKey = "name"
	
	return schema.NewSchema([]schema.ObjectSchema{*objectSchema})
}